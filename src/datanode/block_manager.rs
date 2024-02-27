use crate::block::BLOCK_CHUNK_SIZE;
use crate::error::RSHDFSError;
use crate::proto::data_node_name_node_service_client::DataNodeNameNodeServiceClient;
use crate::proto::rshdfs_block_service_server::RshdfsBlockService;
use crate::proto::{
    BlockChunk, BlockReportRequest, BlockStreamInfo, GetBlockRequest, TransferStatus,
};
use adler::Adler32;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, Mutex, RwLock};

use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

pub struct BlockInfo {
    pub block_seq: i32,
}

/// Manages the sending of the block report to the NameNode at regular intervals
/// Manages the Blocks on the DataNodes local disk.
pub struct BlockManager {
    datanode_service_client: Arc<Mutex<DataNodeNameNodeServiceClient<Channel>>>,
    data_dir: String,
    pub blocks: Arc<RwLock<HashMap<Uuid, BlockInfo>>>,
    interval: Duration,
}

impl BlockManager {
    pub fn new(
        datanode_service_client: Arc<Mutex<DataNodeNameNodeServiceClient<Channel>>>,
        interval: Duration,
        data_dir: String,
    ) -> Self {
        BlockManager {
            datanode_service_client,
            interval,
            data_dir,
            blocks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn start(self: Arc<Self>) {
        let self_clone = self.clone();
        let client = self.datanode_service_client.clone();
        let interval = self.interval;

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;
                let mut client_guard = client.lock().await;
                let block_report_vec: Vec<String>;
                {
                    let blocks = self_clone.blocks.read().await;
                    block_report_vec = blocks.keys().map(|uuid| uuid.to_string()).collect();
                } // blocks read lock is dropped here

                let request = BlockReportRequest {
                    block_ids: block_report_vec,
                };
                println!("Sending Block Report.");
                match client_guard.send_block_report(request).await {
                    Ok(response) => {
                        let inner_response = response.into_inner();
                        let mut paths_to_delete = Vec::new();
                        {
                            let block_guard = self_clone.blocks.read().await;
                            for block_id in inner_response.block_ids.iter() {
                                if let Ok(block_uuid) = Uuid::parse_str(block_id) {
                                    if let Some(block) = block_guard.get(&block_uuid) {
                                        let path = format!(
                                            "{}/{}_{}.dat",
                                            self_clone.data_dir, block_id, block.block_seq
                                        );
                                        paths_to_delete.push((path, block_uuid));
                                    }
                                } else {
                                    println!("Invalid UUID format: {:?}", block_id);
                                }
                            }
                        } // block_guard read lock is dropped here

                        for (path, block_uuid) in paths_to_delete {
                            if let Err(e) = tokio::fs::remove_file(&path).await {
                                println!("Failed to delete file {}: {:?}", path, e);
                                continue;
                            }

                            let mut write_block_guard = self_clone.blocks.write().await;
                            write_block_guard.remove(&block_uuid);
                        }
                    }
                    Err(_) => {
                        println!("BlockReport Failed.");
                    }
                }
            }
        });
    }
}

#[tonic::async_trait]
impl RshdfsBlockService for Arc<BlockManager> {
    async fn put_block_streamed(
        &self,
        request: Request<Streaming<BlockChunk>>,
    ) -> Result<Response<TransferStatus>, Status> {
        let mut stream = request.into_inner();

        // Grab the first block off the stream
        let first_chunk = match stream.next().await {
            Some(Ok(chunk)) => chunk,
            Some(Err(e)) => return Err(Status::aborted(format!("Failed to read chunk: {}", e))),
            None => return Err(Status::invalid_argument("No chunks received")),
        };

        // Create a file based on the first block's information
        let file_path = format!(
            "{}/{}_{}.dat",
            self.data_dir, first_chunk.block_id, first_chunk.block_seq
        );
        let mut file = match File::create(&file_path).await {
            Ok(file) => file,
            Err(e) => return Err(Status::internal(format!("Failed to create file: {}", e))),
        };

        // Write the first chunk's data to the file
        if let Err(e) = file.write_all(&first_chunk.chunked_data).await {
            return Err(Status::internal(format!("Failed to write to file: {}", e)));
        }

        // Loop to process remaining chunks
        let mut seq = 1;
        while let Some(chunk_result) = stream.next().await {
            let block_chunk = match chunk_result {
                Ok(chunk) => chunk,
                Err(e) => return Err(Status::aborted(format!("Failed to read chunk: {}", e))),
            };

            // Validate the block_chunk
            let mut adler = Adler32::new();
            adler.write_slice(&block_chunk.chunked_data);
            let checksum = adler.checksum();

            if block_chunk.checksum == checksum && seq == block_chunk.chunk_seq {
                // Write the chunk's data to the file
                if let Err(e) = file.write_all(&block_chunk.chunked_data).await {
                    return Err(Status::internal(format!("Failed to write to file: {}", e)));
                }
                seq += 1;
            }
        }

        // Return transfer status
        Ok(Response::new(TransferStatus {
            success: true,
            message: String::from("All chunks processed successfully"),
        }))
    }

    type GetBlockStreamedStream = ReceiverStream<Result<BlockChunk, Status>>;

    async fn get_block_streamed(
        &self,
        request: Request<GetBlockRequest>,
    ) -> Result<Response<Self::GetBlockStreamedStream>, Status> {
        let inner_request = request.into_inner();
        let block_id = inner_request.block_id;
        let block_id_uuid = Uuid::parse_str(&block_id)
            .map_err(|_| Status::invalid_argument("Invalid UUID format."))?;

        let blocks_guard = self.blocks.read().await;
        let block_info = blocks_guard
            .get(&block_id_uuid)
            .ok_or(RSHDFSError::FileSystemError(String::from(
                "Block not found.",
            )))?;
        let block_info_seq = block_info.block_seq;
        let file_path = PathBuf::from(format!(
            "{}/{}_{}.dat",
            self.data_dir, block_id, block_info_seq
        ));
        let mut file = File::open(&file_path)
            .await
            .map_err(|_| Status::internal("Failed to open file"))?;

        let (sender, receiver) = mpsc::channel(15);

        tokio::spawn(async move {
            let mut seq = 0;
            loop {
                let mut chunk_sized_buffer = vec![0; BLOCK_CHUNK_SIZE];
                let n = match file.read(&mut chunk_sized_buffer).await {
                    Ok(n) if n == 0 => break, // End of file
                    Ok(n) => n,
                    Err(_) => {
                        eprintln!("Error reading file chunk");
                        break;
                    }
                };

                chunk_sized_buffer.truncate(n); // Adjust buffer size to actual data read

                let mut adler = Adler32::new();
                adler.write_slice(&chunk_sized_buffer);
                let checksum = adler.checksum();
                let block_chunk = BlockChunk {
                    block_id: block_id.clone(),
                    block_seq: block_info_seq,
                    chunked_data: chunk_sized_buffer,
                    chunk_seq: seq,
                    checksum,
                };

                if sender.send(Ok(block_chunk)).await.is_err() {
                    eprintln!("Receiver dropped, stopping stream.");
                    break;
                }

                seq += 1; // Increment the sequence for the next chunk
            }
        });

        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn start_block_stream(
        &self,
        request: Request<BlockStreamInfo>,
    ) -> Result<Response<TransferStatus>, Status> {
        let block_stream_info = request.into_inner();
        let block_info = BlockInfo {
            block_seq: block_stream_info.block_seq,
        };
        let block_uuid = Uuid::parse_str(&block_stream_info.block_id)
            .map_err(|_| RSHDFSError::UUIDError(String::from("Bad UUID string.")))?;

        let mut blocks_guard = self.blocks.write().await;
        blocks_guard.insert(block_uuid, block_info);
        Ok(Response::new(TransferStatus {
            success: true,
            message: "Block information updated.".to_string(),
        }))
    }
}
