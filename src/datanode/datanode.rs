use std::collections::{HashMap};
use std::hash::Hasher;
use std::path::PathBuf;

use crate::config::datanode_config::DataNodeConfig;
use crate::error::RSHDFSError;

use crate::proto::data_node_name_node_service_client::DataNodeNameNodeServiceClient;
use crate::proto::rshdfs_data_node_service_server::RshdfsDataNodeService;
use crate::proto::{BlockChunk, BlockStreamInfo, GetBlockRequest, HeartBeatRequest, NodeHealthMetrics, RegistrationRequest, RegistrationResponse, TransferStatus};
use std::sync::{Arc};
use std::time::Duration;
use adler::Adler32;
use sysinfo::System;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, mpsc, RwLock};
use tokio::time;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status, Streaming};
use tonic::codegen::Body;
use tonic::codegen::tokio_stream::StreamExt;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;
use crate::block::BLOCK_CHUNK_SIZE;


/// Manages the sending of heartbeats to the NameNode at regular intervals.
struct HeartbeatManager {
    datanode_service_client: Arc<Mutex<DataNodeNameNodeServiceClient<Channel>>>,
    interval: Duration,
    running: bool,
    id: Uuid,
}

struct BlockPayload {
    seq: i32,
    checksum: u32,
    data: Vec<u8>,
}

struct BlockInfo {
    block_seq: i32,
}


impl HeartbeatManager {
    fn new(
        datanode_service_client: Arc<Mutex<DataNodeNameNodeServiceClient<Channel>>>,
        interval: Duration,
        id: Uuid,
    ) -> Self {
        HeartbeatManager {
            datanode_service_client,
            interval,
            running: false,
            id,
        }
    }

    /// Starts the heartbeat process, sending heartbeats at regular intervals.
    async fn start(&mut self) {
        self.running = true;
        let client = self.datanode_service_client.clone();
        let interval = self.interval;
        let datanode_id_str = self.id.to_string();

        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval);
            loop {
                interval_timer.tick().await;
                let mut client_guard = client.lock().await;
                let request = HeartBeatRequest {
                    datanode_id: datanode_id_str.clone(),
                };
                println!("Sending heartbeat.");
                match client_guard.send_heart_beat(request).await {
                    Ok(response) => {
                        println!("{:?}", response);
                        response.into_inner();
                    }
                    Err(_e) => {
                        RSHDFSError::HeartBeatFailed(String::from("Heartbeat Failed."));
                    }
                }
            }
        });
    }
}

/// Represents a DataNode in a distributed file system.
pub struct DataNode {
    id: Uuid,
    data_dir: String,
    client: Arc<Mutex<DataNodeNameNodeServiceClient<Channel>>>,
    heartbeat_interval: Duration,
    blocks: RwLock<HashMap<Uuid, BlockInfo>>,
    block_report_interval: Duration,
    addr: String,
    port: String,
    namenode_addr: String,
    heartbeat_manager: HeartbeatManager,
}

impl DataNode {
    /// Registers the DataNode with the NameNode and provides health metrics.
    async fn register_with_namenode(
        &self,
        data_node_client: &mut DataNodeNameNodeServiceClient<Channel>,
    ) -> Result<RegistrationResponse, RSHDFSError> {
        let node_health_metrics = gather_metrics().await?;
        let hostname = System::host_name().unwrap(); //todo: unwrap
        let registration_request = RegistrationRequest {
            datanode_id: self.id.to_string(),
            ipc_address: self.addr.clone(),
            hostname_port: format!("{}:{}", hostname, self.port),
            health_metrics: Some(node_health_metrics),
        };

        data_node_client
            .register_with_namenode(registration_request)
            .await
            .map_err(|_| {
                RSHDFSError::RegistrationFailed(String::from(
                    "Registration with the NameNode failed.",
                ))
            })
            .map(|response| response.into_inner())
    }

    /// Creates a new DataNode from the provided configuration.
    pub async fn from_config(config: DataNodeConfig) -> Result<Self, RSHDFSError> {
        println!(
            "Attempting to establish connection with: {}",
            config.namenode_address
        );
        let id = Uuid::new_v4();
        let endpoint = Endpoint::from_shared(format!("http://{}", config.namenode_address))?;
        let client = DataNodeNameNodeServiceClient::connect(endpoint).await?;
        let wrapped_client = Arc::new(Mutex::new(client));
        let heartbeat_manager = HeartbeatManager::new(
            wrapped_client.clone(),
            Duration::from_millis(config.heartbeat_interval),
            id,
        );

        Ok(DataNode {
            id,
            client: wrapped_client,
            data_dir: config.data_dir,
            heartbeat_interval: Duration::from_millis(config.heartbeat_interval),
            block_report_interval: Duration::from_millis(config.block_report_interval),
            addr: config.ipc_address,
            port: config.port,
            namenode_addr: config.namenode_address,
            heartbeat_manager,
            blocks: RwLock::new(HashMap::new()),
        })
    }

    /// Starts the main operations of the DataNode, including registration and heartbeat sending.
    pub async fn start(&mut self) -> Result<(), RSHDFSError> {
        // Step 1: Register with NameNode
        let mut client_guard = self.client.lock().await;
        let registration_response = self.register_with_namenode(&mut client_guard).await?;
        drop(client_guard);
        println!("Registered with NameNode: {:?}", registration_response);
        println!("Starting Heartbeat...");

        // Step 2: Start HeartbeatManager
        self.heartbeat_manager.start().await;

        Ok(())
    }

    async fn get_block_info(&self, uuid: Uuid) -> Result<BlockInfo, RSHDFSError> {
        let block_read_guard = self.blocks.read().await;
        let maybe_block_info = block_read_guard.get(&uuid);
        match maybe_block_info {
            None => Err(RSHDFSError::ReadError(String::from("Block not found."))),
            Some(block_info) => {
                return Ok(BlockInfo {
                    block_seq: block_info.block_seq,
                });
            }
        }
    }
}

/// Gathers health metrics from the system for reporting to the NameNode.
async fn gather_metrics() -> Result<NodeHealthMetrics, RSHDFSError> {
    let mut system = System::new_all();
    system.refresh_all();

    let cpu_load = system.global_cpu_info().cpu_usage() as f64;
    let memory_usage = system.used_memory() as f64 / system.total_memory() as f64 * 100.0;

    Ok(NodeHealthMetrics {
        cpu_load,
        memory_usage,
    })
}

#[tonic::async_trait]
impl RshdfsDataNodeService for DataNode {
    async fn put_block_streamed(&self, request: Request<Streaming<BlockChunk>>) -> Result<Response<TransferStatus>, Status> {
        let mut stream = request.into_inner();

        // Grab the first block off the stream
        let first_chunk = match stream.next().await {
            Some(Ok(chunk)) => chunk,
            Some(Err(e)) => return Err(Status::aborted(format!("Failed to read chunk: {}", e))),
            None => return Err(Status::invalid_argument("No chunks received")),
        };

        // Create a file based on the first block's information
        let file_path = format!("{}/{}_{}.dat", self.data_dir, first_chunk.block_id, first_chunk.block_seq);
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
        let mut total_chunks_received = BLOCK_CHUNK_SIZE;
        while let Some(chunk_result) = stream.next().await {
            let block_chunk = match chunk_result {
                Ok(chunk) => chunk,
                Err(e) => return Err(Status::aborted(format!("Failed to read chunk: {}", e))),
            };

            println!("received chunk: block_id={}, seq={}, checksum ={}", block_chunk.block_id, block_chunk.block_seq, block_chunk.checksum);
            // Validate the block_chunk (e.g., checksum validation here)
            let mut adler = Adler32::new();
            adler.write_slice(&block_chunk.chunked_data);
            let checksum = adler.checksum();

            if block_chunk.checksum == checksum && seq == block_chunk.chunk_seq {

                // Write the chunk's data to the file
                if let Err(e) = file.write_all(&block_chunk.chunked_data).await {
                    return Err(Status::internal(format!("Failed to write to file: {}", e)));
                }
                total_chunks_received += block_chunk.chunked_data.len();
                seq += 1;
            }
        }
        println!("Total chunks: {}", total_chunks_received);

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

        let block_info = self.get_block_info(block_id_uuid).await?;
        let block_info_seq = block_info.block_seq;
        let file_path = PathBuf::from(format!("{}/{}_{}.dat", self.data_dir, block_id, block_info_seq));
        let mut file = File::open(&file_path).await
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

    async fn start_block_stream(&self, request: Request<BlockStreamInfo>) -> Result<Response<TransferStatus>, Status> {
        let block_stream_info = request.into_inner();
        let block_info = BlockInfo {
            block_seq: block_stream_info.block_seq,
        };
        let block_uuid = Uuid::parse_str(&block_stream_info.block_id).map_err(|_| RSHDFSError::UUIDError(String::from("Bad UUID string.")))?;
        let mut blocks_guard = self.blocks.write().await;

        blocks_guard.insert(block_uuid, block_info);
        Ok(Response::new(TransferStatus {
            success: true,
            message: "Block information updated.".to_string(),
        }))
    }
}