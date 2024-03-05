use std::collections::HashMap;
use std::io::SeekFrom;

use crate::error::RSHDFSError;
use crate::proto::{BlockChunk, BlockStreamInfo, GetBlockRequest, GetResponse, PutFileResponse};
use adler::Adler32;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tonic::Request;

use crate::block::{BLOCK_CHUNK_SIZE, BLOCK_SIZE};
use crate::proto::rshdfs_data_node_service_client::RshdfsDataNodeServiceClient;
use async_trait::async_trait;
use std::path::PathBuf;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use crate::config::rshdfs_config::RSHDFSConfig;

#[async_trait]
pub trait Client {
    async fn get(&self, final_data_path: PathBuf, response: GetResponse)
        -> Result<(), RSHDFSError>;
    async fn put(
        &self,
        local_file: File,
        response: PutFileResponse,
    ) -> Result<Vec<String>, RSHDFSError>;
}

pub struct RSHDFSClient {
    pub (crate) namenode_addr: String,
    pub (crate) data_dir: String,
}
impl RSHDFSClient {
    pub fn from_config(config: RSHDFSConfig) -> Self {
        RSHDFSClient {
            namenode_addr: config.namenode_address,
            data_dir: config.data_dir,
        }
    }
}

#[async_trait]
impl Client for RSHDFSClient {
    async fn get(
        &self,
        final_data_path: PathBuf,
        response: GetResponse,
    ) -> Result<(), RSHDFSError> {
        let mut block_map = HashMap::new();
        let block_metadata_vec = response.file_blocks;
        let data_dir = self.data_dir.clone();

        for block in block_metadata_vec.iter() {
            let mut success = false;

            for addr in block.datanodes.iter() {
                let mut datanode_client =
                    RshdfsDataNodeServiceClient::connect(format!("http://{}", addr))
                        .await
                        .map_err(|e| RSHDFSError::ConnectionError(e.to_string()))?;
                let get_block_request = GetBlockRequest {
                    block_id: block.block_id.clone(),
                };

                let mut stream = datanode_client
                    .get_block_streamed(Request::new(get_block_request))
                    .await
                    .map_err(|e| RSHDFSError::GrpcError(e.to_string()))?
                    .into_inner();

                let mut block_buffer = Vec::new();

                while let Some(chunk_result) = stream
                    .message()
                    .await
                    .map_err(|e| RSHDFSError::GrpcError(e.to_string()))?
                {
                    let chunk = chunk_result;
                    let mut adler = Adler32::new();
                    adler.write_slice(&chunk.chunked_data);
                    if adler.checksum() == chunk.checksum {
                        block_buffer.extend_from_slice(&chunk.chunked_data);
                        success = true;
                    } else {
                        success = false;
                        break; // Checksum mismatch, try the next datanode
                    }
                }

                if success {
                    let temp_file_path = format!("{}/{}.tmp", data_dir, block.block_id);
                    let mut temp_file = File::create(&temp_file_path)
                        .await
                        .map_err(|e| RSHDFSError::IOError(e.to_string()))?;
                    temp_file
                        .write_all(&block_buffer)
                        .await
                        .map_err(|e| RSHDFSError::IOError(e.to_string()))?;
                    block_map.insert(block.seq, temp_file_path);
                    break;
                }
            }

            if !success {
                return Err(RSHDFSError::DataValidationError(
                    "Failed to validate block data from any datanode.".to_string(),
                ));
            }
        }

        let mut final_file = File::create(&final_data_path)
            .await
            .map_err(|e| RSHDFSError::IOError(e.to_string()))?;

        let mut keys: Vec<_> = block_map.keys().collect();
        keys.sort();

        for key in keys {
            if let Some(temp_file_path) = block_map.get(key) {
                let mut temp_file = File::open(temp_file_path)
                    .await
                    .map_err(|e| RSHDFSError::IOError(e.to_string()))?;
                let mut data_buffer = Vec::new();
                temp_file
                    .read_to_end(&mut data_buffer)
                    .await
                    .map_err(|e| RSHDFSError::IOError(e.to_string()))?;
                final_file
                    .write_all(&data_buffer)
                    .await
                    .map_err(|e| RSHDFSError::IOError(e.to_string()))?;
                tokio::fs::remove_file(temp_file_path).await?;
            }
        }

        Ok(())
    }

    async fn put(
        &self,
        mut local_file: File,
        response: PutFileResponse,
    ) -> Result<Vec<String>, RSHDFSError> {
        const MAX_READ_BUFFER_SIZE: usize = 1024; // 1KB read buffer size

        let blocks = response.blocks;

        for block in blocks.iter().cloned() {
            let offset = block.seq as u64 * BLOCK_SIZE as u64;
            local_file
                .seek(SeekFrom::Start(offset))
                .await
                .map_err(|_| {
                    RSHDFSError::FileSystemError(String::from("Failed to seek in file"))
                })?;

            let mut block_buffer = vec![0; BLOCK_SIZE];
            let mut total_read = 0;

            // Read into block_buffer in 2MB increments until it's filled or EOF
            loop {
                if total_read >= BLOCK_SIZE {
                    break; // Exit if the block buffer is full
                }

                let mut read_buffer = vec![0; MAX_READ_BUFFER_SIZE];
                let read_bytes = local_file.read(&mut read_buffer).await.map_err(|_| {
                    RSHDFSError::FileSystemError(String::from("Failed to read from file"))
                })?;

                if read_bytes == 0 {
                    break; // EOF reached
                }

                for byte in 0..read_bytes {
                    block_buffer[total_read + byte] = read_buffer[byte]
                }
                total_read += read_bytes;
            }
            block_buffer.truncate(total_read);
            let buffer_size = block_buffer.len();

            for addr in block.datanodes.clone() {
                let mut datanode_client =
                    RshdfsDataNodeServiceClient::connect(format!("http://{}", addr))
                        .await
                        .map_err(|_| {
                            RSHDFSError::ConnectionError(String::from("unable to connect"))
                        })?;

                let start_put_block_stream_request = Request::new(BlockStreamInfo {
                    block_id: block.block_id.clone(),
                    block_seq: block.seq,
                });
                let response = datanode_client
                    .start_block_stream(start_put_block_stream_request)
                    .await;
                if response.is_err() {
                    println!("{:?}", response);
                    return Err(RSHDFSError::WriteError(response.err().unwrap().to_string()));
                }

                let (client, server) = mpsc::channel::<BlockChunk>(10);
                let receiver_stream = ReceiverStream::new(server);
                let block_buffer_clone = block_buffer.clone();
                let block_id = block.block_id.clone();
                let block_seq = block.seq.clone();

                tokio::spawn(async move {
                    let mut seq = 0;
                    for chunk_start in (0..buffer_size).step_by(BLOCK_CHUNK_SIZE) {
                        let chunk_end = usize::min(chunk_start + BLOCK_CHUNK_SIZE, buffer_size);
                        let chunk = &block_buffer_clone[chunk_start..chunk_end];

                        let mut adler = Adler32::new();
                        adler.write_slice(&chunk);
                        let checksum = adler.checksum();

                        let block_chunk = BlockChunk {
                            block_id: block_id.clone(),
                            block_seq,
                            chunk_seq: seq,
                            chunked_data: chunk.to_vec(),
                            checksum,
                        };
                        seq += 1;
                        client
                            .send(block_chunk)
                            .await
                            .expect("Failed to send chunk");
                    }
                });

                datanode_client
                    .put_block_streamed(Request::new(receiver_stream))
                    .await
                    .map_err(|e| RSHDFSError::PutBlockStreamedError(e.message().to_string()))?;
            }
        }

        Ok(blocks.iter().map(|block| block.block_id.clone()).collect())
    }
}
