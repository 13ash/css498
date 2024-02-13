use std::collections::HashMap;
use std::io::SeekFrom;

use crate::error::RSHDFSError;
use crate::proto::{rshdfs_data_node_service_client::RshdfsDataNodeServiceClient, GetResponse, GetBlockRequest, PutFileResponse, BlockChunk, BlockStreamInfo};
use tokio::fs::File;
use tokio::sync::mpsc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tonic::{Request};
use adler::Adler32;

use std::path::{PathBuf};
use tonic::codegen::Body;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use crate::block::{BLOCK_CHUNK_SIZE, BLOCK_SIZE};



pub async fn get_handler(final_data_path: PathBuf, response: GetResponse) -> Result<(), RSHDFSError> {
    let mut block_map = HashMap::new();
    let block_metadata_vec = response.file_blocks;

    for block in block_metadata_vec.iter() {
        let mut success = false;

        for addr in block.datanodes.iter() {
            let mut datanode_client = RshdfsDataNodeServiceClient::connect(format!("http://{}", addr))
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

            while let Some(chunk_result) = stream.message().await.map_err(|e| RSHDFSError::GrpcError(e.to_string()))? {
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
                let temp_file_path = format!("/tmp/{}.tmp", block.block_id);
                let mut temp_file = File::create(&temp_file_path).await.map_err(|e| RSHDFSError::IOError(e.to_string()))?;
                temp_file.write_all(&block_buffer).await.map_err(|e| RSHDFSError::IOError(e.to_string()))?;
                block_map.insert(block.seq, temp_file_path);
                break;
            }
        }

        if !success {
            return Err(RSHDFSError::DataValidationError("Failed to validate block data from any datanode.".to_string()));
        }
    }

    let mut final_file = File::create(&final_data_path).await.map_err(|e| RSHDFSError::IOError(e.to_string()))?;

    let mut keys: Vec<_> = block_map.keys().collect();
    keys.sort();

    for key in keys {
        if let Some(temp_file_path) = block_map.get(key) {
            let mut temp_file = File::open(temp_file_path).await.map_err(|e| RSHDFSError::IOError(e.to_string()))?;
            let mut data_buffer = Vec::new();
            temp_file.read_to_end(&mut data_buffer).await.map_err(|e| RSHDFSError::IOError(e.to_string()))?;
            final_file.write_all(&data_buffer).await.map_err(|e| RSHDFSError::IOError(e.to_string()))?;
        }
    }

    Ok(())
}

pub async fn put_handler(mut local_file: File, response: PutFileResponse) -> Result<Vec<String>, RSHDFSError> {
    let blocks = response.blocks.clone();

    for block in blocks.iter().cloned() {
        let offset = block.seq as u64 * BLOCK_SIZE as u64;
        local_file.seek(SeekFrom::Start(offset)).await
            .map_err(|_| RSHDFSError::FileSystemError(String::from("Failed to seek in file")))?;


        let mut block_buffer = vec![0; BLOCK_SIZE];
        println!("{}", block_buffer.len());
        let buffer_size = local_file.read_exact(&mut block_buffer).await?;
        println!("buffer_size: {}", buffer_size);
        block_buffer.truncate(buffer_size);

        for addr in block.datanodes.clone() {
            let mut datanode_client = RshdfsDataNodeServiceClient::connect(format!("http://{}", addr)).await
                .map_err(|_| RSHDFSError::ConnectionError(String::from("unable to connect")))?;

            let start_put_block_stream_request = Request::new(BlockStreamInfo {
                block_id: block.block_id.clone(),
                block_seq: block.seq,
            });
            let response = datanode_client.start_block_stream(start_put_block_stream_request).await;
            if response.is_err() {
                return Err(RSHDFSError::WriteError("Failed to write.".to_string()));
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
                    client.send(block_chunk).await.expect("Failed to send chunk");
                }
            });

            datanode_client.put_block_streamed(Request::new(receiver_stream)).await
                .map_err(|e| RSHDFSError::GrpcError(e.to_string()))?;
        }
    }

    Ok(blocks.iter().map(|block| block.block_id.clone()).collect())
}
  