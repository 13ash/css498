use std::hash::Hasher;
use crate::error::RSHDFSError;
use crate::proto::{rshdfs_data_node_service_client::RshdfsDataNodeServiceClient, ReadBlockRequest, ReadResponse, WriteBlockRequest, WriteBlockResponse, WriteFileResponse};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tonic::{Request, Response};
use adler::Adler32;
pub async fn read_handler(response: ReadResponse) -> Result<Vec<u8>, RSHDFSError> {
    let blocks = response.file_blocks.clone();
    println!("{:?}", response.file_blocks);
    let mut valid_data = Vec::new(); // To store valid data

    for block in blocks.iter() {
        let mut valid_block_found = false;

        for addr in &block.datanodes {
            let datanode_client_future = RshdfsDataNodeServiceClient::connect(format!("http://{}", addr));
            let block_id = block.block_id.clone();

            let result = async move {
                let mut datanode_client = datanode_client_future.await?;
                let request = Request::new(ReadBlockRequest { block_id });

                match datanode_client.read_block_data(request).await {
                    Ok(response) => {
                        let inner_response = response.into_inner();
                        let read_checksum = inner_response.checksum;
                        let mut calculated_checksum_adler = Adler32::new();
                        calculated_checksum_adler.write_slice(&inner_response.block_data);
                        let calculated_checksum = calculated_checksum_adler.checksum();

                        if calculated_checksum == read_checksum {
                            Ok::<_, RSHDFSError>(inner_response.block_data)
                        } else {
                            Err(RSHDFSError::ChecksumError("Checksum Mismatch".to_string()))
                        }
                    }
                    Err(e) => Err(RSHDFSError::ReadError(e.to_string()))
                }
            }.await;

            match result {
                Ok(block_data) => {
                    valid_data.extend(block_data);
                    valid_block_found = true;
                    break; // Valid block found, no need to check other replicas
                }
                Err(_) => continue, // Checksum mismatch, try next replica
            }
        }

        if !valid_block_found {
            return Err(RSHDFSError::ReadError("Valid block replica not found".to_string()));
        }
    }

    Ok(valid_data)
}




pub async fn write_block_handler(
    mut local_file: File,
    response: WriteFileResponse,
) -> Result<Vec<String>, RSHDFSError> {
    const BLOCK_SIZE: usize = 128 * 1024 * 1024;
    let blocks = response.blocks;
    let mut block_id_vec = Vec::new();
    blocks.iter().for_each(|block| block_id_vec.push(block.clone().block_id));

    for block in blocks.iter() {
        // Read the next chunk of the file
        let mut data_chunk = vec![0; BLOCK_SIZE];
        let bytes_read = local_file.read(&mut data_chunk).await?;

        // End of file reached, no more data to send
        if bytes_read == 0 {
            break;
        }

        // Resize the buffer if less data is read
        if bytes_read < BLOCK_SIZE {
            data_chunk.resize(bytes_read, 0);
        }

        // Send the data chunk to each DataNode
        let mut write_futures = vec![];
        for addr in &block.datanodes {
            let mut block_data_alder = Adler32::new();
            block_data_alder.write(data_chunk.as_slice());
            let checksum = block_data_alder.checksum();
            let write_block_request = Request::new(WriteBlockRequest {
                block_id: block.block_id.clone(),
                seq: block.seq.clone(),
                data: data_chunk.clone(),
                checksum,
            });

            let client_future = send_to_datanode(addr.clone(), write_block_request.into_inner());
            write_futures.push(client_future);
        }

        // Await all futures to ensure all DataNodes receive the block
        let results = futures::future::try_join_all(write_futures).await;
        if let Err(e) = results {
            return Err(e); // Stop if any block fails to write
        }
    }

    Ok(block_id_vec)
}

async fn send_to_datanode(
    addr: String,
    request: WriteBlockRequest,
) -> Result<Response<WriteBlockResponse>, RSHDFSError> {
    let mut datanode_client =
        RshdfsDataNodeServiceClient::connect(format!("http://{}", addr)).await?;
    datanode_client
        .write_block_data(request)
        .await
        .map_err(|e| RSHDFSError::WriteError(e.to_string()))
}
