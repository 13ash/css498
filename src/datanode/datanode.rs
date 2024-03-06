use crate::block::BLOCK_CHUNK_SIZE;
use crate::config::datanode_config::DataNodeConfig;
use crate::error::RSHDFSError;
use crate::proto::data_node_name_node_service_client::DataNodeNameNodeServiceClient;
use crate::proto::rshdfs_data_node_service_server::RshdfsDataNodeService;
use crate::proto::{
    BlockChunk, BlockReportRequest, BlockStreamInfo, GetBlockRequest, HeartBeatRequest,
    NodeHealthMetrics, RegistrationRequest, RegistrationResponse, TransferStatus,
};
use adler::Adler32;
use async_trait::async_trait;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use sysinfo::{Disks, System};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info};
use uuid::Uuid;

pub(crate) struct BlockInfo {
    pub(crate) id: String,
    pub(crate) seq: i32,
}

impl PartialEq<Self> for BlockInfo {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.seq == other.seq
    }
}

impl Eq for BlockInfo {}

impl Hash for BlockInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(Default)]
pub(crate) struct HealthMetrics {
    pub(crate) cpu_load: f32,
    pub(crate) memory_usage: u64,
    pub(crate) disk_space: u64,
}

impl HealthMetrics {
    pub fn init() -> Self {
        HealthMetrics {
            cpu_load: 0.0,
            memory_usage: 0,
            disk_space: 0,
        }
    }
}

/// Represents a DataNode in a distributed file system.
pub struct DataNode {
    pub(crate) id: Uuid,
    pub(crate) data_dir: String,
    pub(crate) hostname_port: String,
    pub(crate) datanode_namenode_service_client: Arc<Mutex<DataNodeNameNodeServiceClient<Channel>>>,
    pub(crate) blocks: Arc<RwLock<HashMap<Uuid, BlockInfo>>>,
    pub(crate) metrics: Arc<Mutex<HealthMetrics>>,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) block_report_interval: Duration,
    pub(crate) metrics_interval: Duration,
}

impl DataNode {
    pub async fn from_config(config: DataNodeConfig) -> Self {
        DataNode {
            id: Uuid::new_v4(),
            data_dir: config.data_dir,
            hostname_port: format!("{}:{}", System::host_name().unwrap(), config.port),
            datanode_namenode_service_client: Arc::new(Mutex::new(
                DataNodeNameNodeServiceClient::connect(format!(
                    "http://{}",
                    config.namenode_address
                ))
                .await
                .unwrap(),
            )),
            blocks: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_interval: Duration::from_millis(config.heartbeat_interval),
            metrics: Arc::new(Mutex::new(HealthMetrics::init())),
            block_report_interval: Duration::from_millis(config.block_report_interval),
            metrics_interval: Duration::from_millis(config.metrics_interval),
        }
    }
    pub async fn start(&mut self) -> Result<(), RSHDFSError> {
        self.register_with_namenode().await?;
        self.start_heartbeat_worker().await;
        self.start_metrics_worker().await;
        self.start_block_report_worker().await;

        Ok(())
    }
}

#[async_trait]
trait DataNodeManager {
    async fn start_heartbeat_worker(&self);
    async fn start_block_report_worker(&self);
    async fn start_metrics_worker(&self);
    async fn register_with_namenode(&self) -> Result<Response<RegistrationResponse>, RSHDFSError>;
}

#[async_trait]
impl DataNodeManager for DataNode {
    async fn start_heartbeat_worker(&self) {
        let interval = self.heartbeat_interval;
        let client = self.datanode_namenode_service_client.clone();
        let metrics = self.metrics.clone();
        let datanode_id_str = self.id.to_string();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;
                let metrics_guard = metrics.lock().await;

                let request = HeartBeatRequest {
                    datanode_id: datanode_id_str.clone(),
                    health_metrics: Some(NodeHealthMetrics {
                        cpu_load: metrics_guard.cpu_load,
                        memory_usage: metrics_guard.memory_usage,
                        disk_space: metrics_guard.disk_space,
                    }),
                };
                drop(metrics_guard);
                info!("Sending Heartbeat...");
                let result = client.lock().await.send_heart_beat(request).await;

                if let Err(e) = result {
                    error!(
                        "Heartbeat failed: {:?}",
                        RSHDFSError::HeartBeatFailed(e.to_string())
                    );
                }
            }
        });
    }

    async fn start_block_report_worker(&self) {
        let interval = self.block_report_interval.clone();
        let blocks = self.blocks.clone();
        let client = self.datanode_namenode_service_client.clone();
        let data_dir = self.data_dir.clone();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;
                let blocks_read_guard = blocks.read().await;
                let mut request_vec = vec![];

                for block_info in blocks_read_guard.values() {
                    request_vec.push(block_info.id.clone());
                }

                drop(blocks_read_guard);

                let request = BlockReportRequest {
                    block_ids: request_vec,
                };

                info!("Sending Block Report....");
                match client.lock().await.send_block_report(request).await {
                    Ok(response) => {
                        let mut blocks_write_guard = blocks.write().await;
                        let inner_response = response.into_inner();

                        for block_id_str in inner_response.block_ids.iter() {
                            let block_id = Uuid::parse_str(block_id_str)
                                .expect("Invalid UUID in block report response");
                            if let Some(block) = blocks_write_guard.get(&block_id) {
                                let file_path =
                                    format!("{}/{}_{}.dat", data_dir, block.id, block.seq);
                                if let Err(e) = tokio::fs::remove_file(&file_path).await {
                                    error!("Failed to remove file {}: {}", file_path, e);
                                } else {
                                    blocks_write_guard.remove(&block_id);
                                }
                            } else {
                                error!("Block not found in local storage: {}", block_id);
                            }
                        }
                    }
                    Err(_) => error!("Block Report Response Error"),
                }
            }
        });
    }

    async fn start_metrics_worker(&self) {
        let metrics = self.metrics.clone();
        let interval = self.metrics_interval;
        let data_dir = self.data_dir.clone();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;
                let mut metrics_guard = metrics.lock().await;

                let system_state = System::new_all();
                match Disks::new_with_refreshed_list()
                    .list()
                    .iter()
                    .find(|disk| disk.mount_point() == OsStr::new(&data_dir))
                {
                    None => {
                        error!("Mount point not found.");
                        drop(metrics_guard);
                    }
                    Some(disk) => {
                        metrics_guard.cpu_load = system_state.global_cpu_info().cpu_usage();
                        metrics_guard.memory_usage =
                            system_state.available_memory() / system_state.total_memory() * 100;
                        metrics_guard.disk_space = disk.available_space();
                        drop(metrics_guard);
                    }
                }
            }
        });
    }

    async fn register_with_namenode(&self) -> Result<Response<RegistrationResponse>, RSHDFSError> {
        let metrics_guard = self.metrics.lock().await;
        let request = RegistrationRequest {
            datanode_id: self.id.to_string(),
            hostname_port: self.hostname_port.clone(),
            health_metrics: Some(NodeHealthMetrics {
                cpu_load: metrics_guard.cpu_load,
                memory_usage: metrics_guard.memory_usage,
                disk_space: metrics_guard.disk_space,
            }),
        };
        drop(metrics_guard);
        match self
            .datanode_namenode_service_client
            .lock()
            .await
            .register_with_namenode(request)
            .await
            .map_err(|_| {
                RSHDFSError::RegistrationError(String::from("Failed to register with Namenode."))
            }) {
            Ok(response) => Ok(response),
            Err(e) => Err(e),
        }
    }
}

#[tonic::async_trait]
impl RshdfsDataNodeService for DataNode {
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
            Err(e) => return Err(Status::from(RSHDFSError::PutBlockStreamedError(format!("Failed to create file: {}", e)))),
        };

        // Write the first chunk's data to the file
        if let Err(e) = file.write_all(&first_chunk.chunked_data).await {
            return Err(Status::from(RSHDFSError::PutBlockStreamedError(format!("Failed to write to file: {}", e))));
        }

        // Loop to process remaining chunks
        let mut seq = 1;
        while let Some(chunk_result) = stream.next().await {
            let block_chunk = match chunk_result {
                Ok(chunk) => chunk,
                Err(e) => return Err(Status::from(RSHDFSError::PutBlockStreamedError(format!("Failed to read chunk: {}", e)))),
            };

            // Validate the block_chunk
            let mut adler = Adler32::new();
            adler.write_slice(&block_chunk.chunked_data);
            let checksum = adler.checksum();

            if block_chunk.checksum == checksum && seq == block_chunk.chunk_seq {
                // Write the chunk's data to the file
                if let Err(e) = file.write_all(&block_chunk.chunked_data).await {
                    return Err(Status::from(RSHDFSError::PutBlockStreamedError(format!("Failed to write to file: {}", e))));
                }
                seq += 1;
            }

            else {
                return Err(Status::from(RSHDFSError::PutBlockStreamedError("Checksum mismatch".to_string())))
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
            .map_err(|_| Status::from(RSHDFSError::GetBlockStreamedError("Invalid UUID format.".to_string())))?;

        let blocks_guard = self.blocks.read().await;
        let block_info = blocks_guard
            .get(&block_id_uuid)
            .ok_or(Status::from(RSHDFSError::GetBlockStreamedError("Block not found.".to_string())))?;
        let block_info_seq = block_info.seq;
        let file_path = PathBuf::from(format!(
            "{}/{}_{}.dat",
            self.data_dir, block_id, block_info_seq
        ));
        let mut file = File::open(&file_path).await.map_err(|_| {
            Status::from(RSHDFSError::GetBlockStreamedError(format!("File at {:?} cannot be opened.", file_path)))
        })?;

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
        let block_uuid = Uuid::parse_str(&block_stream_info.block_id)
            .map_err(|_| RSHDFSError::UUIDError(String::from("Bad UUID string.")))?;
        let block_info = BlockInfo {
            id: block_uuid.to_string(),
            seq: block_stream_info.block_seq,
        };

        let mut blocks_guard = self.blocks.write().await;
        blocks_guard.insert(block_uuid, block_info);
        Ok(Response::new(TransferStatus {
            success: true,
            message: "Block information updated.".to_string(),
        }))
    }
}
