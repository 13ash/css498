use std::collections::{HashMap};
use std::path::PathBuf;
use std::str::FromStr;

use crate::config::datanode_config::DataNodeConfig;
use crate::error::RSHDFSError;

use crate::proto::data_node_name_node_service_client::DataNodeNameNodeServiceClient;
use crate::proto::rshdfs_data_node_service_server::RshdfsDataNodeService;
use crate::proto::{
    HeartBeatRequest, NodeHealthMetrics, ReadBlockRequest, ReadBlockResponse, RegistrationRequest,
    RegistrationResponse, WriteBlockRequest, WriteBlockResponse,
};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use adler::Adler32;
use sysinfo::System;
use tokio::sync::Mutex;
use tokio::time;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};
use uuid::Uuid;

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

#[derive(Clone, Copy)]
struct BlockInfo {
    seq: i32,
    checksum: u32,
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
        drop(client_guard); // Explicitly drop to release the lock.
        println!("Registered with NameNode: {:?}", registration_response);
        println!("Starting Heartbeat...");

        // Step 2: Start HeartbeatManager
        self.heartbeat_manager.start().await;

        Ok(())
    }

    async fn read_block(&self, block_id: String) -> Result<BlockPayload, RSHDFSError> {
        // Parse the UUID first to handle any potential errors
        let block_uuid = Uuid::from_str(&block_id)
            .map_err(|_| RSHDFSError::FileSystemError("Invalid Block ID".to_string()))?;

        // Scope to limit the lifetime of the lock guard
        let block_info = {
            let block_set_read_guard = self.blocks.read().map_err(|_| RSHDFSError::LockError("Failed to acquire read lock".to_string()))?;
            match block_set_read_guard.get(&block_uuid) {
                Some(&block_info) => block_info,
                None => return Err(RSHDFSError::FileSystemError("Block not found.".to_string())),
            }
        }; // Lock is dropped here

        // Now perform the asynchronous file read operation
        let path = PathBuf::from(format!("{}/{}_{}.dat", self.data_dir, block_id, block_info.seq));
        match tokio::fs::read(path).await {
            Ok(data) => Ok(BlockPayload {seq: block_info.seq, checksum: block_info.checksum, data}),
            Err(_) => Err(RSHDFSError::FileSystemError("Error reading block data.".to_string())),
        }
    }


    async fn write_block(&self, block_id: String, seq: i32, data: Vec<u8>, data_checksum: u32) -> Result<(), RSHDFSError> {
        let path = PathBuf::from(format!("{}/{}_{}.dat", self.data_dir, block_id, seq));
        match tokio::fs::write(path, data).await {
            Ok(_) => {
                let mut block_set_write_guard = self.blocks.write().map_err(|_| RSHDFSError::LockError("Failed to acquire write lock".to_string()))?;
                block_set_write_guard.insert(Uuid::from_str(block_id.as_str()).unwrap(), BlockInfo {
                    seq,
                    checksum: data_checksum
                });
                Ok(())
            },
            Err(_) => Err(RSHDFSError::FileSystemError(
                "Error reading block data.".to_string(),
            )),
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
    async fn read_block_data(
        &self,
        request: Request<ReadBlockRequest>,
    ) -> Result<Response<ReadBlockResponse>, Status> {
        let inner_request = request.into_inner();
        match self
            .read_block(inner_request.block_id)
            .await
        {
            Ok(block_data) => Ok(Response::new(ReadBlockResponse { seq: block_data.seq, block_data: block_data.data, checksum: block_data.checksum })),
            Err(e) => return Err(Status::from(e)),
        }
    }

    async fn write_block_data(
        &self,
        request: Request<WriteBlockRequest>,
    ) -> Result<Response<WriteBlockResponse>, Status> {
        let inner_request = request.into_inner();
        let request_checksum = inner_request.checksum;
        let request_block = inner_request.data.clone();

        let mut request_block_adler = Adler32::new();
        request_block_adler.write_slice(&request_block);
        let calculated_checksum = request_block_adler.checksum();

        if request_checksum == calculated_checksum {
            // Proceed with writing the block
            match self
                .write_block(
                    inner_request.block_id,
                    inner_request.seq,
                    request_block,
                    calculated_checksum
                )
                .await
            {
                Ok(_) => Ok(Response::new(WriteBlockResponse {
                    success: true,
                    message: "Block Written to DataNode".to_string(),
                })),
                Err(e) => Err(Status::from(e)),
            }
        } else {
            // Checksum mismatch
            Err(Status::internal("Checksum Mismatch"))
        }
    }
}

