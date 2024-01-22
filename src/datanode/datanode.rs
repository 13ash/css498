use crate::block::Block;
use crate::config::datanode_config::DataNodeConfig;
use crate::error::RSHDFSError;
use crate::proto::data_node_service_client::DataNodeServiceClient;
use crate::proto::data_node_service_server::DataNodeService;
use crate::proto::{
    HeartBeatRequest, NodeHealthMetrics, RegistrationRequest, RegistrationResponse,
};
use std::sync::Arc;
use std::time::Duration;
use sysinfo::System;
use tokio::sync::Mutex;
use tokio::time;
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

/// Manages block operations such as reading, writing, and replication.
struct BlockManager {
    blocks: Vec<Block>,
}

/// Manages the sending of heartbeats to the NameNode at regular intervals.
struct HeartbeatManager {
    datanode_service_client: Arc<Mutex<DataNodeServiceClient<Channel>>>,
    interval: Duration,
    running: bool,
    id: Uuid,
}

impl HeartbeatManager {
    /// Constructs a new HeartbeatManager.
    fn new(
        datanode_service_client: Arc<Mutex<DataNodeServiceClient<Channel>>>,
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
    client: Arc<Mutex<DataNodeServiceClient<Channel>>>,
    heartbeat_interval: Duration,
    block_report_interval: Duration,
    addr: String,
    namenode_addr: String,
    heartbeat_manager: HeartbeatManager,
}

impl DataNode {
    /// Registers the DataNode with the NameNode and provides health metrics.
    async fn register_with_namenode(
        &self,
        data_node_client: &mut DataNodeServiceClient<Channel>,
    ) -> Result<RegistrationResponse, RSHDFSError> {
        let node_health_metrics = gather_metrics().await?;
        let registration_request = RegistrationRequest {
            datanode_id: self.id.to_string(),
            addr: self.addr.clone(),
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
        let client = DataNodeServiceClient::connect(endpoint).await?;
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
            namenode_addr: config.namenode_address,
            heartbeat_manager,
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

        // Return Ok if everything is initialized properly
        Ok(())
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
