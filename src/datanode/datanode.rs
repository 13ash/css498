use crate::config::datanode_config::DataNodeConfig;
use crate::datanode::block_manager::BlockManager;
use crate::datanode::heartbeat_manager::HeartbeatManager;
use crate::error::RSHDFSError;
use crate::proto::data_node_name_node_service_client::DataNodeNameNodeServiceClient;
use crate::proto::{NodeHealthMetrics, RegistrationRequest, RegistrationResponse};
use std::sync::Arc;
use std::time;
use sysinfo::System;
use time::Duration;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

/// Represents a DataNode in a distributed file system.
pub struct DataNode {
    id: Uuid,
    data_dir: String,
    client: Arc<Mutex<DataNodeNameNodeServiceClient<Channel>>>,
    heartbeat_interval: Duration,
    block_report_interval: Duration,
    addr: String,
    port: String,
    namenode_addr: String,
    heartbeat_manager: HeartbeatManager,
    pub block_manager: Arc<BlockManager>,
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
        let block_manager = Arc::new(BlockManager::new(
            wrapped_client.clone(),
            Duration::from_millis(config.block_report_interval),
            config.data_dir.clone(),
        ));

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
            block_manager,
        })
    }

    /// Starts the main operations of the DataNode, including registration and heartbeat sending.
    pub async fn start(&mut self) -> Result<(), RSHDFSError> {
        // Step 1: Register with NameNode
        let mut client_guard = self.client.lock().await;
        let registration_response = self.register_with_namenode(&mut client_guard).await?;
        drop(client_guard);
        println!(
            "Registered with NameNode: {}",
            registration_response.success
        );
        println!("Starting HeartbeatManager...");
        println!("Starting BlockReportManager...");

        // Step 2: Start HeartbeatManager and BlockReportManager
        self.heartbeat_manager.start().await;
        self.block_manager.clone().start().await;

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
