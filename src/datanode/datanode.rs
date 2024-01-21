use crate::config::datanode_config::DataNodeConfig;
use crate::proto::data_node_service_client::DataNodeServiceClient;
use crate::proto::data_node_service_server::DataNodeService;
use crate::proto::{HeartBeatRequest, RegistrationRequest, RegistrationResponse};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time;
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

use crate::error::RSHDFSError;

/// Responsibilities
///
/// Registering with the NameNode
///
/// Handling block management operations (reading, writing, replicating)
///
/// Sending heartbeats and block reports to the NameNode
///
/// Handling client requests for block data

struct BlockManager {}

struct HeartbeatManager {
    datanode_service_client: Arc<Mutex<DataNodeServiceClient<Channel>>>,
    interval: Duration,
    running: bool,
    id: Uuid,
}

impl HeartbeatManager {
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

    async fn start(&mut self) {
        self.running = true;
        let client = self.datanode_service_client.clone();
        let interval = self.interval;
        let request = HeartBeatRequest {
            datanode_id: self.id.to_string(),
        };

        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval);
            loop {
                interval_timer.tick().await;
                let mut client_guard = client.lock().await;
                println!("Sending heartbeat.");
                match client_guard.send_heart_beat(request.clone()).await {
                    Ok(response) => {
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
    async fn register_with_namenode(
        &self,
        data_node_client: &mut DataNodeServiceClient<Channel>,
    ) -> Result<RegistrationResponse, RSHDFSError> {
        let registration_request = RegistrationRequest {
            datanode_id: self.id.to_string(),
        };

        match data_node_client
            .register_with_namenode(registration_request.clone())
            .await
        {
            Ok(response) => Ok(response.into_inner()),
            Err(_e) => Err(RSHDFSError::RegistrationFailed(String::from(
                "Registration with the NameNode failed.",
            ))),
        }
    }

    pub async fn from_config(config: DataNodeConfig) -> Result<Self, RSHDFSError> {
        println!("Attempting to establish connection with: {}", config.namenode_address);
        let id = Uuid::new_v4();
        let ipc_address = config.ipc_address.clone();
        let namenode_addr = config.namenode_address.clone();
        let endpoint = Endpoint::from_shared(format!("http://{}",config.namenode_address))?;
        let client= DataNodeServiceClient::connect(endpoint).await?;
        let wrapped_client = Arc::new(Mutex::new(client));
        let heartbeat_client= wrapped_client.clone();
        let heartbeat_manager = HeartbeatManager::new(
            heartbeat_client,
            Duration::from_millis(config.heartbeat_interval),
            id,
        );

        Ok(DataNode {
            id,
            client: wrapped_client,
            data_dir: config.data_dir,
            heartbeat_interval: Duration::from_millis(config.heartbeat_interval),
            block_report_interval: Duration::from_millis(config.block_report_interval),
            addr: ipc_address,
            namenode_addr,
            heartbeat_manager,
        })
    }

    pub async fn start(&mut self) -> Result<(), RSHDFSError> {
        // Step 1: Register with NameNode
        let mut client_guard = self.client.lock().await;
        let registration_response = self.register_with_namenode(&mut client_guard).await?;
        drop(client_guard);
        println!("Registered with NameNode: {:?}", registration_response);
        println!("Starting Heartbeat...");

        // Step 2: Start HeartbeatManager
        self.heartbeat_manager.start().await;

        // Return Ok if everything is initialized properly
        Ok(())
    }
}
