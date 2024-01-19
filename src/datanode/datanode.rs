use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time;
use tonic::transport::{Channel, Uri};
use uuid::Uuid;
use crate::config::datanode_config::DataNodeConfig;
use crate::proto::data_node_client::DataNodeClient;
use crate::proto::{HeartBeatRequest, RegistrationRequest, RegistrationResponse};

use crate::error::{RSHDFSError};


/// Responsibilities
///
/// Registering with the NameNode
///
/// Handling block management operations (reading, writing, replicating)
///
/// Sending heartbeats and block reports to the NameNode
///
/// Handling client requests for block data

struct BlockManager {


}

struct HeartbeatManager {
    datanode_service_client: Arc<Mutex<DataNodeClient<Channel>>>,
    interval: Duration,
    running: bool,
    id: Uuid,
}

impl HeartbeatManager {
    fn new(datanode_service_client: DataNodeClient<Channel>, interval: Duration, id: Uuid) -> Self {
        HeartbeatManager {
            datanode_service_client: Arc::new(Mutex::new(datanode_service_client)),
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
            datanode_id: self.id.to_string()
        };

        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval);
            loop {
                interval_timer.tick().await;
                let mut client_guard = client.lock().await;
                match client_guard.send_heart_beat(request.clone()).await {
                    Ok(response) => {
                        response.into_inner();
                    }
                    Err(e) => {
                        RSHDFSError::HeartBeatFailed(String::from("Heartbeat Failed."));
                        break;
                    }
                }
            }
        });
    }
}



pub struct DataNode {
    id: Uuid,
    data_dir: String,
    heartbeat_interval: Duration,
    block_report_interval: Duration,
    addr: String,
    heartbeat_manager: HeartbeatManager,
}

impl DataNode {
    pub async fn register_with_namenode(&self, data_node_client: &mut DataNodeClient<Channel>) -> Result<RegistrationResponse, RSHDFSError> {
        let registration_request = RegistrationRequest {
            datanode_id: self.id.to_string(),
        };

        match (data_node_client.register_with_namenode(registration_request.clone()).await) {
            Ok(response) => {
                Ok(response.into_inner())
            }
            Err(e) => {
                Err(RSHDFSError::RegistrationFailed(String::from("Registration with the namenode failed.")))
            }
        }
    }

    pub async fn from_config(config: DataNodeConfig) -> Result<Self, RSHDFSError> {
        let uri = Uri::from_str(&config.ipc_address)
            .map_err(|_| RSHDFSError::ConfigError("Invalid URI for DataNode address".to_string()))?;

        let channel = Channel::builder(uri)
            .connect_lazy();
        let client = DataNodeClient::new(channel);
        let id = Uuid::new_v4();

        Ok(DataNode {
            id,
            data_dir: config.data_dir,
            heartbeat_interval: Duration::from_millis(config.heartbeat_interval),
            block_report_interval: Duration::from_millis(config.block_report_interval),
            addr: config.ipc_address,
            heartbeat_manager: HeartbeatManager::new(client, Duration::from_millis(config.heartbeat_interval), id)
        })
    }
}