use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time;
use crate::proto::data_node_client::DataNodeClient;
use crate::proto::HeartBeatRequest;
use tonic::transport::Channel;
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


struct HeartbeatManager {
    datanode_service_client: Arc<Mutex<DataNodeClient<Channel>>>,
    interval: Duration,
    running: bool,
    id: u64,
}

impl HeartbeatManager {
    fn new(datanode_service_client: DataNodeClient<Channel>, interval: Duration, id: u64) -> Self {
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
            datanode_id: self.id
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
    id: u64,
}