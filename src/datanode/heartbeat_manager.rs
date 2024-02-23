use crate::error::RSHDFSError;
use crate::proto::data_node_name_node_service_client::DataNodeNameNodeServiceClient;
use crate::proto::HeartBeatRequest;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time;
use tonic::transport::Channel;
use uuid::Uuid;

/// Manages the sending of heartbeats to the NameNode at regular intervals.
pub struct HeartbeatManager {
    datanode_service_client: Arc<Mutex<DataNodeNameNodeServiceClient<Channel>>>,
    interval: Duration,
    running: bool,
    id: Uuid,
}

impl HeartbeatManager {
    pub fn new(
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
    pub async fn start(&mut self) {
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
