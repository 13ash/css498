use crate::config::namenode_config::NameNodeConfig;
use crate::error::RSHDFSError;
use crate::proto::data_node_service_server::DataNodeService;
use crate::proto::{
    HeartBeatRequest, HeartBeatResponse, RegistrationRequest, RegistrationResponse,
};
use tonic::{Request, Response, Status};
use uuid::Uuid;

enum Role {
    PRIMARY,
    BACKUP,
}

pub struct NameNode {
    id: Uuid,
    data_dir: String,
    ipc_address: String,
    checkpoint_interval: u16,
    handler_count: u8,
}

impl NameNode {
    pub async fn from_config(config: NameNodeConfig) -> Result<Self, RSHDFSError> {
        let id = Uuid::new_v4();

        Ok(NameNode {
            id,
            data_dir: config.data_dir,
            ipc_address: config.ipc_address,
            checkpoint_interval: config.checkpoint_period,
            handler_count: config.handler_count,
        })
    }
}

#[tonic::async_trait]
impl DataNodeService for NameNode {
    async fn send_heart_beat(
        &self,
        request: Request<HeartBeatRequest>,
    ) -> Result<Response<HeartBeatResponse>, Status> {
        println!(
            "Received heartbeat from {:?}",
            request.into_inner().datanode_id
        );
        let response = HeartBeatResponse { success: true };
        Ok(Response::new(response))
    }

    async fn register_with_namenode(
        &self,
        request: Request<RegistrationRequest>,
    ) -> Result<Response<RegistrationResponse>, Status> {
        println!(
            "Received registration {:?}",
            request
        );
        let unwrapped_request = request.into_inner().health_metrics.unwrap();
        let success = unwrapped_request.cpu_load < 3.0 && unwrapped_request.memory_usage < 50.0 ;
        let response = RegistrationResponse {
            success};
        Ok(Response::new(response))
    }
}
