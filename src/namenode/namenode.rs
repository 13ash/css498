use std::str::FromStr;
use std::sync::{Arc, RwLock};
use tonic::{ Request, Response, Status};
use uuid::Uuid;

use crate::config::namenode_config::NameNodeConfig;
use crate::datanode::block_report::BlockReport;
use crate::error::RSHDFSError;
use crate::proto;
use crate::proto::data_node_service_server::DataNodeService;
use crate::proto::{
    BlockReportRequest, BlockReportResponse, HeartBeatRequest, HeartBeatResponse,
    RegistrationRequest, RegistrationResponse, CreateRequest, CreateResponse
};
use crate::proto::client_protocol_server::ClientProtocol;

/// Represents the possible states of a DataNode.
enum DataNodeStatus {
    HEALTHY,
    UNHEALTHY,
    DEFAULT,
}

/// Represents a DataNode in the distributed file system.
struct DataNode {
    id: Uuid,
    status: DataNodeStatus,
    block_report: BlockReport,
}

impl PartialEq for DataNode {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl DataNode {
    /// Creates a new instance of DataNode.
    fn new(id: Uuid, status: DataNodeStatus, block_report: BlockReport) -> Self {
        DataNode {
            id,
            status,
            block_report,
        }
    }
}

/// Represents the NameNode in the distributed file system.

pub struct NameNode {
    id: Uuid,
    data_dir: String,
    ipc_address: String,
    checkpoint_interval: u16,
    handler_count: u8,
    datanodes: RwLock<Vec<DataNode>>,
}

impl NameNode {
    /// Creates a new instance of NameNode from configuration.
    pub async fn from_config(config: NameNodeConfig) -> Result<Self, RSHDFSError> {
        let id = Uuid::new_v4();

        Ok(NameNode {
            id,
            data_dir: config.data_dir,
            ipc_address: config.ipc_address,
            checkpoint_interval: config.checkpoint_period,
            handler_count: config.handler_count,
            datanodes: RwLock::new(Vec::new()),
        })
    }
}

#[tonic::async_trait]
impl DataNodeService for Arc<NameNode> {
    /// Handles heartbeat received from a DataNode.
    async fn send_heart_beat(
        &self,
        request: Request<HeartBeatRequest>,
    ) -> Result<Response<HeartBeatResponse>, Status> {
        let datanode_id_str = request.into_inner().datanode_id;
        println!("Received heartbeat from {:?}", datanode_id_str);

        let datanode_id = Uuid::from_str(&datanode_id_str)
            .map_err(|_| Status::invalid_argument("Invalid DataNode ID format"))?;

        let mut datanodes = self.datanodes.write().unwrap(); // todo: unwrap

        if let Some(datanode) = datanodes.iter_mut().find(|dn| dn.id == datanode_id) {
            if matches!(
                datanode.status,
                DataNodeStatus::UNHEALTHY | DataNodeStatus::DEFAULT
            ) {
                datanode.status = DataNodeStatus::HEALTHY;
                println!("DataNode {:?} status changed to HEALTHY", datanode_id);
            }
        }

        Ok(Response::new(HeartBeatResponse { success: true }))
    }

    /// Handles registration request from a DataNode.
    async fn register_with_namenode(
        &self,
        request: Request<RegistrationRequest>,
    ) -> Result<Response<RegistrationResponse>, Status> {
        println!("Received registration {:?}", request);
        let inner_request = request.into_inner();

        let unwrapped_request_id = Uuid::from_str(&inner_request.datanode_id)
            .map_err(|_| Status::invalid_argument("Invalid DataNode ID format"))?;

        let health_metrics = inner_request
            .health_metrics
            .ok_or_else(|| Status::invalid_argument("Missing health metrics"))?;

        let success = health_metrics.cpu_load < 3.0 && health_metrics.memory_usage < 50.0;
        let response = RegistrationResponse { success };

        if success {
            self.datanodes.write().unwrap().push(DataNode::new(
                unwrapped_request_id,
                DataNodeStatus::DEFAULT,
                BlockReport::new(unwrapped_request_id, Vec::new()),
            ));
        }

        Ok(Response::new(response))
    }

    /// Handles block report request from a DataNode.
    async fn send_block_report(
        &self,
        _request: Request<BlockReportRequest>,
    ) -> Result<Response<BlockReportResponse>, Status> {
        let response = BlockReportResponse { success: true };
        Ok(Response::new(response))
    }
}


#[tonic::async_trait]
impl ClientProtocol for Arc<NameNode> {
    async fn create(&self, request: Request<CreateRequest>) -> Result<Response<CreateResponse>, Status> {
        let inner_request = request.into_inner();
        println!("{:?}", inner_request);
        Ok(Response::new(CreateResponse {
            status: Option::from(proto::Status {
                success: true,
                message: String::from("Created File"),
            }),
        }))
    }
}


