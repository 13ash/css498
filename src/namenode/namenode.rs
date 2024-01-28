use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use tonic::{ Request, Response, Status};
use uuid::Uuid;
use crate::block::Block;

use crate::config::namenode_config::NameNodeConfig;
use crate::datanode::block_report::BlockReport;
use crate::error::RSHDFSError;
use crate::proto;
use crate::proto::data_node_service_server::DataNodeService;
use crate::proto::{
    BlockReportRequest, BlockReportResponse, HeartBeatRequest, HeartBeatResponse,
    RegistrationRequest, RegistrationResponse, CreateRequest, CreateResponse,
    DeleteRequest, DeleteResponse
};
use crate::proto::client_protocol_server::ClientProtocol;


pub enum INode {
    Directory { name: String, children: HashMap<String, INode> },
    File { name: String, blocks: Vec<Block> },
}

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
    namespace: Arc<RwLock<INode>>,
}

impl NameNode {
    /// Creates a new instance of NameNode from configuration.
    pub async fn from_config(config: NameNodeConfig) -> Result<Self, RSHDFSError> {
        let id = Uuid::new_v4();
        let root = INode::Directory {
            name: String::from("/"),
            children: HashMap::new(),
        };

        Ok(NameNode {
            id,
            data_dir: config.data_dir,
            ipc_address: config.ipc_address,
            checkpoint_interval: config.checkpoint_period,
            handler_count: config.handler_count,
            datanodes: RwLock::new(Vec::new()),
            namespace: Arc::new(RwLock::new(root))
        })
    }

    async fn create_file(&self, path: &str) -> Result<(), RSHDFSError> {
        let mut namespace_guard = self.namespace.write()
            .map_err(|_| RSHDFSError::LockError("Failed to acquire write lock".to_string()))?;

        let parts: Vec<&str> = path.split('/').collect();
        if parts.is_empty() {
            return Err(RSHDFSError::InvalidPathError("Invalid path".to_string()));
        }
        let file_name = parts.last().unwrap().to_string();
        let file_name_clone = file_name.clone();

        let parent_path = parts[..parts.len() - 1].join("/");
        let parent_dir = Self::find_directory(&mut *namespace_guard, &parent_path)
            .map_err(|e| RSHDFSError::FileSystemError(format!("Directory error: {:?}", e)))?;

        match parent_dir {
            INode::Directory { children, .. } => {
                if children.contains_key(&file_name) {
                    Err(RSHDFSError::FileSystemError("File already exists".to_string()))
                } else {
                    children.insert(file_name, INode::File { name: file_name_clone, blocks: Vec::new() });
                    Ok(())
                }
            },
            _ => Err(RSHDFSError::FileSystemError("Parent path is not a directory".to_string())),
        }
    }



    fn find_directory<'a>(node: &'a mut INode, path: &str) -> Result<&'a mut INode, String> {
        let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
        let mut current_node = node;

        for part in parts.iter() {
            match current_node {
                INode::Directory { name: _, children } => {
                    current_node = children.get_mut(*part).ok_or("Directory not found")?;
                },
                _ => return Err("Path is not a directory".to_string()),
            }
        }

        Ok(current_node)
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
        let path = inner_request.path.as_str();

        match self.create_file(path).await {
            Ok(_) => {
                // File successfully created
                Ok(Response::new(CreateResponse {
                    status: Some(proto::Status {
                        success: true,
                        message: String::from("Created File"),
                    }),
                }))
            },
            Err(err) => {
                let status = match err {
                    RSHDFSError::InvalidPathError(_) => Status::invalid_argument("Invalid Path"),
                    RSHDFSError::FileSystemError(msg) => Status::internal(msg),
                    _ => Status::unknown("Unknown error"),
                };
                Err(status)
            }
        }
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let inner_request = request.into_inner();
        println!("{:?}", inner_request);
        Ok(Response::new(DeleteResponse {
            status: Option::from(proto::Status {
            success: true,
            message: String::from("Deleted File"),
        }),
        }))
    }
}


