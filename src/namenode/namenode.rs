use std::collections::HashMap;

use std::path::{Component, Path, PathBuf};

use crate::block::{BlockMetadata, BlockStatus, BLOCK_SIZE};
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use tonic::codegen::Body;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::config::namenode_config::NameNodeConfig;
use crate::datanode::block_report::BlockReport;
use crate::error::RSHDFSError;
use crate::namenode::block_map::BlockMap;

use crate::proto::data_node_name_node_service_server::DataNodeNameNodeService;
use crate::proto::rshdfs_name_node_service_server::RshdfsNameNodeService;
use crate::proto::{
    BlockMetadata as ProtoBlockMetadata, ConfirmFileWriteRequest, ConfirmFileWriteResponse,
    CreateRequest, CreateResponse, HeartBeatRequest, HeartBeatResponse, LsRequest, LsResponse,
    ReadRequest, ReadResponse, RegistrationRequest, RegistrationResponse, WriteBlockUpdateRequest,
    WriteBlockUpdateResponse, WriteFileRequest, WriteFileResponse,
};
#[derive(Debug, Clone)]
pub enum INode {
    Directory {
        path: PathBuf,
        children: HashMap<String, INode>,
    },
    File {
        path: PathBuf,
        block_ids: Vec<Uuid>,
    },
}

impl INode {
    pub fn new_directory(path: PathBuf) -> Self {
        INode::Directory {
            path,
            children: HashMap::new(),
        }
    }

    pub fn new_file(path: PathBuf) -> Self {
        INode::File {
            path,
            block_ids: Vec::new(),
        }
    }
}

/// Represents the possible states of a DataNode.
#[derive(Debug, Clone, PartialEq)]
enum DataNodeStatus {
    HEALTHY,
    UNHEALTHY,
    DEFAULT,
}

/// Represents a DataNode in the distributed file system.
#[derive(Debug, Clone)]
pub struct DataNode {
    id: Uuid,
    addr: String,
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
    fn new(id: Uuid, addr: String, status: DataNodeStatus, block_report: BlockReport) -> Self {
        DataNode {
            id,
            addr,
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
    replication_factor: i8,
    datanodes: RwLock<Vec<DataNode>>,
    namespace: Arc<RwLock<INode>>,
    block_map: BlockMap,
}

impl NameNode {
    /// Creates a new instance of NameNode from configuration.
    pub async fn from_config(config: NameNodeConfig) -> Result<Self, RSHDFSError> {
        let id = Uuid::new_v4();
        let root = INode::Directory {
            path: PathBuf::from("/"),
            children: HashMap::new(),
        };

        Ok(NameNode {
            id,
            data_dir: config.data_dir,
            ipc_address: config.ipc_address,
            datanodes: RwLock::new(Vec::new()),
            namespace: Arc::new(RwLock::new(root)),
            block_map: BlockMap::new(),
            replication_factor: config.replication_factor,
        })
    }

    async fn ls_inodes(&self, path: PathBuf) -> Result<Vec<String>, RSHDFSError> {
        let namespace_read_guard = self
            .namespace
            .read()
            .map_err(|_| RSHDFSError::LockError("Failed to acquire read lock".to_string()))?;

        // Start at the root of the namespace
        let mut current_node = &*namespace_read_guard;

        // Traverse the path
        for component in path.iter() {
            let component_str = component.to_str().ok_or_else(|| {
                RSHDFSError::InvalidPathError("Invalid path component".to_string())
            })?;

            current_node = match current_node {
                INode::Directory { children, .. } => {
                    children.get(component_str).ok_or_else(|| {
                        RSHDFSError::FileSystemError(format!(
                            "Directory '{}' not found",
                            component_str
                        ))
                    })?
                }
                _ => {
                    return Err(RSHDFSError::FileSystemError(format!(
                        "'{}' is not a directory",
                        component_str
                    )))
                }
            };
        }

        // List the contents of the directory
        match current_node {
            INode::Directory { children, .. } => Ok(children.keys().cloned().collect()),
            _ => Err(RSHDFSError::FileSystemError(
                "Specified path is not a directory".to_string(),
            )),
        }
    }

    async fn add_block_to_file(
        &self,
        path_buf: PathBuf,
        block_metadata: BlockMetadata,
    ) -> Result<Vec<Uuid>, RSHDFSError> {
        let mut namespace_write_guard = self
            .namespace
            .write()
            .map_err(|_| RSHDFSError::LockError("Failed to acquire write lock".to_string()))?;
        let path = Path::new(path_buf.as_os_str());
        let inode = Self::traverse_to_inode(&mut namespace_write_guard, path)?;

        return match inode {
            INode::Directory { .. } => {
                Err(RSHDFSError::FileSystemError("File not found.".to_string()))
            }
            INode::File {
                ref mut block_ids, ..
            } => {
                // Add the block to the BlockMap and store the block ID in the file inode
                self.block_map.add_block(block_metadata.clone());
                block_ids.push(block_metadata.id);
                Ok(block_ids.clone())
            }
        };
    }

    // Method to get file blocks retrieves BlockMetadata from BlockMap
    async fn get_file_blocks(&self, path: PathBuf) -> Result<Vec<BlockMetadata>, RSHDFSError> {
        let namespace_read_guard = self
            .namespace
            .read()
            .map_err(|_| RSHDFSError::LockError("Failed to acquire read lock".to_string()))?;

        let mut current_node = &*namespace_read_guard;

        for component in path.iter() {
            let component_str = component.to_str().ok_or_else(|| {
                RSHDFSError::InvalidPathError("Invalid path component".to_string())
            })?;

            current_node = match current_node {
                INode::Directory { children, .. } => {
                    children.get(component_str).ok_or_else(|| {
                        RSHDFSError::FileSystemError(format!(
                            "'{}' not found in current directory",
                            component_str
                        ))
                    })?
                }
                _ => {
                    return Err(RSHDFSError::FileSystemError(format!(
                        "'{}' is not a directory",
                        component_str
                    )))
                }
            };
        }

        if let INode::File { block_ids, .. } = current_node {
            // Retrieve actual BlockMetadata from BlockMap
            let mut blocks = vec![];
            for block_id in block_ids {
                let block = self.block_map.get_block(*block_id)?;
                blocks.push(block);
            }
            Ok(blocks)
        } else {
            Err(RSHDFSError::FileSystemError(
                "Path does not point to a file".to_string(),
            ))
        }
    }

    async fn add_inode_to_namespace(&self, path: PathBuf, inode: INode) -> Result<(), RSHDFSError> {
        let mut namespace_guard = self
            .namespace
            .write()
            .map_err(|_| RSHDFSError::LockError("Failed to acquire write lock".to_string()))?;

        if path.components().count() == 0 {
            return Err(RSHDFSError::InvalidPathError("Empty path".to_string()));
        }

        let mut current_node = &mut *namespace_guard;

        // Iterate over the path components, except for the last one (the name of the new inode)
        for component in path.iter().take(path.components().count() - 1) {
            let component_str = component.to_str().ok_or_else(|| {
                RSHDFSError::InvalidPathError("Invalid path component".to_string())
            })?;

            current_node = match current_node {
                INode::Directory { children, .. } => {
                    // Get or create a new directory for the current component
                    children
                        .entry(component_str.to_string())
                        .or_insert_with(|| INode::new_directory(PathBuf::from(component)))
                }
                _ => {
                    return Err(RSHDFSError::FileSystemError(format!(
                        "'{}' is not a directory",
                        component_str
                    )))
                }
            };
        }

        // Extract the final part of the path (the name of the new inode)
        let final_part = path
            .file_name()
            .ok_or_else(|| {
                RSHDFSError::InvalidPathError("Invalid final path component".to_string())
            })?
            .to_str()
            .unwrap();

        match current_node {
            INode::Directory { children, .. } => {
                if children.contains_key(final_part) {
                    Err(RSHDFSError::FileSystemError(
                        "File or directory already exists".to_string(),
                    ))
                } else {
                    // Insert the new inode into the parent directory's children
                    children.insert(final_part.to_string(), inode);
                    Ok(())
                }
            }
            _ => Err(RSHDFSError::FileSystemError(
                "Parent path is not a directory".to_string(),
            )),
        }
    }

    async fn select_datanodes(&self) -> Result<Vec<String>, RSHDFSError> {
        let datanodes_read_guard = self
            .datanodes
            .read()
            .map_err(|_| RSHDFSError::LockError("Failed to acquire read lock".to_string()))?;
        let mut selected_datanodes = vec![];
        for datanode in datanodes_read_guard.iter() {
            if datanode.status == DataNodeStatus::HEALTHY
                && selected_datanodes.len() < self.replication_factor as usize
            {
                selected_datanodes.push(datanode.clone().addr);
            }
            if selected_datanodes.len() == self.replication_factor as usize {
                break;
            }
        }
        if selected_datanodes.len() < self.replication_factor as usize {
            return Err(RSHDFSError::InsufficientDataNodes(
                "Not enough healthy datanodes".to_string(),
            ));
        }
        Ok(selected_datanodes)
    }

    fn traverse_to_inode<'a>(
        current_node: &'a mut INode,
        path: &Path,
    ) -> Result<&'a mut INode, RSHDFSError> {
        let mut node = current_node;

        for component in path.components() {
            match component {
                Component::Normal(name) => {
                    let name_str = name
                        .to_str()
                        .ok_or_else(|| {
                            RSHDFSError::PathError(format!("Invalid path component: {:?}", name))
                        })?
                        .to_string();

                    // Traverse to the next component in the path
                    match node {
                        INode::Directory { children, .. } => {
                            node = children.get_mut(&name_str).ok_or_else(|| {
                                RSHDFSError::PathError(format!("Path not found: {:?}", name_str))
                            })?;
                        }
                        INode::File { .. } => {
                            // If a file is encountered in the middle of the path, return an error
                            return Err(RSHDFSError::PathError(
                                "Encountered file in path to directory".to_string(),
                            ));
                        }
                    }
                }
                _ => return Err(RSHDFSError::PathError("Invalid path".to_string())),
            }
        }
        Ok(node)
    }
}

#[tonic::async_trait]
impl DataNodeNameNodeService for Arc<NameNode> {
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

        let _health_metrics = inner_request
            .health_metrics
            .ok_or_else(|| Status::invalid_argument("Missing health metrics"))?;

        //let success = health_metrics.cpu_load < 3.0 && health_metrics.memory_usage < 50.0;
        let response = RegistrationResponse { success: true };

            self.datanodes.write().unwrap().push(DataNode::new(
                unwrapped_request_id,
                inner_request.hostname_port,
                DataNodeStatus::DEFAULT,
                BlockReport::new(unwrapped_request_id, Vec::new()),
            ));

        Ok(Response::new(response))
    }

    async fn write_block_update(
        &self,
        request: Request<WriteBlockUpdateRequest>,
    ) -> Result<Response<WriteBlockUpdateResponse>, Status> {
        let inner_request = request.into_inner();
        let block_id = Uuid::parse_str(inner_request.block_id.as_str()).unwrap();
        let status = inner_request.status;

        let mut block_in_map = self.block_map.get_block(block_id)?;

        return if status == BlockStatus::Written as i32 {
            block_in_map.status = BlockStatus::Written;
            Ok(Response::new(WriteBlockUpdateResponse { success: true }))
        } else if status == BlockStatus::Waiting as i32 {
            block_in_map.status = BlockStatus::Waiting;
            Ok(Response::new(WriteBlockUpdateResponse { success: false }))
        } else {
            block_in_map.status == BlockStatus::InProgress;
            Ok(Response::new(WriteBlockUpdateResponse { success: false }))
        };
    }
}

#[tonic::async_trait]
impl RshdfsNameNodeService for Arc<NameNode> {
    async fn create(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, Status> {
        let inner_request = request.into_inner();

        let new_file = INode::File {
            path: PathBuf::from(inner_request.path.clone()),
            block_ids: Vec::new(),
        };
        let result = self
            .add_inode_to_namespace(PathBuf::from(inner_request.path), new_file)
            .await;
        match result {
            Ok(_) => Ok(Response::new(CreateResponse { success: true })),
            Err(e) => Err(Status::from(e)),
        }
    }

    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let inner_request = request.into_inner();
        match self
            .get_file_blocks(PathBuf::from(inner_request.path))
            .await
        {
            Ok(blocks) => {
                let mut proto_blocks = vec![];
                blocks.iter().for_each(|block| {
                    proto_blocks.push(ProtoBlockMetadata {
                        block_id: String::from(block.id),
                        seq: block.seq,
                        datanodes: block.clone().datanodes,
                    })
                });
                Ok(Response::new(ReadResponse {
                    file_blocks: proto_blocks,
                }))
            }
            Err(e) => Err(Status::from(e)),
        }
    }

    async fn ls(&self, request: Request<LsRequest>) -> Result<Response<LsResponse>, Status> {
        let inner_request = request.into_inner();
        match self.ls_inodes(PathBuf::from(inner_request.path)).await {
            Ok(result) => Ok(Response::new(LsResponse { inodes: result })),
            Err(e) => Err(Status::from(e)),
        }
    }

    async fn write_file(
        &self,
        request: Request<WriteFileRequest>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        let inner_request = request.into_inner();
        let file_size = inner_request.file_size;
        let num_blocks = (file_size + BLOCK_SIZE as u64 - 1) / BLOCK_SIZE as u64;

        let selected_datanodes = self.select_datanodes().await.map_err(Status::from)?;
        let mut block_ids = Vec::new();
        let mut block_info = Vec::new();

        // Create blocks and add them to BlockMap
        for seq in 0..num_blocks {
            let block_id = Uuid::new_v4();
            block_ids.push(block_id);
            let new_block = BlockMetadata {
                id: block_id,
                seq: seq as i32,
                status: BlockStatus::Waiting,
                datanodes: selected_datanodes.clone(),
                size: BLOCK_SIZE,
            };
            block_info.push(ProtoBlockMetadata {
                block_id: String::from(block_id),
                seq: seq as i32,
                datanodes: selected_datanodes.clone(),
            });
            self.block_map.add_block(new_block);
        }

        // Send block information to client
        Ok(Response::new(WriteFileResponse {
            blocks: block_info, // Send this information to the client
        }))
    }

    async fn confirm_file_write(
        &self,
        request: Request<ConfirmFileWriteRequest>,
    ) -> Result<Response<ConfirmFileWriteResponse>, Status> {
        let mut uuid_vec = vec![];
        let inner_request = request.into_inner().clone();
        inner_request
            .block_ids
            .iter()
            .for_each(|block_id| uuid_vec.push(Uuid::parse_str(&*block_id).unwrap()));
        let new_file = INode::File {
            path: PathBuf::from(inner_request.path.clone()),
            block_ids: uuid_vec,
        };
        match self
            .add_inode_to_namespace(PathBuf::from(inner_request.path.clone()), new_file)
            .await
        {
            Ok(_) => Ok(Response::new(ConfirmFileWriteResponse { success: true })),
            Err(e) => Err(Status::from(e)),
        }
    }
}