use std::collections::HashMap;

use std::path::{Component, Path, PathBuf};

use crate::block::{BlockMetadata, BlockStatus, BLOCK_SIZE};
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use tonic::{async_trait, Request, Response, Status};
use uuid::Uuid;

use crate::config::namenode_config::NameNodeConfig;
use crate::error::RSHDFSError;
use crate::namenode::block_map::BlockMap;

use crate::proto::data_node_name_node_service_server::DataNodeNameNodeService;
use crate::proto::rshdfs_name_node_service_server::RshdfsNameNodeService;
use crate::proto::{
    BlockMetadata as ProtoBlockMetadata, BlockReportRequest, BlockReportResponse,
    ConfirmFilePutRequest, ConfirmFilePutResponse, DeleteFileRequest, DeleteFileResponse,
    GetRequest, GetResponse, HeartBeatRequest, HeartBeatResponse, LsRequest, LsResponse,
    PutFileRequest, PutFileResponse, RegistrationRequest, RegistrationResponse,
    WriteBlockUpdateRequest, WriteBlockUpdateResponse,
};

#[cfg(test)]
use mockall::automock;
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
pub enum DataNodeStatus {
    HEALTHY,
    DEFAULT,
}

/// Represents a DataNode in the distributed file system.
#[derive(Debug, Clone)]
pub struct DataNode {
    pub id: Uuid,
    pub addr: String,
    pub status: DataNodeStatus,
}

impl PartialEq for DataNode {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl DataNode {
    /// Creates a new instance of DataNode.
    fn new(id: Uuid, addr: String, status: DataNodeStatus) -> Self {
        DataNode { id, addr, status }
    }
}

/// Represents the NameNode in the distributed file system.

#[cfg_attr(test, automock)]
#[async_trait]
trait NamespaceManager {
    async fn add_inode_to_namespace(&self, path: PathBuf, inode: INode) -> Result<(), RSHDFSError>;
    async fn remove_inode_from_namespace(&self, path: PathBuf) -> Result<(), RSHDFSError>;
    async fn ls_inodes(&self, path: PathBuf) -> Result<Vec<String>, RSHDFSError>;
    async fn add_block_to_file(
        &self,
        path_buf: PathBuf,
        block_metadata: BlockMetadata,
    ) -> Result<Vec<Uuid>, RSHDFSError>;

    async fn get_file_blocks(&self, path: PathBuf) -> Result<Vec<BlockMetadata>, RSHDFSError>;
}

#[cfg_attr(test, automock)]
#[async_trait]
trait DatanodeManager {
    async fn select_datanodes(&self) -> Result<Vec<String>, RSHDFSError>;
}

pub struct NameNode {
    pub id: Uuid,
    pub data_dir: String,
    pub ipc_address: String,
    pub replication_factor: i8,
    pub datanodes: RwLock<Vec<DataNode>>,
    pub namespace: Arc<RwLock<INode>>,
    pub block_map: BlockMap,
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
                            RSHDFSError::InvalidPathError(format!(
                                "Invalid path component: {:?}",
                                name
                            ))
                        })?
                        .to_string();

                    // Traverse to the next component in the path
                    match node {
                        INode::Directory { children, .. } => {
                            node = children.get_mut(&name_str).ok_or_else(|| {
                                RSHDFSError::InvalidPathError(format!(
                                    "Path not found: {:?}",
                                    name_str
                                ))
                            })?;
                        }
                        INode::File { .. } => {
                            // If a file is encountered in the middle of the path, return an error
                            return Err(RSHDFSError::InvalidPathError(
                                "Encountered file in path to directory".to_string(),
                            ));
                        }
                    }
                }
                _ => return Err(RSHDFSError::InvalidPathError("Invalid path".to_string())),
            }
        }
        Ok(node)
    }
}

#[async_trait]
impl NamespaceManager for NameNode {
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

    async fn remove_inode_from_namespace(&self, path: PathBuf) -> Result<(), RSHDFSError> {
        let mut namespace_guard = self
            .namespace
            .write()
            .map_err(|_| RSHDFSError::LockError("Failed to acquire write lock".to_string()))?;

        if path.components().count() == 0 {
            return Err(RSHDFSError::InvalidPathError("Empty path".to_string()));
        }

        // Collect path components to a Vec to allow indexing
        let components: Vec<_> = path.iter().collect();
        if components.is_empty() {
            return Err(RSHDFSError::InvalidPathError("Empty path".to_string()));
        }

        let mut current_node = &mut *namespace_guard;

        // Iterate over the path components to the parent of the node to remove
        for component in components.iter().take(components.len() - 1) {
            let component_str = component.to_str().ok_or_else(|| {
                RSHDFSError::InvalidPathError("Invalid path component".to_string())
            })?;

            match current_node {
                INode::Directory { children, .. } => {
                    if let Some(node) = children.get_mut(component_str) {
                        current_node = node;
                    } else {
                        return Err(RSHDFSError::InvalidPathError(format!(
                            "Path component '{}' not found",
                            component_str
                        )));
                    }
                }
                _ => {
                    return Err(RSHDFSError::InvalidPathError(format!(
                        "'{}' is not a directory",
                        component_str
                    )))
                }
            }
        }

        // Remove the target inode
        if let Some(component) = components.last().and_then(|c| c.to_str()) {
            if let INode::Directory { children, .. } = current_node {
                if children.remove(component).is_some() {
                    Ok(())
                } else {
                    Err(RSHDFSError::FileSystemError(
                        "Node to remove not found".to_string(),
                    ))
                }
            } else {
                Err(RSHDFSError::FileSystemError(
                    "Parent is not a directory".to_string(),
                ))
            }
        } else {
            Err(RSHDFSError::InvalidPathError(
                "Invalid target component".to_string(),
            ))
        }
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
}

#[async_trait]
impl DatanodeManager for NameNode {
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
}

#[tonic::async_trait]
impl DataNodeNameNodeService for Arc<NameNode> {
    /// Handles heartbeat received from a DataNode.
    async fn send_heart_beat(
        &self,
        request: Request<HeartBeatRequest>,
    ) -> Result<Response<HeartBeatResponse>, Status> {
        let inner_request = request.into_inner();
        eprintln!(" Received Heartbeat: {:?}",inner_request.clone());
        let datanode_id_str = inner_request.datanode_id;
        let datanode_id = Uuid::from_str(&datanode_id_str)
            .map_err(|_| Status::invalid_argument("Invalid DataNode ID format"))?;

        let mut datanodes = self.datanodes.write().unwrap(); // todo: unwrap

        if let Some(datanode) = datanodes.iter_mut().find(|dn| dn.id == datanode_id) {
            if matches!(datanode.status, DataNodeStatus::DEFAULT) {
                datanode.status = DataNodeStatus::HEALTHY;
            }
        }

        Ok(Response::new(HeartBeatResponse { success: true }))
    }
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
        ));

        Ok(Response::new(response))
    }

    async fn send_block_report(
        &self,
        request: Request<BlockReportRequest>,
    ) -> Result<Response<BlockReportResponse>, Status> {
        let inner_request = request.into_inner();
        let block_ids = inner_request.block_ids;
        let mut uuid_vec = vec![];
        let mut ready_to_delete = vec![];

        for block_id in block_ids.iter() {
            let block_uuid = Uuid::parse_str(&block_id)
                .map_err(|_| RSHDFSError::UUIDError(String::from("Invalid UUID String.")))?;
            uuid_vec.push(block_uuid);
        }

        for block_uuid in uuid_vec {
            match self.block_map.get_block(block_uuid) {
                Ok(block) => {
                    if block.status == BlockStatus::AwaitingDeletion {
                        ready_to_delete.push(block.id.to_string());
                    }
                }
                Err(e) => {
                    return Err(Status::from(e));
                }
            }
        }

        let response = BlockReportResponse {
            block_ids: ready_to_delete,
        };
        Ok(Response::new(response))
    }

    async fn write_block_update(
        &self,
        _request: Request<WriteBlockUpdateRequest>,
    ) -> Result<Response<WriteBlockUpdateResponse>, Status> {
        todo!()
    }
}

#[tonic::async_trait]
impl RshdfsNameNodeService for Arc<NameNode> {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
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
                Ok(Response::new(GetResponse {
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

    async fn put_file(
        &self,
        request: Request<PutFileRequest>,
    ) -> Result<Response<PutFileResponse>, Status> {
        let inner_request = request.into_inner();
        let file_size = inner_request.file_size;

        if file_size == 0u64 {
            return Err(Status::from(RSHDFSError::FileSystemError(String::from(
                "Invalid file size.",
            ))));
        }

        let num_blocks = (file_size + BLOCK_SIZE as u64 - 1) / BLOCK_SIZE as u64;

        let selected_datanodes = self.select_datanodes().await.map_err(|_| {
            RSHDFSError::InsufficientDataNodes(String::from(
                "Not enough datanodes to handle replication.",
            ))
        })?;
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
        Ok(Response::new(PutFileResponse { blocks: block_info }))
    }

    async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> Result<Response<DeleteFileResponse>, Status> {
        let mut proto_blocks = vec![];
        // set the status of the blocks to await deletion
        let inner_request = request.into_inner();
        let path = inner_request.path;
        match self.get_file_blocks(PathBuf::from(path.clone())).await {
            Ok(file_blocks) => {
                self.remove_inode_from_namespace(PathBuf::from(path.clone()))
                    .await;
                for block in file_blocks {
                    self.block_map.modify_block_metadata(block.id, |block| {
                        block.status = BlockStatus::AwaitingDeletion
                    })?;
                    proto_blocks.push(ProtoBlockMetadata {
                        block_id: block.id.to_string(),
                        seq: block.seq,
                        datanodes: block.datanodes.clone(),
                    })
                }
                println!("{:?}", proto_blocks);

                Ok(Response::new(DeleteFileResponse {
                    file_blocks: proto_blocks,
                }))
            }
            Err(e) => Err(Status::from(e)),
        }
    }

    async fn confirm_file_put(
        &self,
        request: Request<ConfirmFilePutRequest>,
    ) -> Result<Response<ConfirmFilePutResponse>, Status> {
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
            Ok(_) => Ok(Response::new(ConfirmFilePutResponse { success: true })),
            Err(e) => Err(Status::from(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::RSHDFSError;
    use crate::namenode::namenode::{INode, MockNamespaceManager, NamespaceManager};
    use std::collections::HashMap;
    use std::path::PathBuf;

    #[tokio::test]
    async fn add_inode_to_namespace_expects_invalid_path_empty() {
        let test_path = PathBuf::from("");
        let test_inode = INode::File {
            path: test_path.clone(),
            block_ids: vec![],
        };
        let mut mock_namespace_manager = MockNamespaceManager::new();
        mock_namespace_manager
            .expect_add_inode_to_namespace()
            .times(1)
            .returning(|_path, _inode| {
                Err(RSHDFSError::InvalidPathError("Empty Path".to_string()))
            });
        let result = mock_namespace_manager
            .add_inode_to_namespace(test_path, test_inode)
            .await;

        assert_eq!(
            result,
            Err(RSHDFSError::InvalidPathError("Empty Path".to_string()))
        );
    }

    #[tokio::test]
    async fn add_inode_to_namespace_expects_invalid_path_component() {
        let test_path = PathBuf::from("/valid/path/to/\u{FFFF}/inode");
        let test_inode = INode::File {
            path: test_path.clone(),
            block_ids: vec![],
        };
        let mut mock_namespace_manager = MockNamespaceManager::new();
        mock_namespace_manager
            .expect_add_inode_to_namespace()
            .times(1)
            .returning(|_path, _inode| {
                Err(RSHDFSError::InvalidPathError(
                    "Invalid path component".to_string(),
                ))
            });
        let result = mock_namespace_manager
            .add_inode_to_namespace(test_path, test_inode)
            .await;

        assert_eq!(
            result,
            Err(RSHDFSError::InvalidPathError(
                "Invalid path component".to_string()
            ))
        );
    }

    #[tokio::test]
    async fn add_inode_to_namespace_expects_is_not_a_directory() {
        let test_path = PathBuf::from("/a");
        let test_path_clone = test_path.clone();
        let _test_path_string = String::from("/a");

        let mut children = HashMap::new();
        children.insert(
            "some".to_string(),
            INode::File {
                path: test_path,
                block_ids: vec![],
            },
        );

        let test_inode = INode::Directory {
            path: PathBuf::from("/"),
            children,
        };
        let mut mock_namespace_manager = MockNamespaceManager::new();
        mock_namespace_manager
            .expect_add_inode_to_namespace()
            .times(1)
            .returning(|path, _inode| {
                Err(RSHDFSError::FileSystemError(format!(
                    "{} is not a directory",
                    path.to_str().unwrap()
                )))
            });
        let result = mock_namespace_manager
            .add_inode_to_namespace(test_path_clone, test_inode)
            .await;

        assert_eq!(
            result,
            Err(RSHDFSError::FileSystemError(String::from(
                "/a is not a directory"
            )))
        );
    }

    #[tokio::test]
    async fn remove_inode_from_namespace_expects_invalid_path_error_empty() {
        let test_path = PathBuf::from("");

        let mut mock_namespace_manager = MockNamespaceManager::new();
        mock_namespace_manager
            .expect_remove_inode_from_namespace()
            .times(1)
            .returning(|_path| Err(RSHDFSError::InvalidPathError(String::from("Empty path"))));

        let result = mock_namespace_manager
            .remove_inode_from_namespace(test_path)
            .await;

        assert_eq!(
            result,
            Err(RSHDFSError::InvalidPathError(String::from("Empty path")))
        );
    }
}
