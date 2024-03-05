#[cfg(test)]
mod tests {
    use crate::block::{BlockMetadata, BlockStatus};
    use crate::namenode::block_map::BlockMap;
    use crate::namenode::namenode::{DataNodeStatus, INode, NameNode};
    use crate::proto::data_node_name_node_service_client::DataNodeNameNodeServiceClient;
    use crate::proto::data_node_name_node_service_server::DataNodeNameNodeServiceServer;
    use crate::proto::{BlockReportRequest, GetResponse, HeartBeatRequest, NodeHealthMetrics, PutFileResponse};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::fs::File;
    use tokio::sync::{Mutex, RwLock};
    use tokio_test::{assert_err, assert_ok};
    use tonic::transport::Server;
    use uuid::Uuid;
    use crate::config::rshdfs_config::RSHDFSConfig;
    use crate::datanode::datanode::{BlockInfo, DataNode, HealthMetrics};
    use crate::error::RSHDFSError;
    use crate::error::RSHDFSError::PutBlockStreamedError;
    use crate::namenode;
    use crate::proto::rshdfs_data_node_service_server::RshdfsDataNodeServiceServer;
    use crate::rshdfs::client::{Client, RSHDFSClient};

    const DATANODE_UUID: &str = "00000000-1111-2222-3333-444444444444";
    const NAMENODE_UUID: &str = "11111111-1111-2222-3333-444444444444";
    const DATANODE_UUID_INVALID: &str = "00000000-1111-2222-3333-44444444444";

    const DATANODE_HOSTNAME_PORT: &str = "127.0.0.1:50002";
    const DATANODE_DATA_DIR: &str = "src/tests/datanode/hdfs/data";
    const DATANODE_DATA_DIR_INVALID: &str = "src/tests/datanode/data";

    const CLIENT_DATA_DIR: &str = "src/tests/rshdfs/data";
    const LOCALHOST_IPV4: &str = "127.0.0.1";
    const NAMENODE_DATA_DIR: &str = "hdfs/namenode";
    const TEST_BLOCK_ONE_ID: &str = "11111111-1111-1111-1111-111111111111";

    const TEST_BLOCK_TWO_ID: &str = "22222222-2222-2222-2222-222222222222";

    const TEST_BLOCK_ID_GET: &str = "33333333-3333-3333-3333-333333333333";

    async fn start_namenode(namenode: Arc<NameNode>, port: u16) {
        tokio::spawn(async move {
            Server::builder()
                .add_service(DataNodeNameNodeServiceServer::new(namenode.clone()))
                .serve(format!("{}:{}", LOCALHOST_IPV4, port).parse().unwrap())
                .await
        });
    }

    async fn start_datanode(datanode: DataNode, port: u16) {
        tokio::spawn(async move {
        Server::builder()
            .add_service(RshdfsDataNodeServiceServer::new(datanode))
            .serve(format!("{}:{}", LOCALHOST_IPV4, port).parse().unwrap())
            .await
    });
    }

    #[tokio::test]
    async fn send_heartbeat_expects_success() {
        let namenode = Arc::new(NameNode {
            id: Uuid::try_parse(NAMENODE_UUID).unwrap(),
            data_dir: NAMENODE_DATA_DIR.to_string(),
            ipc_address: Default::default(),
            replication_factor: 3,
            datanodes: RwLock::new(Vec::new()),
            namespace: Arc::new(RwLock::new(INode::Directory {
                path: PathBuf::from("/"),
                children: HashMap::new(),
            })),
            block_map: BlockMap {
                blocks: RwLock::new(HashMap::new()),
            },
        });

        let port = 50001u16;
        start_namenode(namenode, port).await;
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let mut client =
            DataNodeNameNodeServiceClient::connect(format!("http://{}:{}", LOCALHOST_IPV4, port))
                .await
                .unwrap();

        let request = HeartBeatRequest {
            datanode_id: DATANODE_UUID.to_string(),
            health_metrics: Some(NodeHealthMetrics {
                cpu_load: 0.0,
                memory_usage: 0,
                disk_space: 0,
            }),
        };

        let result = client.send_heart_beat(request).await;

        assert_ok!(result);
    }

    #[tokio::test]
    async fn send_heartbeat_expects_failure_invalid_uuid() {
        let namenode = Arc::new(NameNode {
            id: Uuid::try_parse(NAMENODE_UUID).unwrap(),
            data_dir: NAMENODE_DATA_DIR.to_string(),
            ipc_address: Default::default(),
            replication_factor: 3,
            datanodes: RwLock::new(Vec::new()),
            namespace: Arc::new(RwLock::new(INode::Directory {
                path: PathBuf::from("/"),
                children: HashMap::new(),
            })),
            block_map: BlockMap {
                blocks: RwLock::new(HashMap::new()),
            },
        });

        let port = 50002u16;
        start_namenode(namenode, port).await;
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let mut client =
            DataNodeNameNodeServiceClient::connect(format!("http://{}:{}", LOCALHOST_IPV4, port))
                .await
                .unwrap();

        let request = HeartBeatRequest {
            datanode_id: DATANODE_UUID_INVALID.to_string(),
            health_metrics: Some(NodeHealthMetrics {
                cpu_load: 0.0,
                memory_usage: 0,
                disk_space: 0,
            }),
        };

        let result = client.send_heart_beat(request).await;
        assert_err!(result);
    }

    #[tokio::test]
    async fn send_block_report_expects_success_with_blocks_to_delete_uuid() {
        let test_block_one_awaiting_deletion: &BlockMetadata = &BlockMetadata {
            id: Uuid::try_parse(TEST_BLOCK_ONE_ID).unwrap(),
            seq: 0,
            status: BlockStatus::AwaitingDeletion,
            size: 1000000,
            datanodes: vec![DATANODE_HOSTNAME_PORT.to_string()],
        };

        let namenode = Arc::new(NameNode {
            id: Uuid::try_parse(NAMENODE_UUID).unwrap(),
            data_dir: NAMENODE_DATA_DIR.to_string(),
            ipc_address: Default::default(),
            replication_factor: 3,
            datanodes: RwLock::new(Vec::new()),
            namespace: Arc::new(RwLock::new(INode::Directory {
                path: PathBuf::from("/"),
                children: HashMap::new(),
            })),
            block_map: BlockMap {
                blocks: RwLock::new(HashMap::new()),
            },
        });

        let mut block_map_guard = namenode.block_map.blocks.write().await;
        block_map_guard.insert(
            Uuid::try_parse(TEST_BLOCK_ONE_ID).unwrap(),
            test_block_one_awaiting_deletion.clone(),
        );
        drop(block_map_guard);

        let port = 50003u16;
        start_namenode(namenode.clone(), port).await;
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let mut client =
            DataNodeNameNodeServiceClient::connect(format!("http://{}:{}", LOCALHOST_IPV4, port))
                .await
                .unwrap();
        let mock_request = BlockReportRequest {
            block_ids: vec![TEST_BLOCK_ONE_ID.to_string()],
        };

        let response = client
            .send_block_report(mock_request)
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.block_ids, vec![TEST_BLOCK_ONE_ID.to_string()])
    }

    #[tokio::test]
    async fn test_put_valid() {
        let datanode_port = 50005u16;
        let namenode_port = 50006u16;


        let namenode = Arc::new(NameNode {
            id: Uuid::try_parse(NAMENODE_UUID).unwrap(),
            data_dir: NAMENODE_DATA_DIR.to_string(),
            ipc_address: Default::default(),
            replication_factor: 3,
            datanodes: RwLock::new(vec![namenode::namenode::DataNode {
                id: Uuid::try_parse(DATANODE_UUID).unwrap(),
                addr: DATANODE_HOSTNAME_PORT.to_string(),
                status: DataNodeStatus::HEALTHY,
            }]),
            namespace: Arc::new(RwLock::new(INode::Directory {
                path: PathBuf::from("/"),
                children: HashMap::new(),
            })),
            block_map: BlockMap {
                blocks: RwLock::new(HashMap::new()),
            },
        });

        start_namenode(namenode.clone(), namenode_port).await;
        tokio::time::sleep(Duration::from_millis(1000)).await;


        let datanode = DataNode {
            id: Uuid::try_parse(DATANODE_UUID).unwrap(),
            data_dir: DATANODE_DATA_DIR.to_string(),
            hostname_port: DATANODE_HOSTNAME_PORT.to_string(),
            datanode_namenode_service_client: Arc::new(Mutex::new(DataNodeNameNodeServiceClient::connect(format!("http://{}:{}", LOCALHOST_IPV4, namenode_port)).await.unwrap())),
            blocks: Arc::new(Default::default()),
            metrics: Arc::new(Mutex::new(HealthMetrics {
                cpu_load: 0.0,
                memory_usage: 0,
                disk_space: 0,
            })),
            heartbeat_interval: Default::default(),
            block_report_interval: Default::default(),
            metrics_interval: Default::default(),
        };

        start_datanode(datanode, datanode_port).await;
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let rshdfs_client = RSHDFSClient::from_config(RSHDFSConfig {
            namenode_address: format!("{}:{}", LOCALHOST_IPV4, namenode_port),
            data_dir: CLIENT_DATA_DIR.to_string(),
        });

        let local_file = File::open("src/tests/put_test.txt").await.unwrap();
        let block = crate::proto::BlockMetadata {
            block_id: TEST_BLOCK_ONE_ID.to_string(),
            seq: 0,
            datanodes: vec![DATANODE_HOSTNAME_PORT.to_string()],
        };
        let put_file_response = PutFileResponse {
            blocks: vec![block],
        };
        let result = rshdfs_client.put(local_file, put_file_response).await;
        assert_eq!(result, Err(RSHDFSError::ConnectionError("unable to connect".to_string())));

        let valid_block = crate::proto::BlockMetadata {
            block_id: TEST_BLOCK_ONE_ID.to_string(),
            seq: 0,
            datanodes: vec![format!("127.0.0.1:{}", datanode_port)]
        };

        let valid_put_file_response = PutFileResponse {
            blocks: vec![valid_block],
        };

        let local_file_valid_test = File::open("src/tests/put_test.txt").await.unwrap();

        let valid_result = rshdfs_client.put(local_file_valid_test, valid_put_file_response).await;
        assert_eq!(valid_result, Ok(vec![TEST_BLOCK_ONE_ID.to_string()]));

        let datanode_file = File::open(format!("{}/{}_{}.dat", DATANODE_DATA_DIR, TEST_BLOCK_ONE_ID, 0)).await;
        assert_ok!(datanode_file);

        tokio::fs::remove_file(format!("{}/{}_{}.dat", DATANODE_DATA_DIR, TEST_BLOCK_ONE_ID, 0)).await.unwrap();
    }

    #[tokio::test]
    async fn test_put_invalid() {
        let datanode_port = 50007u16;
        let namenode_port = 50008u16;


        let namenode = Arc::new(NameNode {
            id: Uuid::try_parse(NAMENODE_UUID).unwrap(),
            data_dir: NAMENODE_DATA_DIR.to_string(),
            ipc_address: Default::default(),
            replication_factor: 3,
            datanodes: RwLock::new(vec![namenode::namenode::DataNode {
                id: Uuid::try_parse(DATANODE_UUID).unwrap(),
                addr: DATANODE_HOSTNAME_PORT.to_string(),
                status: DataNodeStatus::HEALTHY,
            }]),
            namespace: Arc::new(RwLock::new(INode::Directory {
                path: PathBuf::from("/"),
                children: HashMap::new(),
            })),
            block_map: BlockMap {
                blocks: RwLock::new(HashMap::new()),
            },
        });

        start_namenode(namenode.clone(), namenode_port).await;
        tokio::time::sleep(Duration::from_millis(1000)).await;


        let datanode_invalid_data_dir = DataNode {
            id: Uuid::try_parse(DATANODE_UUID).unwrap(),
            data_dir: DATANODE_DATA_DIR_INVALID.to_string(),
            hostname_port: DATANODE_HOSTNAME_PORT.to_string(),
            datanode_namenode_service_client: Arc::new(Mutex::new(DataNodeNameNodeServiceClient::connect(format!("http://{}:{}", LOCALHOST_IPV4, namenode_port)).await.unwrap())),
            blocks: Arc::new(Default::default()),
            metrics: Arc::new(Mutex::new(HealthMetrics {
                cpu_load: 0.0,
                memory_usage: 0,
                disk_space: 0,
            })),
            heartbeat_interval: Default::default(),
            block_report_interval: Default::default(),
            metrics_interval: Default::default(),
        };

        start_datanode(datanode_invalid_data_dir, datanode_port).await;
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let rshdfs_client = RSHDFSClient::from_config(RSHDFSConfig {
            namenode_address: format!("{}:{}", LOCALHOST_IPV4, namenode_port),
            data_dir: CLIENT_DATA_DIR.to_string(),
        });

        let local_file = File::open("src/tests/put_test.txt").await.unwrap();
        let block = crate::proto::BlockMetadata {
            block_id: TEST_BLOCK_ONE_ID.to_string(),
            seq: 0,
            datanodes: vec![DATANODE_HOSTNAME_PORT.to_string()],
        };
        let put_file_response = PutFileResponse {
            blocks: vec![block],
        };
        let result = rshdfs_client.put(local_file, put_file_response).await;
        assert_eq!(result, Err(RSHDFSError::ConnectionError("unable to connect".to_string())));

        let valid_block = crate::proto::BlockMetadata {
            block_id: TEST_BLOCK_TWO_ID.to_string(),
            seq: 0,
            datanodes: vec![format!("127.0.0.1:{}", datanode_port)]
        };

        let valid_put_file_response = PutFileResponse {
            blocks: vec![valid_block],
        };

        let local_file_valid_test = File::open("src/tests/put_test.txt").await.unwrap();

        let directory_does_not_exist_result = rshdfs_client.put(local_file_valid_test, valid_put_file_response).await;
        assert_eq!(directory_does_not_exist_result, Err(PutBlockStreamedError("Failed to create file: No such file or directory (os error 2)".to_string())));

        let non_existent_datanode_file = File::open(format!("{}/{}_{}.dat", DATANODE_DATA_DIR, TEST_BLOCK_TWO_ID, 0)).await;
        assert_err!(non_existent_datanode_file);
    }

    #[tokio::test]
    async fn test_get_valid() {
        let datanode_port = 50009u16;
        let namenode_port = 50010u16;

        let namenode = Arc::new(NameNode {
            id: Uuid::try_parse(NAMENODE_UUID).unwrap(),
            data_dir: NAMENODE_DATA_DIR.to_string(),
            ipc_address: Default::default(),
            replication_factor: 1, // setting replication factor to 1 for this test
            datanodes: RwLock::new(vec![namenode::namenode::DataNode {
                id: Uuid::try_parse(DATANODE_UUID).unwrap(),
                addr: DATANODE_HOSTNAME_PORT.to_string(),
                status: DataNodeStatus::HEALTHY,
            }]),
            namespace: Arc::new(RwLock::new(INode::Directory {
                path: PathBuf::from("/"),
                children: HashMap::new(),
            })),
            block_map: BlockMap {
                blocks: RwLock::new(HashMap::new()),
            },
        });

        start_namenode(namenode.clone(), namenode_port).await;
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let datanode = DataNode {
            id: Uuid::try_parse(DATANODE_UUID).unwrap(),
            data_dir: DATANODE_DATA_DIR.to_string(),
            hostname_port: DATANODE_HOSTNAME_PORT.to_string(),
            datanode_namenode_service_client: Arc::new(Mutex::new(DataNodeNameNodeServiceClient::connect(format!("http://{}:{}", LOCALHOST_IPV4, namenode_port)).await.unwrap())),
            blocks: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(Mutex::new(Default::default())),
            heartbeat_interval: Default::default(),
            block_report_interval: Default::default(),
            metrics_interval: Default::default(),
        };

        datanode.blocks.write().await.insert(Uuid::try_parse(TEST_BLOCK_ID_GET).unwrap(), BlockInfo {
            id: TEST_BLOCK_ID_GET.to_string(),
            seq: 0,
        });


        start_datanode(datanode, datanode_port).await;
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let final_path = PathBuf::from(format!("{}/get_test.txt", CLIENT_DATA_DIR.to_string()));

        let client = RSHDFSClient::from_config(RSHDFSConfig {
            namenode_address: format!("{}:{}",LOCALHOST_IPV4, namenode_port),
            data_dir: CLIENT_DATA_DIR.to_string(),
        });
        let test_block = crate::proto::BlockMetadata {
            block_id: TEST_BLOCK_ID_GET.to_string(),
            seq: 0,
            datanodes: vec![format!("{}:{}", LOCALHOST_IPV4, datanode_port)],
        };

        let get_response = GetResponse {
            file_blocks: vec![test_block],
        };
        let result = client.get(final_path, get_response).await;

        assert_ok!(result);

        let remove_result = tokio::fs::remove_file(format!("{}/get_test.txt", CLIENT_DATA_DIR)).await;
        assert_ok!(remove_result);

    }
}
