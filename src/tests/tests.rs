#[cfg(test)]
mod tests {
    use crate::block::{BlockMetadata, BlockStatus, BLOCK_SIZE};
    use crate::config::rshdfs_config::RSHDFSConfig;
    use crate::datanode::datanode::{BlockInfo, DataNode, HealthMetrics};
    use crate::error::RSHDFSError::PutError;
    use crate::namenode;
    use crate::namenode::namenode::{DataNodeStatus, INode, NameNode};
    use crate::proto::data_node_name_node_service_client::DataNodeNameNodeServiceClient;
    use crate::proto::data_node_name_node_service_server::DataNodeNameNodeServiceServer;
    use crate::proto::rshdfs_data_node_service_server::RshdfsDataNodeServiceServer;
    use crate::proto::{
        BlockReportRequest, GetResponse, HeartBeatRequest, NodeHealthMetrics, PutFileResponse,
    };
    use crate::rshdfs::client::{Client, RSHDFSClient};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::{Mutex, RwLock};
    use tokio_test::{assert_err, assert_ok};
    use tonic::transport::Server;
    use uuid::Uuid;

    const DATANODE_UUID: &str = "00000000-1111-2222-3333-444444444444";
    const NAMENODE_UUID: &str = "11111111-1111-2222-3333-444444444444";
    const DATANODE_UUID_INVALID: &str = "00000000-1111-2222-3333-44444444444";

    const DATANODE_HOSTNAME_PORT: &str = "127.0.0.1:50002";
    const DATANODE_DATA_DIR: &str = "src/tests/datanode/hdfs/data";
    const DATANODE_DATA_DIR_INVALID: &str = "src/tests/datanode/data";
    const CLIENT_DATA_DIR: &str = "src/tests/rshdfs/data";
    const LOCALHOST_IPV4: &str = "127.0.0.1";
    const NAMENODE_DATA_DIR: &str = "hdfs/namenode";
    const TEST_BLOCK_GET_ID_ONE: &str = "00000000-0000-0000-0000-000000000000";
    const TEST_BLOCK_GET_ID_TWO: &str = "00000000-0000-0000-0000-000000000001";
    const TEST_BLOCK_PUT_ID_ONE: &str = "11111111-1111-1111-1111-111111111111";
    const TEST_BLOCK_PUT_ID_TWO: &str = "22222222-2222-2222-2222-222222222222";
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
            namespace: RwLock::new(INode::Directory {
                path: PathBuf::from("/"),
                children: HashMap::new(),
            }),
            edit_log: RwLock::new(Default::default()),
            block_map: RwLock::new(Default::default()),
            flush_interval: 30000,
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
            namespace: RwLock::new(INode::Directory {
                path: PathBuf::from("/"),
                children: HashMap::new(),
            }),
            edit_log: Default::default(),
            block_map: RwLock::new(Default::default()),
            flush_interval: 30000,
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
            id: Uuid::try_parse(TEST_BLOCK_PUT_ID_ONE).unwrap(),
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
            namespace: RwLock::new(INode::Directory {
                path: PathBuf::from("/"),
                children: HashMap::new(),
            }),
            edit_log: Default::default(),
            block_map: RwLock::new(Default::default()),
            flush_interval: 30000,
        });

        let mut block_map_guard = namenode.block_map.write().await;
        block_map_guard.blocks.insert(
            Uuid::try_parse(TEST_BLOCK_PUT_ID_ONE).unwrap(),
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
            block_ids: vec![TEST_BLOCK_PUT_ID_ONE.to_string()],
        };

        let response = client
            .send_block_report(mock_request)
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.block_ids, vec![TEST_BLOCK_PUT_ID_ONE.to_string()])
    }

    #[tokio::test]
    async fn test_put_expects_success() {
        let datanode_port = 50005u16;
        let namenode_port = 50006u16;

        let namenode = Arc::new(NameNode {
            id: Uuid::try_parse(NAMENODE_UUID).unwrap(),
            data_dir: NAMENODE_DATA_DIR.to_string(),
            ipc_address: Default::default(),
            replication_factor: 1,
            datanodes: RwLock::new(vec![namenode::namenode::DataNode {
                id: Uuid::try_parse(DATANODE_UUID).unwrap(),
                addr: DATANODE_HOSTNAME_PORT.to_string(),
                status: DataNodeStatus::HEALTHY,
            }]),
            namespace: RwLock::new(INode::Directory {
                path: PathBuf::from("/"),
                children: HashMap::new(),
            }),
            edit_log: Default::default(),
            block_map: RwLock::new(Default::default()),
            flush_interval: 30000,
        });

        start_namenode(namenode.clone(), namenode_port).await;
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let datanode = DataNode {
            id: Uuid::try_parse(DATANODE_UUID).unwrap(),
            data_dir: DATANODE_DATA_DIR.to_string(),
            hostname_port: DATANODE_HOSTNAME_PORT.to_string(),
            datanode_namenode_service_client: Arc::new(Mutex::new(
                DataNodeNameNodeServiceClient::connect(format!(
                    "http://{}:{}",
                    LOCALHOST_IPV4, namenode_port
                ))
                .await
                .unwrap(),
            )),
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
            block_id: TEST_BLOCK_PUT_ID_ONE.to_string(),
            seq: 0,
            datanodes: vec![DATANODE_HOSTNAME_PORT.to_string()],
        };
        let put_file_response = PutFileResponse {
            blocks: vec![block],
        };
        let result = rshdfs_client.put(local_file, put_file_response).await;
        assert_eq!(result, Err(PutError("unable to connect".to_string())));

        let valid_block = crate::proto::BlockMetadata {
            block_id: TEST_BLOCK_PUT_ID_ONE.to_string(),
            seq: 0,
            datanodes: vec![format!("127.0.0.1:{}", datanode_port)],
        };

        let valid_put_file_response = PutFileResponse {
            blocks: vec![valid_block],
        };

        let local_file_valid_test = File::open("src/tests/put_test.txt").await.unwrap();

        let valid_result = rshdfs_client
            .put(local_file_valid_test, valid_put_file_response)
            .await;
        assert_eq!(valid_result, Ok(vec![TEST_BLOCK_PUT_ID_ONE.to_string()]));

        let datanode_file = File::open(format!(
            "{}/{}_{}.dat",
            DATANODE_DATA_DIR, TEST_BLOCK_PUT_ID_ONE, 0
        ))
        .await;
        assert_ok!(datanode_file);

        tokio::fs::remove_file(format!(
            "{}/{}_{}.dat",
            DATANODE_DATA_DIR, TEST_BLOCK_PUT_ID_ONE, 0
        ))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_put_expects_failure() {
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
            namespace: RwLock::new(INode::Directory {
                path: PathBuf::from("/"),
                children: HashMap::new(),
            }),
            edit_log: Default::default(),
            block_map: RwLock::new(Default::default()),
            flush_interval: 30000,
        });

        start_namenode(namenode.clone(), namenode_port).await;
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let datanode_invalid_data_dir = DataNode {
            id: Uuid::try_parse(DATANODE_UUID).unwrap(),
            data_dir: DATANODE_DATA_DIR_INVALID.to_string(),
            hostname_port: DATANODE_HOSTNAME_PORT.to_string(),
            datanode_namenode_service_client: Arc::new(Mutex::new(
                DataNodeNameNodeServiceClient::connect(format!(
                    "http://{}:{}",
                    LOCALHOST_IPV4, namenode_port
                ))
                .await
                .unwrap(),
            )),
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
            block_id: TEST_BLOCK_PUT_ID_ONE.to_string(),
            seq: 0,
            datanodes: vec![DATANODE_HOSTNAME_PORT.to_string()],
        };
        let put_file_response = PutFileResponse {
            blocks: vec![block],
        };
        let result = rshdfs_client.put(local_file, put_file_response).await;
        assert_eq!(result, Err(PutError("unable to connect".to_string())));

        let valid_block = crate::proto::BlockMetadata {
            block_id: TEST_BLOCK_PUT_ID_TWO.to_string(),
            seq: 0,
            datanodes: vec![format!("127.0.0.1:{}", datanode_port)],
        };

        let valid_put_file_response = PutFileResponse {
            blocks: vec![valid_block],
        };

        let local_file_valid_test = File::open("src/tests/put_test.txt").await.unwrap();

        let directory_does_not_exist_result = rshdfs_client
            .put(local_file_valid_test, valid_put_file_response)
            .await;
        assert_eq!(
            directory_does_not_exist_result,
            Err(PutError(
                "Failed to create file: No such file or directory (os error 2)".to_string()
            ))
        );

        let non_existent_datanode_file = File::open(format!(
            "{}/{}_{}.dat",
            DATANODE_DATA_DIR, TEST_BLOCK_PUT_ID_TWO, 0
        ))
        .await;
        assert_err!(non_existent_datanode_file);
    }

    #[tokio::test]
    async fn test_get_expects_success() {
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
            namespace: RwLock::new(INode::Directory {
                path: PathBuf::from("/"),
                children: HashMap::new(),
            }),
            edit_log: Default::default(),
            block_map: RwLock::new(Default::default()),
            flush_interval: 30000,
        });

        start_namenode(namenode.clone(), namenode_port).await;
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let datanode = DataNode {
            id: Uuid::try_parse(DATANODE_UUID).unwrap(),
            data_dir: DATANODE_DATA_DIR.to_string(),
            hostname_port: DATANODE_HOSTNAME_PORT.to_string(),
            datanode_namenode_service_client: Arc::new(Mutex::new(
                DataNodeNameNodeServiceClient::connect(format!(
                    "http://{}:{}",
                    LOCALHOST_IPV4, namenode_port
                ))
                .await
                .unwrap(),
            )),
            blocks: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(Mutex::new(Default::default())),
            heartbeat_interval: Default::default(),
            block_report_interval: Default::default(),
            metrics_interval: Default::default(),
        };

        datanode.blocks.write().await.insert(
            Uuid::try_parse(TEST_BLOCK_GET_ID_ONE).unwrap(),
            BlockInfo {
                id: TEST_BLOCK_GET_ID_ONE.to_string(),
                seq: 0,
            },
        );
        datanode.blocks.write().await.insert(
            Uuid::try_parse(TEST_BLOCK_GET_ID_TWO).unwrap(),
            BlockInfo {
                id: TEST_BLOCK_GET_ID_TWO.to_string(),
                seq: 1,
            },
        );

        let block_one_data = vec!['r' as u8; BLOCK_SIZE];
        let block_two_data = vec!['s' as u8; BLOCK_SIZE - 100 * 1024 * 1024]; // Smaller by 100 MB

        let mut pre_get_data_buffer = vec![0u8; BLOCK_SIZE + BLOCK_SIZE - 24 * 1024 * 1024];

        pre_get_data_buffer[0..BLOCK_SIZE].copy_from_slice(&block_one_data);

        pre_get_data_buffer[BLOCK_SIZE..BLOCK_SIZE + BLOCK_SIZE - 100 * 1024 * 1024]
            .copy_from_slice(&block_two_data);

        let mut file_one = File::create(format!(
            "{}/{}_{}.dat",
            DATANODE_DATA_DIR, TEST_BLOCK_GET_ID_ONE, 0
        ))
        .await
        .unwrap();
        let mut file_two = File::create(format!(
            "{}/{}_{}.dat",
            DATANODE_DATA_DIR, TEST_BLOCK_GET_ID_TWO, 1
        ))
        .await
        .unwrap();

        file_one.write_all(&block_one_data).await.unwrap();
        file_two.write_all(&block_two_data).await.unwrap();

        start_datanode(datanode, datanode_port).await;
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let final_path = PathBuf::from(format!("{}/get_test.txt", CLIENT_DATA_DIR.to_string()));

        let client = RSHDFSClient::from_config(RSHDFSConfig {
            namenode_address: format!("{}:{}", LOCALHOST_IPV4, namenode_port),
            data_dir: CLIENT_DATA_DIR.to_string(),
        });
        let test_block_one = crate::proto::BlockMetadata {
            block_id: TEST_BLOCK_GET_ID_ONE.to_string(),
            seq: 0,
            datanodes: vec![format!("{}:{}", LOCALHOST_IPV4, datanode_port)],
        };
        let test_block_two = crate::proto::BlockMetadata {
            block_id: TEST_BLOCK_GET_ID_TWO.to_string(),
            seq: 1,
            datanodes: vec![format!("{}:{}", LOCALHOST_IPV4, datanode_port)],
        };

        let get_response = GetResponse {
            file_blocks: vec![test_block_one, test_block_two],
        };
        let result = client.get(final_path, get_response).await;

        assert_ok!(result);

        let file_size = File::open(format!("{}/get_test.txt", CLIENT_DATA_DIR))
            .await
            .unwrap()
            .metadata()
            .await
            .unwrap()
            .len();
        assert_eq!((2 * BLOCK_SIZE - 100 * 1024 * 1024) as u64, file_size);

        let remove_result_local_file =
            tokio::fs::remove_file(format!("{}/get_test.txt", CLIENT_DATA_DIR)).await;
        let remove_result_block_one_file = tokio::fs::remove_file(format!(
            "{}/{}_{}.dat",
            DATANODE_DATA_DIR, TEST_BLOCK_GET_ID_ONE, 0
        ))
        .await;
        let remove_result_block_two_file = tokio::fs::remove_file(format!(
            "{}/{}_{}.dat",
            DATANODE_DATA_DIR, TEST_BLOCK_GET_ID_TWO, 1
        ))
        .await;
        assert_ok!(remove_result_block_one_file);
        assert_ok!(remove_result_block_two_file);
        assert_ok!(remove_result_local_file);
    }

    #[tokio::test]
    async fn test_put_multiple_blocks_expects_success() {
        let datanode_port = 50011u16;
        let namenode_port = 50012u16;

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
            namespace: RwLock::new(INode::Directory {
                path: PathBuf::from("/"),
                children: HashMap::new(),
            }),
            edit_log: RwLock::new(Default::default()),
            block_map: RwLock::new(Default::default()),
            flush_interval: 30000,
        });

        start_namenode(namenode.clone(), namenode_port).await;
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let datanode = DataNode {
            id: Uuid::try_parse(DATANODE_UUID).unwrap(),
            data_dir: DATANODE_DATA_DIR.to_string(),
            hostname_port: DATANODE_HOSTNAME_PORT.to_string(),
            datanode_namenode_service_client: Arc::new(Mutex::new(
                DataNodeNameNodeServiceClient::connect(format!(
                    "http://{}:{}",
                    LOCALHOST_IPV4, namenode_port
                ))
                .await
                .unwrap(),
            )),
            blocks: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(Mutex::new(Default::default())),
            heartbeat_interval: Default::default(),
            block_report_interval: Default::default(),
            metrics_interval: Default::default(),
        };

        start_datanode(datanode, datanode_port).await;
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // create the file
        let file_data_buffer = vec![0u8; BLOCK_SIZE * 2 - 12 * 1024 * 1024];
        let mut file = File::create(format!("{}/test_put_multiple_blocks.txt", CLIENT_DATA_DIR))
            .await
            .unwrap();
        file.write_all(&file_data_buffer).await.unwrap();

        let client = RSHDFSClient::from_config(RSHDFSConfig {
            namenode_address: format!("{}:{}", LOCALHOST_IPV4, namenode_port),
            data_dir: CLIENT_DATA_DIR.to_string(),
        });

        let block_one_metadata = crate::proto::BlockMetadata {
            block_id: TEST_BLOCK_PUT_ID_ONE.to_string(),
            seq: 0,
            datanodes: vec![format!("{}:{}", LOCALHOST_IPV4, datanode_port)],
        };

        let block_two_metadata = crate::proto::BlockMetadata {
            block_id: TEST_BLOCK_PUT_ID_TWO.to_string(),
            seq: 1,
            datanodes: vec![format!("{}:{}", LOCALHOST_IPV4, datanode_port)],
        };

        let namenode_put_file_response = PutFileResponse {
            blocks: vec![block_one_metadata, block_two_metadata],
        };

        let local_file = File::open(format!("{}/test_put_multiple_blocks.txt", CLIENT_DATA_DIR))
            .await
            .unwrap();

        let result = client.put(local_file, namenode_put_file_response).await;

        assert_eq!(
            result,
            Ok(vec![
                TEST_BLOCK_PUT_ID_ONE.to_string(),
                TEST_BLOCK_PUT_ID_TWO.to_string()
            ])
        );

        tokio::fs::remove_file(format!(
            "{}/{}_{}.dat",
            DATANODE_DATA_DIR, TEST_BLOCK_PUT_ID_ONE, 0
        ))
        .await
        .unwrap();
        tokio::fs::remove_file(format!(
            "{}/{}_{}.dat",
            DATANODE_DATA_DIR, TEST_BLOCK_PUT_ID_TWO, 1
        ))
        .await
        .unwrap();
        tokio::fs::remove_file(format!("{}/test_put_multiple_blocks.txt", CLIENT_DATA_DIR))
            .await
            .unwrap();
    }
}
