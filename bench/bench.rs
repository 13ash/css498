use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use criterion::{Criterion, criterion_group, criterion_main};
use tokio::fs::File;
use tokio::sync::{Mutex, RwLock};
use tonic::transport::Server;
use uuid::Uuid;

use rs_hdfs::config::rshdfs_config::RSHDFSConfig;
use rs_hdfs::datanode::datanode::{BlockInfo, DataNode};
use rs_hdfs::namenode::namenode::{INode, NameNode, DataNode as NameNodeDataNode, DataNodeStatus};
use rs_hdfs::proto::{BlockMetadata, BlockStreamInfo, GetResponse, PutFileResponse};
use rs_hdfs::proto::data_node_name_node_service_client::DataNodeNameNodeServiceClient;
use rs_hdfs::proto::data_node_name_node_service_server::DataNodeNameNodeServiceServer;
use rs_hdfs::proto::rshdfs_data_node_service_client::RshdfsDataNodeServiceClient;
use rs_hdfs::proto::rshdfs_data_node_service_server::RshdfsDataNodeServiceServer;
use rs_hdfs::rshdfs::client::{Client, RSHDFSClient};

const DATANODE_ONE_UUID: &str = "00000000-0001-0001-0001-000000000001";
const DATANODE_GET_BLOCK_UUID: &str = "00000000-1111-1111-1111-222222222222";
const DATANODE_ONE_DATA_DIR: &str = "bench/datanode1";
const LOCALHOST_IPV4: &str = "127.0.0.1";
const CLIENT_DATA_DIR: &str = "bench/client";


criterion_main!(bench);
criterion_group!(bench, criterion_benchmark);

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

async fn start_bench_servers() {
    let mock_namenode = Arc::new(NameNode {
        id: Uuid::new_v4(),
        data_dir: "".to_string(),
        ipc_address: "127.0.0.1:50000".to_string(),
        replication_factor: 3,
        datanodes: RwLock::new(vec![NameNodeDataNode{
            id: Uuid::try_parse(DATANODE_ONE_UUID).unwrap(),
            addr: format!("{}:{}", LOCALHOST_IPV4, 50001u16),
            status: DataNodeStatus::HEALTHY,
        }]),
        namespace: RwLock::new(INode::Directory {
            path: PathBuf::from("/"),
            children: HashMap::new(),
        }),
        edit_log: Default::default(),
        block_map: Default::default(),
        flush_interval: 15000,
    });

    println!("Starting NameNode on port: {}", 50000u16);
    start_namenode(mock_namenode.clone(), 50000u16).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mock_datanode = DataNode {
        id: Uuid::try_parse(DATANODE_ONE_UUID).unwrap(),
        data_dir: DATANODE_ONE_DATA_DIR.to_string(),
        hostname_port: format!("{}:{}", LOCALHOST_IPV4, 50001u16),
        datanode_namenode_service_client: Arc::new(Mutex::from(DataNodeNameNodeServiceClient::connect(format!("http://{}:{}", LOCALHOST_IPV4, 50000u16)).await.unwrap())),
        blocks: Arc::new(RwLock::new(HashMap::from([(Uuid::try_parse(DATANODE_GET_BLOCK_UUID).unwrap(), BlockInfo {
            id: DATANODE_GET_BLOCK_UUID.to_string(),
            seq: 0,
        })]))),
        metrics: Arc::new(Default::default()),
        heartbeat_interval: Default::default(),
        block_report_interval: Default::default(),
        metrics_interval: Default::default(),
    };

    start_datanode(mock_datanode, 50001u16).await;
    println!("Starting DataNode on port: {}", 50001u16);
    tokio::time::sleep(Duration::from_millis(200)).await;
}

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    let mut iteration = 0;
    runtime.block_on(start_bench_servers());

    let mut group = c.benchmark_group("Block Service");
    let rshdfs_client = RSHDFSClient::from_config(RSHDFSConfig {
        namenode_address: format!("{}:{}", LOCALHOST_IPV4, 50000u16),
        data_dir: CLIENT_DATA_DIR.to_string(),
    });

    group.sample_size(100);
    group.bench_function("Put Block Stream", |bencher| {

        bencher.to_async(&runtime).iter(|| async {
            let block_uuid = Uuid::new_v4();

            let block_one_metadata = BlockMetadata {
                block_id: block_uuid.to_string(),
                seq: 0,
                datanodes: vec!["127.0.0.1:50001".to_string()],
            };

            let put_file_response = PutFileResponse {
                blocks: vec![block_one_metadata],
            };
            let local_file = File::open("bench/bench.txt").await.unwrap();
            rshdfs_client
                .put(local_file, put_file_response)
                .await.unwrap();
        });
    });

    group.bench_function("Get Block Stream", |bencher| {
        iteration += 1;
        bencher.to_async(&runtime).iter(|| async {

            let block_one = BlockMetadata {
                block_id: DATANODE_GET_BLOCK_UUID.to_string(),
                seq: 0,
                datanodes: vec![format!("{}:{}", LOCALHOST_IPV4, 50001u16)],
            };
            let get_response = GetResponse {
                file_blocks: vec![block_one],
            };
            let final_data_path = PathBuf::from(format!("{}/get_bench{}.txt", CLIENT_DATA_DIR, iteration));
            rshdfs_client.get(final_data_path, get_response).await.unwrap();
        })
    });
    group.finish();
}







