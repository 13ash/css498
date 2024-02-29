use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::PathBuf;

use adler::Adler32;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use rs_hdfs::block::{BLOCK_CHUNK_SIZE, BLOCK_SIZE};
use rs_hdfs::datanode::block_manager::BlockManager;
use rs_hdfs::namenode::block_map::BlockMap;
use rs_hdfs::namenode::namenode::{INode, NameNode};
use rs_hdfs::proto::data_node_name_node_service_client::DataNodeNameNodeServiceClient;
use rs_hdfs::proto::data_node_name_node_service_server::DataNodeNameNodeServiceServer;
use rs_hdfs::proto::rshdfs_block_service_client::RshdfsBlockServiceClient;
use rs_hdfs::proto::rshdfs_block_service_server::RshdfsBlockServiceServer;
use rs_hdfs::proto::rshdfs_name_node_service_server::RshdfsNameNodeServiceServer;
use rs_hdfs::proto::{
    BlockChunk, BlockMetadata, BlockStreamInfo, GetBlockRequest, GetResponse, PutFileResponse,
};
use tonic::transport::Server;
use uuid::Uuid;

struct MockRshdfsClient;

impl MockRshdfsClient {
    fn new() -> Self {
        MockRshdfsClient {}
    }

    async fn bench_put(&self, mut local_file: File, response: PutFileResponse) {
        const MAX_READ_BUFFER_SIZE: usize = 1024;

        let blocks = response.blocks;

        for block in blocks.iter().cloned() {
            let offset = block.seq as u64 * BLOCK_SIZE as u64;
            local_file.seek(SeekFrom::Start(offset)).await.unwrap();

            let mut block_buffer = vec![0; BLOCK_SIZE];
            let mut total_read = 0;

            loop {
                if total_read >= BLOCK_SIZE {
                    break;
                }

                let mut read_buffer = vec![0; MAX_READ_BUFFER_SIZE];
                let read_bytes = local_file.read(&mut read_buffer).await.unwrap();

                if read_bytes == 0 {
                    break; // EOF reached
                }

                for byte in 0..read_bytes {
                    block_buffer[total_read + byte] = read_buffer[byte]
                }
                total_read += read_bytes;
            }
            block_buffer.truncate(total_read);
            let buffer_size = block_buffer.len();

            for addr in block.datanodes.clone() {
                let mut block_manager_client =
                    RshdfsBlockServiceClient::connect(format!("http://{}", addr))
                        .await
                        .unwrap();

                let (client, server) = mpsc::channel::<BlockChunk>(50);
                let receiver_stream = ReceiverStream::new(server);
                let block_buffer_clone = block_buffer.clone();
                let block_id = block.block_id.clone();
                let block_seq = block.seq.clone();

                tokio::spawn(async move {
                    let mut seq = 0;
                    for chunk_start in (0..buffer_size).step_by(BLOCK_CHUNK_SIZE) {
                        let chunk_end = usize::min(chunk_start + BLOCK_CHUNK_SIZE, buffer_size);
                        let chunk = &block_buffer_clone[chunk_start..chunk_end];

                        let mut adler = Adler32::new();
                        adler.write_slice(&chunk);
                        let checksum = adler.checksum();

                        let block_chunk = BlockChunk {
                            block_id: block_id.clone(),
                            block_seq,
                            chunk_seq: seq,
                            chunked_data: chunk.to_vec(),
                            checksum,
                        };
                        seq += 1;
                        client
                            .send(block_chunk)
                            .await
                            .expect("Failed to send chunk");
                    }
                });

                block_manager_client
                    .put_block_streamed(Request::new(receiver_stream))
                    .await
                    .unwrap();
            }
        }
    }

    async fn bench_get(&self, final_data_path: PathBuf, response: GetResponse) {
        let mut block_map = HashMap::new();

        let mut block_service_client = RshdfsBlockServiceClient::connect("http://0.0.0.0:50001")
            .await
            .unwrap();

        for block in response.file_blocks.iter() {
            let mut success = false;

            let get_block_request = GetBlockRequest {
                block_id: block.block_id.to_string(),
            };

            let mut stream = block_service_client
                .get_block_streamed(Request::new(get_block_request))
                .await
                .unwrap()
                .into_inner();

            let mut block_buffer = Vec::new();

            while let Some(chunk_result) = stream.message().await.unwrap() {
                let chunk = chunk_result;
                let mut adler = Adler32::new();
                adler.write_slice(&chunk.chunked_data);
                if adler.checksum() == chunk.checksum {
                    block_buffer.extend_from_slice(&chunk.chunked_data);
                    success = true;
                } else {
                    success = false;
                    break; // Checksum mismatch, try the next datanode
                }
            }

            if success {
                let temp_file_path = format!("/tmp/{}.tmp", block.block_id);
                let mut temp_file = File::create(&temp_file_path).await.unwrap();
                temp_file.write_all(&block_buffer).await.unwrap();
                block_map.insert(block.seq, temp_file_path);
                break;
            }
        }

        let mut final_file = File::create(&final_data_path).await.unwrap();

        let mut keys: Vec<_> = block_map.keys().collect();
        keys.sort();

        for key in keys {
            if let Some(temp_file_path) = block_map.get(key) {
                let mut temp_file = File::open(temp_file_path).await.unwrap();
                let mut data_buffer = Vec::new();
                temp_file.read_to_end(&mut data_buffer).await.unwrap();
                final_file.write_all(&data_buffer).await.unwrap();
                tokio::fs::remove_file(temp_file_path).await.unwrap();
            }
        }
    }
}

async fn start_mock_servers() {
    let mock_namenode = Arc::new(NameNode {
        id: Uuid::new_v4(),
        data_dir: "/benches/namenode".to_string(),
        ipc_address: "0.0.0.0:50000".to_string(),
        replication_factor: 3,
        datanodes: RwLock::new(Vec::new()),
        namespace: Arc::new(RwLock::new(INode::Directory {
            path: PathBuf::from("/"),
            children: HashMap::new(),
        })),
        block_map: BlockMap::new(),
    });

    let mock_block_manager_one = Arc::new(BlockManager {
        datanode_service_client: Arc::new(Mutex::new(
            DataNodeNameNodeServiceClient::connect("http://0.0.0.0:50000")
                .await
                .unwrap(),
        )),
        data_dir: "benches/datanode".to_string(),
        blocks: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        interval: Duration::from_millis(15000),
    });

    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(RshdfsNameNodeServiceServer::new(mock_namenode.clone()))
            .add_service(DataNodeNameNodeServiceServer::new(mock_namenode.clone()))
            .serve("0.0.0.0:50000".parse().unwrap())
            .await;
    });

    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(RshdfsBlockServiceServer::new(
                mock_block_manager_one.clone(),
            ))
            .serve("0.0.0.0:50001".parse().unwrap())
            .await;

    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    let mut iteration = 000;

    runtime.block_on(start_mock_servers());

    let mut group = c.benchmark_group("Block Service");
    let mock_rshdfs_client = MockRshdfsClient::new();

    group.sample_size(50);
    group.bench_function("Put Block Stream", |bencher| {
        iteration += 1;

        bencher.to_async(&runtime).iter(|| async {
            let block_uuid = format!("12345678-1234-5678-1{}-567812345678", iteration);

            let block_one_metadata = BlockMetadata {
                block_id: block_uuid,
                seq: 0,
                datanodes: vec!["0.0.0.0:50001".to_string()],
            };

            let put_file_response = PutFileResponse {
                blocks: vec![block_one_metadata],
            };
            let local_file = File::open("benches/bench.txt").await.unwrap();
            mock_rshdfs_client
                .bench_put(local_file, put_file_response)
                .await;
        });
    });

    group.bench_function("Get Block Stream", |bencher| {
        iteration += 1;

        bencher.to_async(&runtime).iter(|| async {
            let block_one = BlockMetadata {
                block_id: "12345678-1234-5678-1111-567812345678".to_string(),
                seq: 0,
                datanodes: vec!["0.0.0.0:50001".to_string()],
            };
            let block_two = BlockMetadata {
                block_id: "12345678-1234-5678-1112-567812345678".to_string(),
                seq: 1,
                datanodes: vec!["0.0.0.0:50001".to_string()],
            };

            let response = GetResponse {
                file_blocks: vec![block_one.clone(), block_two.clone()],
            };

            let path = PathBuf::from(format!("benches/get/get_bench{}.txt", iteration));

            let request_one = BlockStreamInfo {
                block_id: block_one.block_id,
                block_seq: block_one.seq,
            };

            let request_two = BlockStreamInfo {
                block_id: block_two.block_id,
                block_seq: block_two.seq,
            };

            let mut block_service_client = RshdfsBlockServiceClient::connect("http://0.0.0.0:50001")
                .await
                .unwrap();
            block_service_client
                .start_block_stream(request_one)
                .await
                .unwrap();
            block_service_client
                .start_block_stream(request_two)
                .await
                .unwrap();

            mock_rshdfs_client.bench_get(path, response).await;
        });
    });

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
