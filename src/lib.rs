pub mod block;
pub mod config;
pub mod datanode;
pub mod error;
pub mod fs;
pub mod namenode;

pub mod proto {
    tonic::include_proto!("rs_hdfs.proto");
}