pub mod error;
pub mod block;
pub mod fs;
pub mod datanode;
pub mod namenode;
pub mod config;


pub mod proto {
    tonic::include_proto!("rs_hdfs.proto");
}
