use chrono::Local;
use rs_hdfs::config::namenode_config::NameNodeConfig;
use rs_hdfs::error::Result;
use rs_hdfs::namenode::namenode::NameNode;
use rs_hdfs::proto::data_node_name_node_service_server::DataNodeNameNodeServiceServer;
use rs_hdfs::proto::rshdfs_name_node_service_server::RshdfsNameNodeServiceServer;
use std::sync::Arc;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<()> {
    let config = NameNodeConfig::from_xml_file("config/namenode.xml")?;
    let name_node = Arc::new(NameNode::from_config(config.clone()).await?);
    let addr = config.ipc_address.parse().unwrap(); //todo: unwrap

    let now = Local::now();
    println!("Time: {}", now.format("%Y-%m-%d %H:%M:%S"));

    // Server address
    println!("Listening on: {}", addr);

    // Start the server
    Server::builder()
        .add_service(DataNodeNameNodeServiceServer::new(name_node.clone()))
        .add_service(RshdfsNameNodeServiceServer::new(name_node.clone()))
        .serve(addr)
        .await?;

    Ok(())
}
