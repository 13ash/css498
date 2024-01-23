use std::sync::Arc;
use chrono::Local;
use rs_hdfs::config::namenode_config::NameNodeConfig;
use rs_hdfs::error::Result;
use rs_hdfs::namenode::namenode::NameNode;
use rs_hdfs::proto::data_node_service_server::DataNodeServiceServer;

use tonic::transport::Server;
use rs_hdfs::proto::client_protocol_server::ClientProtocolServer;

#[tokio::main]
async fn main() -> Result<()> {
    let config = NameNodeConfig::from_xml_file("config/namenode.xml")?;
    let name_node = Arc::new(NameNode::from_config(config.clone()).await?);
    let addr = config.ipc_address.parse().unwrap();

    let now = Local::now();
    println!("Time: {}", now.format("%Y-%m-%d %H:%M:%S"));

    // Server address
    println!("Listening on: {}", addr);

    // Start the server
    Server::builder()
        .add_service(DataNodeServiceServer::new(name_node.clone()))
        .add_service(ClientProtocolServer::new(name_node.clone()))
        .serve(addr)
        .await?;

    Ok(())
}
