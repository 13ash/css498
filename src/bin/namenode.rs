use std::net::SocketAddr;
use std::str::FromStr;
use chrono::Local;
use rs_hdfs::config::namenode_config::NameNodeConfig;
use rs_hdfs::error::Result;
use rs_hdfs::namenode::namenode::NameNode;
use rs_hdfs::proto::data_node_service_server::DataNodeServiceServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<()> {
    let config = NameNodeConfig::from_xml_file("config/namenode.xml")?;
    let name_node = NameNode::from_config(config.clone()).await?;
    let addr = config.ipc_address.parse().unwrap();

    let now = Local::now();
    println!("Time: {}", now.format("%Y-%m-%d %H:%M:%S"));


    // Server address
    println!("Listening on: {}", addr);

    // Start the server
    Server::builder()
        .add_service(DataNodeServiceServer::new(name_node))
        .serve(addr)
        .await?;

    Ok(())
}
