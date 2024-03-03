use chrono::Local;
use rs_hdfs::config::datanode_config::DataNodeConfig;
use rs_hdfs::datanode::datanode::DataNode;
use rs_hdfs::error::Result;
use rs_hdfs::proto::rshdfs_data_node_service_server::RshdfsDataNodeServiceServer;
use tokio;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<()> {
    let config = DataNodeConfig::from_xml_file("config/datanode.xml")?;
    let addr = config.ipc_address.parse().unwrap();
    let mut datanode: DataNode = DataNode::from_config(config).await;
    datanode.start().await?;

    let now = Local::now();
    println!("Time: {}", now.format("%Y-%m-%d %H:%M:%S"));

    // Server address
    println!("Listening on: {}", addr);

    Server::builder()
        .add_service(RshdfsDataNodeServiceServer::new(datanode))
        .serve(addr)
        .await?;

    Ok(())
}
