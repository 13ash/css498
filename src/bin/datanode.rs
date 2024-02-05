use rs_hdfs::config::datanode_config::DataNodeConfig;
use rs_hdfs::datanode::datanode::DataNode;
use rs_hdfs::error::Result;
use rs_hdfs::proto::rshdfs_data_node_service_server::RshdfsDataNodeServiceServer;
use tokio;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let config = DataNodeConfig::from_xml_file("config/datanode.xml")?;
    let addr = config.ipc_address.parse().unwrap();
    let mut data_node = DataNode::from_config(config).await?;
    data_node.start().await?;

    Server::builder()
        .add_service(RshdfsDataNodeServiceServer::new(data_node))
        .serve(addr)
        .await?;

    Ok(())
}
