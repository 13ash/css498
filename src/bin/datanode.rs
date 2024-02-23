use rs_hdfs::config::datanode_config::DataNodeConfig;
use rs_hdfs::datanode::datanode::DataNode;
use rs_hdfs::error::Result;
use rs_hdfs::proto::rshdfs_block_service_server::RshdfsBlockServiceServer;
use tokio;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let config = DataNodeConfig::from_xml_file("config/datanode.xml")?;
    let addr = config.ipc_address.parse().unwrap();
    let mut data_node = DataNode::from_config(config).await?;
    let block_manager = data_node.block_manager.clone();
    data_node.start().await?;

    Server::builder()
        .add_service(RshdfsBlockServiceServer::new(block_manager))
        .serve(addr)
        .await?;

    Ok(())
}
