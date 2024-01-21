use rs_hdfs::config::datanode_config::DataNodeConfig;
use rs_hdfs::datanode::datanode::DataNode;
use rs_hdfs::error::Result;
use tokio;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let config = DataNodeConfig::from_xml_file("config/datanode.xml")?;
    let mut data_node = DataNode::from_config(config).await?;
    data_node.start().await?;

    Ok(())
}
