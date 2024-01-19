use rs_hdfs::config::datanode_config::DataNodeConfig;
use rs_hdfs::datanode::datanode::DataNode;
use rs_hdfs::error::{Result, RSHDFSError};
use tokio;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let config = DataNodeConfig::from_xml_file("config/datanode.xml")?;
    let data_node = DataNode::from_config(config).await;

    Ok(())
}
