use rs_hdfs::config::datanode_config::DataNodeConfig;
use rs_hdfs::datanode::datanode::DataNode;
use rs_hdfs::error::Result;
use tokio;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let config = DataNodeConfig::from_xml_file("config/datanode.xml")?;
    let mut data_node = DataNode::from_config(config).await?;
    data_node.start().await?;

    // Register a signal handler for graceful shutdown
    let _ = signal::ctrl_c().await;
    println!("Received shutdown signal, shutting down...");

    Ok(())
}
