use chrono::Local;
use rs_hdfs::config::datanode_config::DataNodeConfig;
use rs_hdfs::datanode::datanode::DataNode;
use rs_hdfs::error::Result;
use rs_hdfs::proto::rshdfs_data_node_service_server::RshdfsDataNodeServiceServer;
use tokio;
use tonic::transport::Server;
use tracing::error;
use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::main]
async fn main() -> Result<()> {
    let config = DataNodeConfig::from_xml_file("config/datanode.xml")?;
    let addr = config.ipc_address.parse().unwrap();

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let mut datanode: DataNode = DataNode::from_config(config).await;
    match datanode.start().await {
        Ok(_) => {}
        Err(_) => {
            error!("Failed to configure datanode.");
        }
    }

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
