
use rs_hdfs::error::Result;
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    Ok(())
}