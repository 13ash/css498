use rs_hdfs::error::Result;
use clap::Parser;
use tonic::{Response, Status};
use rs_hdfs::config::client_config::ClientConfig;
use rs_hdfs::proto::client_protocol_client::ClientProtocolClient;
use rs_hdfs::proto::{CreateRequest};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    operation: String,
    #[arg(short, long)]
    file_path: Option<String>,
    #[arg(short, long)]
    data: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = ClientConfig::from_xml_file("/config/client.xml")?;
    let mut client = ClientProtocolClient::connect(format!("http://{}", config.namenode_address)).await?;
    match args.operation.as_str() {
        "create" => {
            match args.file_path {
                None => {
                    println!("File path not provided!");
                }
                Some(file_path) => {
                    let request = CreateRequest {
                        path: file_path,
                    };
                    match client.create(request).await {
                        Ok(response) => {
                            println!("Create file response: {:?}", response);
                        }
                        Err(e) => {
                            eprintln!("Failed to create file: {:?}", e);
                        }
                    }
                }
            }
        },
        "delete" => {

        }
        _ => println!("Unsupported Operation")
    }

    Ok(())
}
