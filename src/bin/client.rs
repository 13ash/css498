use rs_hdfs::error::Result;
use clap::Parser;
use rs_hdfs::config::client_config::ClientConfig;
use rs_hdfs::proto::client_protocol_client::ClientProtocolClient;
use rs_hdfs::proto::{CreateRequest, Path};

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
            let file_path = args.file_path;
            match file_path {
                None => {
                    println!("File path not provided!")
                }
                Some(path_string) => {
                    let request = CreateRequest {
                        path : Option::from(Path {
                            path: path_string,
                        })
                    };
                    let response = client.create(request).await.unwrap().into_inner();
                    println!("{:?}", response);
                }
            }


        },
        _ => println!("Unsupported Operation")
    }

    Ok(())
}
