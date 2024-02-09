use std::io::Write;
use std::path::PathBuf;
use clap::{Parser, Subcommand};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use rs_hdfs::error::{RSHDFSError, Result};

use rs_hdfs::config::rshdfs_config::RSHDFSConfig;
use rs_hdfs::proto::rshdfs_name_node_service_client::RshdfsNameNodeServiceClient;
use rs_hdfs::proto::{ConfirmFileWriteRequest, CreateRequest, LsRequest, ReadRequest, WriteFileRequest};
use rs_hdfs::rshdfs::handler::{read_handler, write_block_handler};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Get {
        #[arg(short, long)]
        fp: String,
    },
    Put {
        #[arg(short, long)]
        fp: String,
        #[arg(short, long)]
        lfp: String,
    },
    Delete {
        #[arg(short, long)]
        fp: String,
    },
    Ls {
        #[arg(short, long)]
        fp: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let config = RSHDFSConfig::from_xml_file("/config/rshdfs.xml")?;
    let mut namenode_client =
        RshdfsNameNodeServiceClient::connect(format!("http://{}", config.namenode_address)).await?;

    match &args.command {

        Commands::Get { fp } => {
            let request = ReadRequest {
                path: fp.clone(),
            };
            let unmatched_response = namenode_client.read(request).await;
            match unmatched_response {
                Ok(response) => {
                    println!("Read file response: {:?}", response);
                    let data = read_handler(response.into_inner()   ).await?;

                    let file_pathbuf = PathBuf::from(fp);
                    let file_name = file_pathbuf.as_path().components().last().unwrap().as_os_str().to_str().unwrap();
                    let temp_file_path = format!("/tmp/{}", file_name);



                    let mut temp_file = File::create(format!("/tmp/{}", file_name)).await.unwrap();
                    temp_file.write_all(&data).await?;

                    Command::new("vim")
                        .arg(temp_file_path)
                        .status()
                        .await
                        .expect("Failed to open editor");

                }
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                }
            }
        }

        Commands::Ls { fp } => {
            let request = LsRequest {
                path: fp.clone(),
            };
            let unmatched_response = namenode_client.ls(request).await;
            match unmatched_response {
                Ok(response) => {
                    println!("{:?}", response.into_inner().inodes);
                }
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                }
            }
        }

        Commands::Put {
            fp,
            lfp,
        } => {
            let local_file = tokio::fs::File::open(lfp)
                .await
                .map_err(|_| RSHDFSError::FileSystemError("File not found.".to_string()))?;
            let local_file_size = local_file.metadata().await.unwrap().len();
            let request = WriteFileRequest {
                path: fp.clone(),
                file_size: local_file_size,
            };
            let unmatched_response = namenode_client.write_file(request).await;
            match unmatched_response {
                Ok(response) => {
                    let inner_response = response.into_inner().clone();
                    println!("{:?}", inner_response.blocks);

                    match write_block_handler(local_file, inner_response).await {
                        Ok(written_blocks) => {
                            match namenode_client.confirm_file_write(ConfirmFileWriteRequest {
                                path: fp.clone(),
                                block_ids: written_blocks,
                            }).await {
                                Ok(_) => println!("File write confirmed successfully."),
                                Err(e) => eprintln!("Error confirming file write: {:?}", e),
                            }
                        }
                        Err(e) => eprintln!("Error writing blocks: {:?}", e),
                    };
                }
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                }
            }
        }
        _ => {
            eprintln!("Not valid command")
        }
    }
    Ok(())
}
