use crate::error::RSHDFSError;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct DataNodeConfig {
    #[serde(rename = "data.dir")]
    pub data_dir: String,

    #[serde(rename = "ipc.address")]
    pub ipc_address: String,

    #[serde(rename = "namenode.address")]
    pub namenode_address: String,

    #[serde(rename = "heartbeat.interval")]
    pub heartbeat_interval: u64,

    #[serde(rename = "block.report.interval")]
    pub block_report_interval: u64,
}

impl DataNodeConfig {
    pub fn from_xml_file(file_path: &str) -> Result<Self, RSHDFSError> {
        let xml_str = std::fs::read_to_string(file_path).unwrap();
        let config: DataNodeConfig = serde_xml_rs::from_str(&xml_str)?;
        Ok(config)
    }
}
