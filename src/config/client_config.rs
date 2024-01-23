use serde::Deserialize;
use crate::config::datanode_config::DataNodeConfig;
use crate::error::RSHDFSError;

#[derive(Debug, Deserialize)]
pub struct ClientConfig {
    #[serde(rename = "namenode.address")]
    pub namenode_address: String,
}


impl ClientConfig {
    pub fn from_xml_file(file_path: &str) -> Result<Self, RSHDFSError> {
        let xml_str = std::fs::read_to_string(file_path).unwrap();
        let config: ClientConfig = serde_xml_rs::from_str(&xml_str)?;
        Ok(config)
    }
}