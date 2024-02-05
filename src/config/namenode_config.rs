use crate::error::RSHDFSError;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct NameNodeConfig {
    #[serde(rename = "data.dir")]
    pub data_dir: String,

    #[serde(rename = "ipc.address")]
    pub ipc_address: String,

    #[serde(rename = "replication.factor")]
    pub replication_factor: i8,
}

impl NameNodeConfig {
    pub fn from_xml_file(file_path: &str) -> Result<Self, RSHDFSError> {
        let xml_str = std::fs::read_to_string(file_path).unwrap();
        let config = serde_xml_rs::from_str(&xml_str)?;
        Ok(config)
    }
}
