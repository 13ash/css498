use serde::Deserialize;

use crate::error::RSHDFSError;

#[derive(Debug, Deserialize)]
pub struct RSHDFSConfig {
    #[serde(rename = "namenode.address")]
    pub namenode_address: String,
}

impl RSHDFSConfig {
    pub fn from_xml_file(file_path: &str) -> Result<Self, RSHDFSError> {
        let xml_str = std::fs::read_to_string(file_path).unwrap(); //todo: unwrap
        let config: RSHDFSConfig = serde_xml_rs::from_str(&xml_str)?;
        Ok(config)
    }
}
