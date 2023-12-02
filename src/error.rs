#[derive(Debug, PartialEq)]
pub enum RSDFSError {
    ConfigError(String),
    ProtoError(String),
    FileSystemError(String),

}


impl From<toml::de::Error> for RSDFSError {
    fn from(error: toml::de::Error) -> Self {
        RSDFSError::ConfigError(error.to_string())
    }
}


impl From<prost::EncodeError> for RSDFSError {
    fn from(error: prost::EncodeError) -> Self {
        RSDFSError::ProtoError(error.to_string())
    }
}

impl From<prost::DecodeError> for RSDFSError {
    fn from(error: prost::DecodeError) -> Self {
        RSDFSError::ProtoError(error.to_string())
    }
}

pub type Result<T> = std::result::Result<T, RSDFSError>;