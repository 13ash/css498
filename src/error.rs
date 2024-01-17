

#[derive(Debug, PartialEq)]
pub enum RSHDFSError {
    ConfigError(String),
    ProtoError(String),
    FileSystemError(String),
    InsufficientSpace(String),
    HeartBeatFailed(String),
}


impl From<toml::de::Error> for RSHDFSError {
    fn from(error: toml::de::Error) -> Self {
        RSHDFSError::ConfigError(error.to_string())
    }
}


impl From<prost::EncodeError> for RSHDFSError {
    fn from(error: prost::EncodeError) -> Self {
        RSHDFSError::ProtoError(error.to_string())
    }
}

impl From<prost::DecodeError> for RSHDFSError {
    fn from(error: prost::DecodeError) -> Self {
        RSHDFSError::ProtoError(error.to_string())
    }
}

pub type Result<T> = std::result::Result<T, RSHDFSError>;