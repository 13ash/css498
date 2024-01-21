#[derive(Debug, PartialEq)]
pub enum RSHDFSError {
    ConfigError(String),
    ConnectionError(String),
    TonicError(String),
    ProtoError(String),
    FileSystemError(String),
    IOError(String),
    InsufficientSpace(String),
    HeartBeatFailed(String),
    RegistrationFailed(String),
}

impl From<tonic::transport::Error> for RSHDFSError {
    fn from(error: tonic::transport::Error) -> Self {
        RSHDFSError::TonicError(error.to_string())
    }
}

impl From<toml::de::Error> for RSHDFSError {
    fn from(error: toml::de::Error) -> Self {
        RSHDFSError::ConfigError(error.to_string())
    }
}

impl From<std::io::Error> for RSHDFSError {
    fn from(error: std::io::Error) -> Self {
        RSHDFSError::IOError(error.to_string())
    }
}

impl From<prost::EncodeError> for RSHDFSError {
    fn from(error: prost::EncodeError) -> Self {
        RSHDFSError::ProtoError(error.to_string())
    }
}

impl From<serde_xml_rs::Error> for RSHDFSError {
    fn from(error: serde_xml_rs::Error) -> Self {
        RSHDFSError::ConfigError(error.to_string())
    }
}

impl From<prost::DecodeError> for RSHDFSError {
    fn from(error: prost::DecodeError) -> Self {
        RSHDFSError::ProtoError(error.to_string())
    }
}

pub type Result<T> = std::result::Result<T, RSHDFSError>;
