use crate::namenode::namenode::INode;
use std::sync::{PoisonError, RwLockWriteGuard};
use tonic::Status;

#[derive(Debug, PartialEq)]
pub enum RSHDFSError {
    DiskError(String),
    ConfigError(String),
    ConnectionError(String),
    ChecksumError(String),
    TonicError(String),
    ProtoError(String),
    PutBlockStreamedError(String),
    GetBlockStreamedError(String),
    FileSystemError(String),
    InvalidPathError(String),
    IOError(String),
    InsufficientSpace(String),
    InsufficientDataNodes(String),
    HeartBeatFailed(String),
    RegistrationError(String),
    LockError(String),
    BlockMapError(String),
    MetricsError(String),
    ReadError(String),
    WriteError(String),
    DataValidationError(String),
    GrpcError(String),
    StreamError(String),
    EditLogError(String),
    UUIDError(String),
    BlockReportError(String),
    GetError(String),
    PutError(String),
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

impl From<PoisonError<RwLockWriteGuard<'_, INode>>> for RSHDFSError {
    fn from(error: PoisonError<RwLockWriteGuard<'_, INode>>) -> RSHDFSError {
        RSHDFSError::LockError(format!("Lock error: {}", error.to_string()))
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

impl From<RSHDFSError> for Status {
    fn from(error: RSHDFSError) -> Self {
        match error {
            RSHDFSError::InvalidPathError(msg) => Status::invalid_argument(msg),
            RSHDFSError::FileSystemError(msg) => Status::internal(msg),
            RSHDFSError::LockError(msg) => Status::aborted(msg),
            RSHDFSError::PutBlockStreamedError(msg) => Status::internal(msg),
            RSHDFSError::GetBlockStreamedError(msg) => Status::internal(msg),
            RSHDFSError::PutError(msg) => Status::internal(msg),
            RSHDFSError::IOError(msg) => Status::internal(msg),
            _ => Status::unknown("Unknown error"),
        }
    }
}

pub type Result<T> = std::result::Result<T, RSHDFSError>;
