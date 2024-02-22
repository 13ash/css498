use std::hash::{Hash, Hasher};
use uuid::Uuid;

pub const BLOCK_SIZE: usize = 128 * 1024 * 1024; // 128MB
pub const BLOCK_CHUNK_SIZE: usize = 64 * 1024; // 64KB chunks

#[derive(Debug, Clone, PartialEq)]
pub enum BlockStatus {
    Waiting = 0,
    InProgress = 1,
    Written = 2,
    AwaitingDeletion = 3,
}

#[derive(Debug, Clone)]
pub struct Block {
    pub block_metadata: BlockMetadata,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct BlockMetadata {
    pub id: Uuid,
    pub seq: i32,
    pub status: BlockStatus,
    pub size: usize,
    pub datanodes: Vec<String>,
}

impl PartialEq for BlockMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.block_metadata.id == other.block_metadata.id
    }
}

impl Eq for Block {}

impl Hash for Block {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.block_metadata.id.hash(state);
    }
}
