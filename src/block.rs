use prost_types::Timestamp;

use std::hash::{Hash, Hasher};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Block {
    block_metadata: BlockMetadata,
    data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct BlockMetadata {
    id: Uuid,
    generation_timestamp: Timestamp,
    length: i64,
    version: i32,
    checksum: String,
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
