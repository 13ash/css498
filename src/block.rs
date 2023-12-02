use crate::proto;

use std::hash::{Hash, Hasher};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    id: u64,
    size: u32,
    creation_time: u64,
    last_modified: u64,
    last_accessed: u64,
    state: BlockState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BlockState {
    Default,
    UnderConstruction,
    Ready,
    Corrupted,
}

impl BlockState {

}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Block {}

impl Hash for Block {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}


// Implement conversion from protobuf enum to Rust enum
impl From<proto::BlockState> for BlockState {
    fn from(state: proto::BlockState) -> Self {
        match state {
            proto::BlockState::Default => BlockState::Default,
            proto::BlockState::UnderConstruction => BlockState::UnderConstruction,
            proto::BlockState::Ready => BlockState::Ready,
            proto::BlockState::Corrupted => BlockState::Corrupted,
        }
    }
}

impl From<proto::Block> for Block {
    fn from(proto_block: proto::Block) -> Self {
        Block {
            id: proto_block.id,
            size: proto_block.size,
            creation_time: proto_block.creation_time,
            last_modified: proto_block.last_modified,
            last_accessed: proto_block.last_accessed,
            state: BlockState::from(proto_block.state()),
        }
    }
}
