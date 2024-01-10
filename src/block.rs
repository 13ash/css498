use std::hash::{Hash, Hasher};
use serde::{Serialize, Deserialize};

use crate::proto;

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