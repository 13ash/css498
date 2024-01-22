use crate::block::BlockMetadata;

use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub struct BlockReport {
    datanode_id: Uuid,
    blocks: Vec<BlockMetadata>,
}

impl BlockReport {
    pub fn new(datanode_id: Uuid, blocks: Vec<BlockMetadata>) -> Self {
        BlockReport {
            datanode_id,
            blocks,
        }
    }
}
