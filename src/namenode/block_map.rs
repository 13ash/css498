use crate::block::BlockMetadata;
use crate::error::RSHDFSError;

use std::collections::HashMap;

use tokio::sync::RwLock;
use uuid::Uuid;

/// Central data-structure used by the NameNode to track block locations.
pub struct BlockMap {
    // Maps a block ID to a Block and the DataNodes storing it.
    pub blocks: RwLock<HashMap<Uuid, BlockMetadata>>,
}

impl BlockMap {
    pub fn new() -> Self {
        BlockMap {
            blocks: RwLock::new(HashMap::new()),
        }
    }

    /// Adds a block to the BlockMap.
    pub async fn add_block(&self, block: BlockMetadata) {
        let mut blocks = self.blocks.write().await;
        blocks.insert(block.id, block);
    }

    /// Retrieves a block and its associated DataNodes.
    pub async fn get_block(&self, block_id: Uuid) -> Result<BlockMetadata, RSHDFSError> {
        let blocks_guard = self.blocks.read().await;

        match blocks_guard.get(&block_id) {
            None => Err(RSHDFSError::BlockMapError(String::from("Block not found."))),
            Some(block) => Ok(block.clone()),
        }
    }

    pub async fn modify_block_metadata<F>(
        &self,
        block_id: Uuid,
        modify: F,
    ) -> Result<(), RSHDFSError>
    where
        F: FnOnce(&mut BlockMetadata),
    {
        let mut blocks_guard = self.blocks.write().await;

        match blocks_guard.get_mut(&block_id) {
            None => Err(RSHDFSError::BlockMapError(String::from("Block not found"))),
            Some(block_metadata) => {
                modify(block_metadata);
                Ok(())
            }
        }
    }

    pub async fn remove_block(&self, block_id: Uuid) -> Result<(), RSHDFSError> {
        let mut blocks_guard = self.blocks.write().await;
        match blocks_guard.remove(&block_id) {
            None => Err(RSHDFSError::BlockMapError(String::from("Block not found."))),
            Some(_) => Ok(()),
        }
    }
}
