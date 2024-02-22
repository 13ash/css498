use crate::block::BlockMetadata;
use crate::error::RSHDFSError;

use std::collections::HashMap;

use std::sync::RwLock;
use uuid::Uuid;

/// Central data-structure used by the NameNode to track block locations.
pub struct BlockMap {
    // Maps a block ID to a Block and the DataNodes storing it.
    blocks: RwLock<HashMap<Uuid, BlockMetadata>>,
}

impl BlockMap {
    pub fn new() -> Self {
        BlockMap {
            blocks: RwLock::new(HashMap::new()),
        }
    }

    /// Adds a block to the BlockMap.
    pub fn add_block(&self, block: BlockMetadata) {
        let mut blocks = self.blocks.write().unwrap();
        blocks.insert(block.id, block);
    }

    /// Retrieves a block and its associated DataNodes.
    pub fn get_block(&self, block_id: Uuid) -> Result<BlockMetadata, RSHDFSError> {
        let blocks_guard = self
            .blocks
            .read()
            .map_err(|_| RSHDFSError::LockError(String::from("Failed to acquire read lock")))?;

        match blocks_guard.get(&block_id) {
            None => Err(RSHDFSError::BlockMapError(String::from("Block not found."))),
            Some(block) => Ok(block.clone()),
        }
    }

    pub fn modify_block_metadata<F>(&self, block_id: Uuid, modify: F) -> Result<(), RSHDFSError>
    where
        F: FnOnce(&mut BlockMetadata),
    {
        let mut blocks_guard = self
            .blocks
            .write()
            .map_err(|_| RSHDFSError::LockError(String::from("Failed to acquire write lock")))?;

        match blocks_guard.get_mut(&block_id) {
            None => Err(RSHDFSError::BlockMapError(String::from("Block not found"))),
            Some(block_metadata) => {
                modify(block_metadata);
                Ok(())
            }
        }
    }

    pub fn remove_block(&self, block_id: Uuid) -> Result<(), RSHDFSError> {
        let mut blocks_guard = self
            .blocks
            .write()
            .map_err(|_| RSHDFSError::LockError(String::from("Failed to acquire write lock")))?;
        match blocks_guard.remove(&block_id) {
            None => Err(RSHDFSError::BlockMapError(String::from("Block not found."))),
            Some(_) => Ok(()),
        }
    }
}
