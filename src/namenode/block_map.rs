use crate::block::BlockMetadata;
use crate::error::RSHDFSError;

use async_trait::async_trait;
use std::collections::HashMap;

use tokio::sync::RwLock;
use uuid::Uuid;

/// Central data-structure used by the NameNode to track block locations.
#[async_trait]
pub trait BlockMapManager {
    async fn add_block(&self, block: BlockMetadata);
    async fn get_block(&self, block_id: Uuid) -> Result<BlockMetadata, RSHDFSError>;
    async fn remove_block(&self, block_id: Uuid) -> Result<(), RSHDFSError>;
}

#[derive(Default)]
pub struct BlockMap {
    // Maps a block ID to a Block and the DataNodes storing it.
    pub blocks: HashMap<Uuid, BlockMetadata>,
}

impl BlockMap {
    pub fn new() -> Self {
        BlockMap {
            blocks: HashMap::new(),
        }
    }

    pub async fn modify_block_metadata<F>(
        &mut self,
        block_id: Uuid,
        modify: F,
    ) -> Result<(), RSHDFSError>
    where
        F: FnOnce(&mut BlockMetadata),
    {
        match self.blocks.get_mut(&block_id) {
            None => Err(RSHDFSError::BlockMapError(String::from("Block not found"))),
            Some(block_metadata) => {
                modify(block_metadata);
                Ok(())
            }
        }
    }
}

#[async_trait]
impl BlockMapManager for RwLock<BlockMap> {
    /// Adds a block to the BlockMap.
    async fn add_block(&self, block: BlockMetadata) {
        self.write().await.blocks.insert(block.id, block);
    }

    /// Retrieves a block and its associated DataNodes.
    async fn get_block(&self, block_id: Uuid) -> Result<BlockMetadata, RSHDFSError> {
        match self.read().await.blocks.get(&block_id) {
            None => Err(RSHDFSError::BlockMapError(String::from("Block not found."))),
            Some(block) => Ok(block.clone()),
        }
    }

    async fn remove_block(&self, block_id: Uuid) -> Result<(), RSHDFSError> {
        match self.write().await.blocks.remove(&block_id) {
            None => Err(RSHDFSError::BlockMapError(String::from("Block not found."))),
            Some(_) => Ok(()),
        }
    }
}
