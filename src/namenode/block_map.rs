use crate::block::BlockMetadata;
use crate::error::RSHDFSError;

use async_trait::async_trait;
use std::collections::HashMap;

use tokio::sync::RwLock;
use uuid::Uuid;

#[cfg(test)]
use mockall::automock;

/// Central data-structure used by the NameNode to track block locations.
#[cfg_attr(test, automock)]
#[async_trait]
pub trait BlockMapManager {
    async fn add_block(&self, block: BlockMetadata);
    async fn get_block(&self, block_id: Uuid) -> Result<BlockMetadata, RSHDFSError>;
    async fn remove_block(&self, block_id: Uuid) -> Result<(), RSHDFSError>;
}

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
}

#[cfg_attr(test, automock)]
#[async_trait]
impl BlockMapManager for BlockMap {
    /// Adds a block to the BlockMap.
    async fn add_block(&self, block: BlockMetadata) {
        let mut blocks = self.blocks.write().await;
        blocks.insert(block.id, block);
    }

    /// Retrieves a block and its associated DataNodes.
    async fn get_block(&self, block_id: Uuid) -> Result<BlockMetadata, RSHDFSError> {
        let blocks_guard = self.blocks.read().await;

        match blocks_guard.get(&block_id) {
            None => Err(RSHDFSError::BlockMapError(String::from("Block not found."))),
            Some(block) => Ok(block.clone()),
        }
    }

    async fn remove_block(&self, block_id: Uuid) -> Result<(), RSHDFSError> {
        let mut blocks_guard = self.blocks.write().await;
        match blocks_guard.remove(&block_id) {
            None => Err(RSHDFSError::BlockMapError(String::from("Block not found."))),
            Some(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::block::{BlockMetadata, BlockStatus};
    use crate::error::RSHDFSError;
    use crate::namenode::block_map::{BlockMapManager, MockBlockMap};
    use uuid::Uuid;

    #[tokio::test]
    async fn remove_block_expects_block_not_found() {
        let test_block_id = Uuid::new_v4();
        let mut mock_block_map = MockBlockMap::new();
        mock_block_map
            .expect_remove_block()
            .times(1)
            .returning(|_block_uuid| {
                Err(RSHDFSError::BlockMapError("Block not found.".to_string()))
            });

        let result = mock_block_map.remove_block(test_block_id).await;

        assert_eq!(
            result,
            Err(RSHDFSError::BlockMapError("Block not found.".to_string()))
        )
    }

    #[tokio::test]
    async fn remove_block_expects_success() {
        let test_block_id = Uuid::new_v4();
        let mut mock_block_map = MockBlockMap::new();
        mock_block_map
            .expect_remove_block()
            .times(1)
            .returning(|_block_uuid| Ok(()));

        let result = mock_block_map.remove_block(test_block_id).await;

        assert_eq!(result, Ok(()))
    }

    #[tokio::test]
    async fn get_block_expects_success() {
        let test_block_id = Uuid::try_parse("11111111-1111-1111-1111-111111111111").unwrap();
        let test_block = BlockMetadata {
            id: Uuid::try_parse("11111111-1111-1111-1111-111111111111").unwrap(),
            seq: 0,
            status: BlockStatus::Waiting,
            size: 100,
            datanodes: vec![],
        };
        let mut mock_block_map = MockBlockMap::new();

        mock_block_map
            .expect_get_block()
            .times(1)
            .returning(|_uuid| {
                Ok(BlockMetadata {
                    id: Uuid::try_parse("11111111-1111-1111-1111-111111111111").unwrap(),
                    seq: 0,
                    status: BlockStatus::Waiting,
                    size: 100,
                    datanodes: vec![],
                })
            });

        let result = mock_block_map.get_block(test_block_id).await;
        assert_eq!(result, Ok(test_block));
    }

    #[tokio::test]
    async fn get_block_expects_block_not_found() {
        let test_block_id = Uuid::try_parse("11111111-1111-1111-1111-111111111111").unwrap();
        let mut mock_block_map = MockBlockMap::new();

        mock_block_map
            .expect_get_block()
            .times(1)
            .returning(|_block_uuid| {
                Err(RSHDFSError::BlockMapError("Block not found.".to_string()))
            });
        let result = mock_block_map.get_block(test_block_id).await;
        assert_eq!(
            result,
            Err(RSHDFSError::BlockMapError("Block not found.".to_string()))
        );
    }
}
