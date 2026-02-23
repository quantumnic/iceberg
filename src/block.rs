use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// A content-addressable block identified by its SHA-256 hash.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Block {
    pub hash: BlockHash,
    pub data: Vec<u8>,
}

/// SHA-256 hash as hex string, used as the block's unique identifier.
pub type BlockHash = String;

impl Block {
    /// Create a new block from raw data; hash is computed automatically.
    pub fn new(data: Vec<u8>) -> Self {
        let hash = compute_hash(&data);
        Self { hash, data }
    }

    /// Verify the block's integrity.
    pub fn verify(&self) -> bool {
        compute_hash(&self.data) == self.hash
    }
}

/// Compute the SHA-256 hex digest of some data.
pub fn compute_hash(data: &[u8]) -> BlockHash {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_content_addressable() {
        let b1 = Block::new(b"hello".to_vec());
        let b2 = Block::new(b"hello".to_vec());
        assert_eq!(b1.hash, b2.hash);
        assert!(b1.verify());
    }

    #[test]
    fn different_data_different_hash() {
        let b1 = Block::new(b"hello".to_vec());
        let b2 = Block::new(b"world".to_vec());
        assert_ne!(b1.hash, b2.hash);
    }

    #[test]
    fn tampered_block_fails_verify() {
        let mut b = Block::new(b"original".to_vec());
        b.data = b"tampered".to_vec();
        assert!(!b.verify());
    }
}
