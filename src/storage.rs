use crate::block::{Block, BlockHash};
use crate::error::{IcebergError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Append-only, content-addressable block store.
///
/// Blocks are stored as individual JSON files keyed by their SHA-256 hash.
/// Duplicate writes are no-ops (content-addressable dedup).
pub struct BlockStore {
    dir: PathBuf,
}

/// The append-only log records every write in order, enabling replay and auditing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub sequence: u64,
    pub hash: BlockHash,
    pub timestamp: String,
}

impl BlockStore {
    /// Open or create a block store at the given directory.
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir.join("blocks"))?;
        fs::create_dir_all(dir.join("log"))?;
        Ok(Self {
            dir: dir.to_path_buf(),
        })
    }

    /// Store a block. Returns the hash. No-op if already present.
    pub fn put(&self, block: &Block) -> Result<BlockHash> {
        let path = self.block_path(&block.hash);
        if !path.exists() {
            let data = serde_json::to_vec(block)?;
            fs::write(&path, &data)?;
            self.append_log(&block.hash)?;
        }
        Ok(block.hash.clone())
    }

    /// Retrieve a block by hash.
    pub fn get(&self, hash: &str) -> Result<Block> {
        let path = self.block_path(hash);
        if !path.exists() {
            return Err(IcebergError::Corruption(format!(
                "block not found: {}",
                hash
            )));
        }
        let data = fs::read(&path)?;
        let block: Block = serde_json::from_slice(&data)?;
        if !block.verify() {
            return Err(IcebergError::Corruption(format!(
                "block integrity check failed: {}",
                hash
            )));
        }
        Ok(block)
    }

    /// Check if a block exists.
    pub fn contains(&self, hash: &str) -> bool {
        self.block_path(hash).exists()
    }

    /// Count stored blocks.
    pub fn block_count(&self) -> Result<usize> {
        Ok(fs::read_dir(self.dir.join("blocks"))?
            .filter_map(|e| e.ok())
            .count())
    }

    /// Return total bytes used by block files.
    pub fn disk_usage(&self) -> Result<u64> {
        let mut total = 0u64;
        for entry in fs::read_dir(self.dir.join("blocks"))? {
            let entry = entry?;
            total += entry.metadata()?.len();
        }
        Ok(total)
    }

    fn block_path(&self, hash: &str) -> PathBuf {
        // Use first 2 chars as directory prefix (like git)
        let prefix = &hash[..2.min(hash.len())];
        let dir = self.dir.join("blocks").join(prefix);
        let _ = fs::create_dir_all(&dir);
        dir.join(hash)
    }

    fn append_log(&self, hash: &BlockHash) -> Result<()> {
        let log_path = self.dir.join("log").join("append.jsonl");
        let seq = self.next_sequence()?;
        let entry = LogEntry {
            sequence: seq,
            hash: hash.clone(),
            timestamp: chrono::Utc::now().to_rfc3339(),
        };
        let mut line = serde_json::to_string(&entry)?;
        line.push('\n');
        use std::io::Write;
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)?;
        f.write_all(line.as_bytes())?;
        Ok(())
    }

    fn next_sequence(&self) -> Result<u64> {
        let log_path = self.dir.join("log").join("append.jsonl");
        if !log_path.exists() {
            return Ok(1);
        }
        let content = fs::read_to_string(&log_path)?;
        Ok(content.lines().count() as u64 + 1)
    }
}

/// In-memory block store for testing.
#[derive(Default)]
pub struct MemoryStore {
    blocks: HashMap<BlockHash, Block>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, block: &Block) -> BlockHash {
        self.blocks
            .entry(block.hash.clone())
            .or_insert_with(|| block.clone());
        block.hash.clone()
    }

    pub fn get(&self, hash: &str) -> Option<&Block> {
        self.blocks.get(hash)
    }

    pub fn contains(&self, hash: &str) -> bool {
        self.blocks.contains_key(hash)
    }

    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blockstore_put_get() {
        let tmp = tempfile::tempdir().unwrap();
        let store = BlockStore::open(tmp.path()).unwrap();

        let block = Block::new(b"test data".to_vec());
        let hash = store.put(&block).unwrap();
        assert_eq!(hash, block.hash);

        let retrieved = store.get(&hash).unwrap();
        assert_eq!(retrieved.data, b"test data");
    }

    #[test]
    fn blockstore_dedup() {
        let tmp = tempfile::tempdir().unwrap();
        let store = BlockStore::open(tmp.path()).unwrap();

        let block = Block::new(b"same data".to_vec());
        store.put(&block).unwrap();
        store.put(&block).unwrap();

        assert_eq!(store.block_count().unwrap(), 1);
    }

    #[test]
    fn memory_store_basics() {
        let mut store = MemoryStore::new();
        let b = Block::new(b"mem".to_vec());
        store.put(&b);
        assert!(store.contains(&b.hash));
        assert_eq!(store.len(), 1);
    }
}
