use crate::error::{IcebergError, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Write-Ahead Log entry types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WalEntry {
    /// Begin a transaction.
    Begin { tx_id: u64 },
    /// A key-value write operation within a transaction.
    Write {
        tx_id: u64,
        key: String,
        value: Vec<u8>,
    },
    /// A delete operation within a transaction.
    Delete { tx_id: u64, key: String },
    /// Commit the transaction (data is now durable).
    Commit { tx_id: u64, commit_id: String },
    /// Rollback the transaction.
    Rollback { tx_id: u64 },
}

/// Write-Ahead Log for crash safety.
///
/// Every mutation is first written to the WAL before being applied to the
/// main storage. On recovery, uncommitted transactions are rolled back and
/// committed but unapplied transactions are replayed.
pub struct Wal {
    path: PathBuf,
    next_tx: u64,
}

impl Wal {
    /// Open or create a WAL at the given directory.
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let path = dir.join("wal.jsonl");
        let next_tx = if path.exists() {
            Self::read_entries_from(&path)?
                .iter()
                .map(|e| match e {
                    WalEntry::Begin { tx_id }
                    | WalEntry::Write { tx_id, .. }
                    | WalEntry::Delete { tx_id, .. }
                    | WalEntry::Commit { tx_id, .. }
                    | WalEntry::Rollback { tx_id } => *tx_id,
                })
                .max()
                .unwrap_or(0)
                + 1
        } else {
            1
        };
        Ok(Self { path, next_tx })
    }

    /// Start a new transaction. Returns the transaction ID.
    pub fn begin(&mut self) -> Result<u64> {
        let tx_id = self.next_tx;
        self.next_tx += 1;
        self.append(&WalEntry::Begin { tx_id })?;
        Ok(tx_id)
    }

    /// Log a write operation.
    pub fn log_write(&mut self, tx_id: u64, key: String, value: Vec<u8>) -> Result<()> {
        self.append(&WalEntry::Write { tx_id, key, value })
    }

    /// Log a delete operation.
    pub fn log_delete(&mut self, tx_id: u64, key: String) -> Result<()> {
        self.append(&WalEntry::Delete { tx_id, key })
    }

    /// Mark a transaction as committed.
    pub fn commit(&mut self, tx_id: u64, commit_id: String) -> Result<()> {
        self.append(&WalEntry::Commit { tx_id, commit_id })?;
        // fsync to ensure durability
        let f = fs::OpenOptions::new().write(true).open(&self.path)?;
        f.sync_all()?;
        Ok(())
    }

    /// Mark a transaction as rolled back.
    pub fn rollback(&mut self, tx_id: u64) -> Result<()> {
        self.append(&WalEntry::Rollback { tx_id })
    }

    /// Read all entries from the WAL.
    pub fn entries(&self) -> Result<Vec<WalEntry>> {
        Self::read_entries_from(&self.path)
    }

    /// Recover: returns committed transaction IDs that may need replay,
    /// and uncommitted transaction IDs that should be ignored.
    pub fn recover(&self) -> Result<WalRecovery> {
        let entries = self.entries()?;
        let mut begun = std::collections::HashSet::new();
        let mut committed = std::collections::HashMap::new(); // tx_id → commit_id
        let mut rolled_back = std::collections::HashSet::new();

        for entry in &entries {
            match entry {
                WalEntry::Begin { tx_id } => {
                    begun.insert(*tx_id);
                }
                WalEntry::Commit { tx_id, commit_id } => {
                    committed.insert(*tx_id, commit_id.clone());
                }
                WalEntry::Rollback { tx_id } => {
                    rolled_back.insert(*tx_id);
                }
                _ => {}
            }
        }

        let uncommitted: Vec<u64> = begun
            .iter()
            .filter(|id| !committed.contains_key(id) && !rolled_back.contains(id))
            .copied()
            .collect();

        Ok(WalRecovery {
            committed,
            uncommitted,
            entries,
        })
    }

    /// Truncate the WAL (call after successful checkpoint).
    pub fn truncate(&mut self) -> Result<()> {
        fs::write(&self.path, "")?;
        Ok(())
    }

    /// Current WAL file size in bytes.
    pub fn size(&self) -> u64 {
        fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }

    fn append(&self, entry: &WalEntry) -> Result<()> {
        let mut line = serde_json::to_string(entry)?;
        line.push('\n');
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        f.write_all(line.as_bytes())?;
        Ok(())
    }

    fn read_entries_from(path: &Path) -> Result<Vec<WalEntry>> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let content = fs::read_to_string(path)?;
        let mut entries = Vec::new();
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let entry: WalEntry = serde_json::from_str(line)
                .map_err(|e| IcebergError::Corruption(format!("WAL parse error: {}", e)))?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

/// Result of WAL recovery analysis.
#[derive(Debug)]
pub struct WalRecovery {
    /// Transactions that were committed (tx_id → commit_id).
    pub committed: std::collections::HashMap<u64, String>,
    /// Transactions that were started but never committed or rolled back.
    pub uncommitted: Vec<u64>,
    /// All WAL entries.
    pub entries: Vec<WalEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_begin_commit() {
        let tmp = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(tmp.path()).unwrap();

        let tx = wal.begin().unwrap();
        assert_eq!(tx, 1);
        wal.log_write(tx, "key".into(), b"value".to_vec()).unwrap();
        wal.commit(tx, "commit_abc".into()).unwrap();

        let entries = wal.entries().unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn wal_recovery_committed() {
        let tmp = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(tmp.path()).unwrap();

        let tx = wal.begin().unwrap();
        wal.log_write(tx, "k".into(), b"v".to_vec()).unwrap();
        wal.commit(tx, "c1".into()).unwrap();

        let recovery = wal.recover().unwrap();
        assert!(recovery.uncommitted.is_empty());
        assert_eq!(recovery.committed.get(&tx), Some(&"c1".to_string()));
    }

    #[test]
    fn wal_recovery_uncommitted() {
        let tmp = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(tmp.path()).unwrap();

        let tx = wal.begin().unwrap();
        wal.log_write(tx, "k".into(), b"v".to_vec()).unwrap();
        // No commit!

        let recovery = wal.recover().unwrap();
        assert_eq!(recovery.uncommitted, vec![tx]);
        assert!(recovery.committed.is_empty());
    }

    #[test]
    fn wal_rollback() {
        let tmp = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(tmp.path()).unwrap();

        let tx = wal.begin().unwrap();
        wal.log_write(tx, "k".into(), b"v".to_vec()).unwrap();
        wal.rollback(tx).unwrap();

        let recovery = wal.recover().unwrap();
        assert!(recovery.uncommitted.is_empty());
        assert!(recovery.committed.is_empty());
    }

    #[test]
    fn wal_truncate() {
        let tmp = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(tmp.path()).unwrap();

        let tx = wal.begin().unwrap();
        wal.commit(tx, "c".into()).unwrap();
        assert!(wal.size() > 0);

        wal.truncate().unwrap();
        assert_eq!(wal.size(), 0);
        assert!(wal.entries().unwrap().is_empty());
    }

    #[test]
    fn wal_multiple_transactions() {
        let tmp = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(tmp.path()).unwrap();

        let tx1 = wal.begin().unwrap();
        let tx2 = wal.begin().unwrap();
        assert_ne!(tx1, tx2);

        wal.log_write(tx1, "a".into(), b"1".to_vec()).unwrap();
        wal.log_write(tx2, "b".into(), b"2".to_vec()).unwrap();
        wal.commit(tx1, "c1".into()).unwrap();
        // tx2 left uncommitted

        let recovery = wal.recover().unwrap();
        assert_eq!(recovery.committed.len(), 1);
        assert_eq!(recovery.uncommitted, vec![tx2]);
    }

    #[test]
    fn wal_reopen_continues_sequence() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut wal = Wal::open(tmp.path()).unwrap();
            let tx = wal.begin().unwrap();
            wal.commit(tx, "c".into()).unwrap();
        }
        // Reopen
        let mut wal = Wal::open(tmp.path()).unwrap();
        let tx = wal.begin().unwrap();
        assert!(tx > 1);
    }
}
