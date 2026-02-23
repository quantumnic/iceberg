use crate::block::{compute_hash, BlockHash};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A git-like commit object: immutable snapshot referencing a tree root.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Commit {
    /// Unique hash of this commit (covers all fields).
    pub id: BlockHash,
    /// Hash of the parent commit (`None` for the initial commit).
    pub parent: Option<BlockHash>,
    /// Root hash of the tree at this version.
    pub tree_root: BlockHash,
    /// When the commit was created.
    pub timestamp: DateTime<Utc>,
    /// Human-readable commit message.
    pub message: String,
}

impl Commit {
    /// Create a new commit. The `id` is computed from all other fields.
    pub fn new(parent: Option<BlockHash>, tree_root: BlockHash, message: String) -> Self {
        let timestamp = Utc::now();
        let id = Self::compute_id(&parent, &tree_root, &timestamp, &message);
        Self {
            id,
            parent,
            tree_root,
            timestamp,
            message,
        }
    }

    /// Create a commit with an explicit timestamp (for testing / determinism).
    pub fn with_timestamp(
        parent: Option<BlockHash>,
        tree_root: BlockHash,
        message: String,
        timestamp: DateTime<Utc>,
    ) -> Self {
        let id = Self::compute_id(&parent, &tree_root, &timestamp, &message);
        Self {
            id,
            parent,
            tree_root,
            timestamp,
            message,
        }
    }

    fn compute_id(
        parent: &Option<BlockHash>,
        tree_root: &BlockHash,
        timestamp: &DateTime<Utc>,
        message: &str,
    ) -> BlockHash {
        let payload = format!(
            "parent:{}\ntree:{}\ntime:{}\nmsg:{}",
            parent.as_deref().unwrap_or("none"),
            tree_root,
            timestamp.to_rfc3339(),
            message,
        );
        compute_hash(payload.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_has_unique_id() {
        let c1 = Commit::new(None, "abc".into(), "first".into());
        let c2 = Commit::new(Some(c1.id.clone()), "def".into(), "second".into());
        assert_ne!(c1.id, c2.id);
    }

    #[test]
    fn deterministic_with_same_inputs() {
        let ts = Utc::now();
        let c1 = Commit::with_timestamp(None, "root".into(), "msg".into(), ts);
        let c2 = Commit::with_timestamp(None, "root".into(), "msg".into(), ts);
        assert_eq!(c1.id, c2.id);
    }
}
