use crate::block::{compute_hash, BlockHash};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A tag is a named, immutable pointer to a specific commit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Tag {
    /// Unique id of this tag (hash of name + commit + timestamp).
    pub id: BlockHash,
    /// Human-readable tag name (e.g. "v1.0", "release-2024").
    pub name: String,
    /// The commit this tag points to.
    pub commit_id: BlockHash,
    /// Optional annotation message.
    pub message: Option<String>,
    /// When the tag was created.
    pub created_at: DateTime<Utc>,
}

impl Tag {
    /// Create a new tag pointing to a commit.
    pub fn new(name: String, commit_id: BlockHash, message: Option<String>) -> Self {
        let created_at = Utc::now();
        let payload = format!(
            "tag:{}\ncommit:{}\ntime:{}",
            name,
            commit_id,
            created_at.to_rfc3339()
        );
        let id = compute_hash(payload.as_bytes());
        Self {
            id,
            name,
            commit_id,
            message,
            created_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_creation() {
        let tag = Tag::new("v1.0".into(), "abc123".into(), Some("release".into()));
        assert_eq!(tag.name, "v1.0");
        assert_eq!(tag.commit_id, "abc123");
        assert_eq!(tag.message, Some("release".into()));
        assert!(!tag.id.is_empty());
    }

    #[test]
    fn tags_have_unique_ids() {
        let t1 = Tag::new("v1".into(), "abc".into(), None);
        let t2 = Tag::new("v2".into(), "abc".into(), None);
        assert_ne!(t1.id, t2.id);
    }
}
