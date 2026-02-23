use crate::block::{compute_hash, BlockHash};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// An immutable sorted key-value tree stored as a content-addressable snapshot.
///
/// Each mutation produces a new `Tree` with a new root hash (copy-on-write semantics).
/// Internally uses a sorted BTreeMap serialized to JSON; the hash covers the entire state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Tree {
    pub root_hash: BlockHash,
    pub entries: BTreeMap<String, Vec<u8>>,
}

impl Tree {
    /// Create an empty tree.
    pub fn empty() -> Self {
        let entries = BTreeMap::new();
        let root_hash = Self::compute_root(&entries);
        Self { root_hash, entries }
    }

    /// Insert or update a key. Returns a new tree (immutable).
    pub fn insert(&self, key: String, value: Vec<u8>) -> Self {
        let mut entries = self.entries.clone();
        entries.insert(key, value);
        let root_hash = Self::compute_root(&entries);
        Self { root_hash, entries }
    }

    /// Delete a key. Returns a new tree (immutable).
    pub fn delete(&self, key: &str) -> Self {
        let mut entries = self.entries.clone();
        entries.remove(key);
        let root_hash = Self::compute_root(&entries);
        Self { root_hash, entries }
    }

    /// Get a value by key.
    pub fn get(&self, key: &str) -> Option<&Vec<u8>> {
        self.entries.get(key)
    }

    /// Check if key exists.
    pub fn contains_key(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the tree is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Range scan: returns entries where `start <= key < end`.
    pub fn range(&self, start: &str, end: &str) -> Vec<(&String, &Vec<u8>)> {
        use std::ops::Bound;
        self.entries
            .range::<String, _>((
                Bound::Included(&start.to_string()),
                Bound::Excluded(&end.to_string()),
            ))
            .collect()
    }

    /// Prefix scan: returns all entries whose key starts with `prefix`.
    pub fn scan_prefix(&self, prefix: &str) -> Vec<(&String, &Vec<u8>)> {
        self.entries
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .collect()
    }

    /// Compute diff between two trees. Returns (added, removed, modified) keys.
    pub fn diff(&self, other: &Tree) -> TreeDiff {
        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut modified = Vec::new();

        for (k, v) in &other.entries {
            match self.entries.get(k) {
                None => added.push(k.clone()),
                Some(old_v) if old_v != v => modified.push(k.clone()),
                _ => {}
            }
        }
        for k in self.entries.keys() {
            if !other.entries.contains_key(k) {
                removed.push(k.clone());
            }
        }

        TreeDiff {
            added,
            removed,
            modified,
        }
    }

    fn compute_root(entries: &BTreeMap<String, Vec<u8>>) -> BlockHash {
        let serialized = serde_json::to_vec(entries).unwrap_or_default();
        compute_hash(&serialized)
    }
}

/// Diff result between two tree versions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub modified: Vec<String>,
}

impl TreeDiff {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.modified.is_empty()
    }

    pub fn total_changes(&self) -> usize {
        self.added.len() + self.removed.len() + self.modified.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_tree() {
        let t = Tree::empty();
        assert!(t.is_empty());
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn insert_produces_new_tree() {
        let t1 = Tree::empty();
        let t2 = t1.insert("key".into(), b"val".to_vec());
        assert!(t1.is_empty()); // original unchanged
        assert_eq!(t2.len(), 1);
        assert_ne!(t1.root_hash, t2.root_hash);
    }

    #[test]
    fn get_and_delete() {
        let t = Tree::empty()
            .insert("a".into(), b"1".to_vec())
            .insert("b".into(), b"2".to_vec());
        assert_eq!(t.get("a"), Some(&b"1".to_vec()));
        let t2 = t.delete("a");
        assert!(!t2.contains_key("a"));
        assert!(t.contains_key("a")); // original untouched
    }

    #[test]
    fn range_and_prefix_scan() {
        let t = Tree::empty()
            .insert("user:1".into(), b"alice".to_vec())
            .insert("user:2".into(), b"bob".to_vec())
            .insert("user:3".into(), b"carol".to_vec())
            .insert("order:1".into(), b"o1".to_vec());

        let users = t.scan_prefix("user:");
        assert_eq!(users.len(), 3);

        let range = t.range("user:1", "user:3");
        assert_eq!(range.len(), 2); // user:1 and user:2
    }

    #[test]
    fn diff_trees() {
        let t1 = Tree::empty()
            .insert("a".into(), b"1".to_vec())
            .insert("b".into(), b"2".to_vec());
        let t2 = t1
            .delete("a")
            .insert("b".into(), b"changed".to_vec())
            .insert("c".into(), b"3".to_vec());

        let diff = t1.diff(&t2);
        assert_eq!(diff.added, vec!["c"]);
        assert_eq!(diff.removed, vec!["a"]);
        assert_eq!(diff.modified, vec!["b"]);
    }

    #[test]
    fn same_content_same_hash() {
        let t1 = Tree::empty()
            .insert("a".into(), b"1".to_vec())
            .insert("b".into(), b"2".to_vec());
        let t2 = Tree::empty()
            .insert("b".into(), b"2".to_vec())
            .insert("a".into(), b"1".to_vec());
        assert_eq!(t1.root_hash, t2.root_hash);
    }
}
