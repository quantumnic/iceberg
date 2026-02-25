use crate::block::Block;
use crate::bloom::BloomFilter;
use crate::commit::Commit;
use crate::compaction::{find_removable_commits, CompactionPolicy, CompactionResult};
use crate::error::{IcebergError, Result};
use crate::index::IndexManager;
use crate::storage::BlockStore;
use crate::tag::Tag;
use crate::tree::{Tree, TreeDiff};
use crate::wal::Wal;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const REFS_DIR: &str = "refs";
const TREES_DIR: &str = "trees";
const COMMITS_DIR: &str = "commits";
const TAGS_DIR: &str = "tags";
const BLOOM_DIR: &str = "bloom";
const INDEXES_FILE: &str = "indexes.json";

/// The main database: versioned, branching, immutable key-value store.
pub struct Database {
    root: PathBuf,
    store: BlockStore,
    wal: Mutex<Wal>,
    bloom: Mutex<BloomFilter>,
    indexes: Mutex<IndexManager>,
}

/// Persistent refs: branches and current HEAD.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Refs {
    /// Maps branch name → commit id
    branches: HashMap<String, String>,
    /// Current branch name
    head: String,
}

impl Database {
    /// Open or create a database at the given path.
    pub fn open(path: &Path) -> Result<Self> {
        fs::create_dir_all(path)?;
        let store = BlockStore::open(&path.join("store"))?;
        fs::create_dir_all(path.join(TREES_DIR))?;
        fs::create_dir_all(path.join(COMMITS_DIR))?;
        fs::create_dir_all(path.join(REFS_DIR))?;
        fs::create_dir_all(path.join(TAGS_DIR))?;
        fs::create_dir_all(path.join(BLOOM_DIR))?;
        let wal = Wal::open(&path.join("wal"))?;
        let bloom = Self::load_bloom_from(path);
        let indexes = Self::load_indexes_from(path);
        let db = Self {
            root: path.to_path_buf(),
            store,
            wal: Mutex::new(wal),
            bloom: Mutex::new(bloom),
            indexes: Mutex::new(indexes),
        };
        db.recover_wal()?;
        Ok(db)
    }

    /// Initialize a new database (creates the "main" branch).
    pub fn init(path: &Path) -> Result<Self> {
        let db = Self::open(path)?;
        if !db.refs_path().exists() {
            let refs = Refs {
                branches: HashMap::new(),
                head: "main".into(),
            };
            db.save_refs(&refs)?;
        }
        Ok(db)
    }

    /// Recover from WAL after crash.
    fn recover_wal(&self) -> Result<()> {
        let mut wal = self.wal.lock().unwrap();
        let recovery = wal.recover()?;
        if !recovery.uncommitted.is_empty() {
            // Uncommitted transactions are simply ignored (rolled back).
            // The WAL is truncated after recovery.
        }
        wal.truncate()?;
        Ok(())
    }

    fn load_bloom_from(path: &Path) -> BloomFilter {
        let bloom_path = path.join(BLOOM_DIR).join("keys.json");
        if bloom_path.exists() {
            if let Ok(data) = fs::read(&bloom_path) {
                if let Ok(bf) = serde_json::from_slice(&data) {
                    return bf;
                }
            }
        }
        BloomFilter::new(10000, 0.01)
    }

    fn save_bloom(&self) -> Result<()> {
        let bloom = self.bloom.lock().unwrap();
        let path = self.root.join(BLOOM_DIR).join("keys.json");
        let data = serde_json::to_vec(&*bloom)?;
        fs::write(path, data)?;
        Ok(())
    }

    fn load_indexes_from(path: &Path) -> IndexManager {
        let idx_path = path.join(INDEXES_FILE);
        if idx_path.exists() {
            if let Ok(data) = fs::read(&idx_path) {
                if let Ok(mgr) = serde_json::from_slice(&data) {
                    return mgr;
                }
            }
        }
        IndexManager::new()
    }

    fn save_indexes(&self) -> Result<()> {
        let indexes = self.indexes.lock().unwrap();
        let path = self.root.join(INDEXES_FILE);
        let data = serde_json::to_vec_pretty(&*indexes)?;
        fs::write(path, data)?;
        Ok(())
    }

    // ── Key-Value API ─────────────────────────────────────────

    /// Get a value by key from the current branch HEAD.
    /// Uses bloom filter for fast negative lookups.
    pub fn get(&self, key: &str) -> Result<Vec<u8>> {
        // Fast path: bloom filter says definitely not present
        {
            let bloom = self.bloom.lock().unwrap();
            if bloom.count() > 0 && !bloom.may_contain(key.as_bytes()) {
                return Err(IcebergError::KeyNotFound(key.into()));
            }
        }
        let tree = self.current_tree()?;
        tree.get(key)
            .cloned()
            .ok_or_else(|| IcebergError::KeyNotFound(key.into()))
    }

    /// Put a key-value pair; creates a new commit on the current branch.
    /// Writes are WAL-protected for crash safety.
    pub fn put(&self, key: &str, value: Vec<u8>, message: Option<&str>) -> Result<Commit> {
        // WAL: begin transaction
        let tx_id = {
            let mut wal = self.wal.lock().unwrap();
            let tx = wal.begin()?;
            wal.log_write(tx, key.into(), value.clone())?;
            tx
        };

        let tree = self.current_tree().unwrap_or_else(|_| Tree::empty());
        let new_tree = tree.insert(key.into(), value.clone());
        let msg = message
            .map(String::from)
            .unwrap_or_else(|| format!("put {}", key));
        let commit = self.commit_tree(&new_tree, &msg)?;

        // WAL: commit transaction
        {
            let mut wal = self.wal.lock().unwrap();
            wal.commit(tx_id, commit.id.clone())?;
        }

        // Update bloom filter
        {
            let mut bloom = self.bloom.lock().unwrap();
            bloom.insert(key.as_bytes());
        }
        self.save_bloom()?;

        // Update secondary indexes
        {
            let mut indexes = self.indexes.lock().unwrap();
            indexes.on_put(key, &value);
        }
        self.save_indexes()?;

        Ok(commit)
    }

    /// Delete a key; creates a new commit.
    /// Writes are WAL-protected for crash safety.
    pub fn delete(&self, key: &str, message: Option<&str>) -> Result<Commit> {
        let tree = self.current_tree()?;
        if !tree.contains_key(key) {
            return Err(IcebergError::KeyNotFound(key.into()));
        }

        // WAL: begin transaction
        let tx_id = {
            let mut wal = self.wal.lock().unwrap();
            let tx = wal.begin()?;
            wal.log_delete(tx, key.into())?;
            tx
        };

        let new_tree = tree.delete(key);
        let msg = message
            .map(String::from)
            .unwrap_or_else(|| format!("delete {}", key));
        let commit = self.commit_tree(&new_tree, &msg)?;

        // WAL: commit
        {
            let mut wal = self.wal.lock().unwrap();
            wal.commit(tx_id, commit.id.clone())?;
        }

        // Update secondary indexes
        {
            let mut indexes = self.indexes.lock().unwrap();
            indexes.on_delete(key);
        }
        self.save_indexes()?;

        Ok(commit)
    }

    /// Scan keys by prefix.
    pub fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let tree = self.current_tree()?;
        Ok(tree
            .scan_prefix(prefix)
            .into_iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    /// Range scan.
    pub fn range(&self, start: &str, end: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let tree = self.current_tree()?;
        Ok(tree
            .range(start, end)
            .into_iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    // ── Version History ───────────────────────────────────────

    /// Get the current branch's HEAD commit.
    pub fn head_commit(&self) -> Result<Commit> {
        let refs = self.load_refs()?;
        let commit_id = refs
            .branches
            .get(&refs.head)
            .ok_or(IcebergError::EmptyDatabase)?;
        self.load_commit(commit_id)
    }

    /// Get the full commit log for the current branch (newest first).
    pub fn log(&self) -> Result<Vec<Commit>> {
        let mut commits = Vec::new();
        let head = match self.head_commit() {
            Ok(c) => c,
            Err(IcebergError::EmptyDatabase) => return Ok(commits),
            Err(e) => return Err(e),
        };
        let mut current = Some(head);
        while let Some(commit) = current {
            let parent_id = commit.parent.clone();
            commits.push(commit);
            current = match parent_id {
                Some(id) => Some(self.load_commit(&id)?),
                None => None,
            };
        }
        Ok(commits)
    }

    /// Get a tree at a specific commit.
    pub fn tree_at(&self, commit_id: &str) -> Result<Tree> {
        let commit = self.load_commit(commit_id)?;
        self.load_tree(&commit.tree_root)
    }

    /// Get a value at a specific version.
    pub fn get_at(&self, key: &str, commit_id: &str) -> Result<Vec<u8>> {
        let tree = self.tree_at(commit_id)?;
        tree.get(key)
            .cloned()
            .ok_or_else(|| IcebergError::KeyNotFound(key.into()))
    }

    /// Diff between two commits.
    pub fn diff(&self, commit_a: &str, commit_b: &str) -> Result<TreeDiff> {
        let tree_a = self.tree_at(commit_a)?;
        let tree_b = self.tree_at(commit_b)?;
        Ok(tree_a.diff(&tree_b))
    }

    // ── Branching ─────────────────────────────────────────────

    /// Get the current branch name.
    pub fn current_branch(&self) -> Result<String> {
        Ok(self.load_refs()?.head)
    }

    /// List all branches.
    pub fn branches(&self) -> Result<Vec<String>> {
        let refs = self.load_refs()?;
        let mut names: Vec<_> = refs.branches.keys().cloned().collect();
        names.sort();
        // Include head branch even if no commits
        if !names.contains(&refs.head) {
            names.push(refs.head);
            names.sort();
        }
        Ok(names)
    }

    /// Create a new branch from the current HEAD.
    pub fn create_branch(&self, name: &str) -> Result<()> {
        let mut refs = self.load_refs()?;
        if refs.branches.contains_key(name) {
            return Err(IcebergError::BranchExists(name.into()));
        }
        if let Some(head_id) = refs.branches.get(&refs.head).cloned() {
            refs.branches.insert(name.into(), head_id);
        }
        // If no commits yet, branch will be created on first commit
        self.save_refs(&refs)
    }

    /// Switch to a branch.
    pub fn checkout(&self, name: &str) -> Result<()> {
        let mut refs = self.load_refs()?;
        // Allow checkout even if branch has no commits yet
        let exists = refs.branches.contains_key(name)
            || refs.head == name
            || self
                .branches()
                .map(|b| b.contains(&name.to_string()))
                .unwrap_or(false);
        if !exists {
            return Err(IcebergError::BranchNotFound(name.into()));
        }
        refs.head = name.into();
        self.save_refs(&refs)
    }

    /// Delete a branch (cannot delete current branch).
    pub fn delete_branch(&self, name: &str) -> Result<()> {
        let mut refs = self.load_refs()?;
        if refs.head == name {
            return Err(IcebergError::Corruption(
                "cannot delete current branch".into(),
            ));
        }
        if refs.branches.remove(name).is_none() {
            return Err(IcebergError::BranchNotFound(name.into()));
        }
        self.save_refs(&refs)
    }

    /// Merge another branch into the current branch (fast-forward or snapshot merge).
    pub fn merge(&self, source_branch: &str, message: Option<&str>) -> Result<Commit> {
        let refs = self.load_refs()?;
        let source_id = refs
            .branches
            .get(source_branch)
            .ok_or_else(|| IcebergError::BranchNotFound(source_branch.into()))?
            .clone();

        let source_tree = self
            .load_commit(&source_id)
            .and_then(|c| self.load_tree(&c.tree_root))?;
        let current_tree = self.current_tree().unwrap_or_else(|_| Tree::empty());

        // Simple merge: apply all entries from source on top of current
        let mut merged = current_tree.entries.clone();
        for (k, v) in &source_tree.entries {
            merged.insert(k.clone(), v.clone());
        }

        let merged_tree = Tree {
            root_hash: {
                let serialized = serde_json::to_vec(&merged).unwrap_or_default();
                crate::block::compute_hash(&serialized)
            },
            entries: merged,
        };

        let msg = message
            .map(String::from)
            .unwrap_or_else(|| format!("merge branch '{}'", source_branch));
        self.commit_tree(&merged_tree, &msg)
    }

    // ── Tags ──────────────────────────────────────────────────

    /// Create a tag pointing to a specific commit (or current HEAD).
    pub fn create_tag(
        &self,
        name: &str,
        commit_id: Option<&str>,
        message: Option<&str>,
    ) -> Result<Tag> {
        // Check if tag name already exists
        if self.load_tag_by_name(name)?.is_some() {
            return Err(IcebergError::Corruption(format!(
                "tag already exists: {}",
                name
            )));
        }
        let cid = match commit_id {
            Some(id) => {
                // Verify commit exists
                self.load_commit(id)?;
                id.to_string()
            }
            None => self.head_commit()?.id,
        };
        let tag = Tag::new(name.into(), cid, message.map(String::from));
        self.save_tag(&tag)?;
        Ok(tag)
    }

    /// List all tags.
    pub fn tags(&self) -> Result<Vec<Tag>> {
        let dir = self.root.join(TAGS_DIR);
        let mut tags = Vec::new();
        if dir.exists() {
            for entry in fs::read_dir(&dir)? {
                let entry = entry?;
                let data = fs::read(entry.path())?;
                let tag: Tag = serde_json::from_slice(&data)?;
                tags.push(tag);
            }
        }
        tags.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(tags)
    }

    /// Get a tag by name.
    pub fn get_tag(&self, name: &str) -> Result<Tag> {
        self.load_tag_by_name(name)?
            .ok_or_else(|| IcebergError::Corruption(format!("tag not found: {}", name)))
    }

    /// Delete a tag by name.
    pub fn delete_tag(&self, name: &str) -> Result<()> {
        let tag = self.get_tag(name)?;
        let path = self.root.join(TAGS_DIR).join(&tag.id);
        fs::remove_file(path)?;
        Ok(())
    }

    // ── Cherry-pick ───────────────────────────────────────────

    /// Cherry-pick a commit onto the current branch.
    /// Applies the diff introduced by the given commit.
    pub fn cherry_pick(&self, commit_id: &str, message: Option<&str>) -> Result<Commit> {
        let commit = self.load_commit(commit_id)?;
        let commit_tree = self.load_tree(&commit.tree_root)?;

        // Get the parent tree (empty if no parent)
        let parent_tree = match &commit.parent {
            Some(pid) => {
                let pc = self.load_commit(pid)?;
                self.load_tree(&pc.tree_root)?
            }
            None => Tree::empty(),
        };

        // Compute the diff introduced by this commit
        let diff = parent_tree.diff(&commit_tree);

        // Apply the diff to current tree
        let mut current = self.current_tree().unwrap_or_else(|_| Tree::empty());
        for key in &diff.added {
            if let Some(val) = commit_tree.get(key) {
                current = current.insert(key.clone(), val.clone());
            }
        }
        for key in &diff.modified {
            if let Some(val) = commit_tree.get(key) {
                current = current.insert(key.clone(), val.clone());
            }
        }
        for key in &diff.removed {
            if current.contains_key(key) {
                current = current.delete(key);
            }
        }

        let msg = message
            .map(String::from)
            .unwrap_or_else(|| format!("cherry-pick {}", &commit_id[..8.min(commit_id.len())]));
        self.commit_tree(&current, &msg)
    }

    // ── Rebase ─────────────────────────────────────────────────

    /// Rebase the current branch onto another branch.
    /// Takes all commits unique to the current branch and replays them
    /// on top of the target branch's HEAD.
    pub fn rebase(&self, onto_branch: &str) -> Result<Vec<Commit>> {
        let refs = self.load_refs()?;
        let current_branch = refs.head.clone();

        if current_branch == onto_branch {
            return Err(IcebergError::Corruption(
                "cannot rebase a branch onto itself".into(),
            ));
        }

        let onto_id = refs
            .branches
            .get(onto_branch)
            .ok_or_else(|| IcebergError::BranchNotFound(onto_branch.into()))?
            .clone();

        // Collect commits on the target branch (to find the fork point)
        let onto_ancestors: HashSet<String> = {
            let mut ancestors = HashSet::new();
            let mut current_id = Some(onto_id.clone());
            while let Some(id) = current_id {
                if !ancestors.insert(id.clone()) {
                    break;
                }
                current_id = self.load_commit(&id).ok().and_then(|c| c.parent);
            }
            ancestors
        };

        // Collect commits unique to the current branch (stop at fork point)
        let current_log = self.log()?;
        let mut unique_commits: Vec<Commit> = Vec::new();
        for commit in &current_log {
            if onto_ancestors.contains(&commit.id) {
                break;
            }
            unique_commits.push(commit.clone());
        }
        unique_commits.reverse(); // oldest first for replay

        if unique_commits.is_empty() {
            return Ok(Vec::new());
        }

        // Switch to onto_branch's state as our new base
        let mut current_tree = self
            .load_commit(&onto_id)
            .and_then(|c| self.load_tree(&c.tree_root))?;
        let mut parent_id = Some(onto_id);
        let mut new_commits = Vec::new();

        // Replay each unique commit
        for old_commit in &unique_commits {
            let old_tree = self.load_tree(&old_commit.tree_root)?;
            let old_parent_tree = match &old_commit.parent {
                Some(pid) => self
                    .load_commit(pid)
                    .and_then(|c| self.load_tree(&c.tree_root))
                    .unwrap_or_else(|_| Tree::empty()),
                None => Tree::empty(),
            };

            // Compute the diff this commit introduced
            let diff = old_parent_tree.diff(&old_tree);

            // Apply the diff to current_tree
            for key in &diff.added {
                if let Some(val) = old_tree.get(key) {
                    current_tree = current_tree.insert(key.clone(), val.clone());
                }
            }
            for key in &diff.modified {
                if let Some(val) = old_tree.get(key) {
                    current_tree = current_tree.insert(key.clone(), val.clone());
                }
            }
            for key in &diff.removed {
                if current_tree.contains_key(key) {
                    current_tree = current_tree.delete(key);
                }
            }

            // Create a new commit with the rebased tree
            self.save_tree(&current_tree)?;
            for v in current_tree.entries.values() {
                let block = Block::new(v.clone());
                self.store.put(&block)?;
            }
            let new_commit = Commit::new(
                parent_id,
                current_tree.root_hash.clone(),
                old_commit.message.clone(),
            );
            self.save_commit(&new_commit)?;
            parent_id = Some(new_commit.id.clone());
            new_commits.push(new_commit);
        }

        // Update the current branch ref to point to the last new commit
        if let Some(last) = new_commits.last() {
            let mut refs = self.load_refs()?;
            refs.branches.insert(current_branch, last.id.clone());
            self.save_refs(&refs)?;
        }

        Ok(new_commits)
    }

    // ── Secondary Indexes ─────────────────────────────────────

    /// Create a secondary index on a JSON field.
    pub fn create_index(&self, name: &str, field_path: &str) -> Result<()> {
        {
            let mut indexes = self.indexes.lock().unwrap();
            indexes.create_index(name, field_path)?;

            // Rebuild from current tree
            if let Ok(tree) = self.current_tree() {
                let entries: Vec<_> = tree
                    .entries
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                indexes.rebuild_all(&entries);
            }
        }
        self.save_indexes()
    }

    /// Drop a secondary index.
    pub fn drop_index(&self, name: &str) -> Result<()> {
        {
            let mut indexes = self.indexes.lock().unwrap();
            indexes.drop_index(name)?;
        }
        self.save_indexes()
    }

    /// Query a secondary index by exact value. Returns matching primary keys.
    pub fn query_index(&self, index_name: &str, value: &str) -> Result<Vec<String>> {
        let indexes = self.indexes.lock().unwrap();
        indexes.query(index_name, value)
    }

    /// Query a secondary index by prefix. Returns matching primary keys.
    pub fn query_index_prefix(&self, index_name: &str, prefix: &str) -> Result<Vec<String>> {
        let indexes = self.indexes.lock().unwrap();
        indexes.query_prefix(index_name, prefix)
    }

    /// List all secondary indexes.
    pub fn list_indexes(&self) -> Vec<String> {
        let indexes = self.indexes.lock().unwrap();
        indexes.list_indexes()
    }

    // ── Bloom Filter ──────────────────────────────────────────

    /// Rebuild the bloom filter from the current tree.
    pub fn rebuild_bloom(&self) -> Result<()> {
        let tree = self.current_tree().unwrap_or_else(|_| Tree::empty());
        let mut bloom = BloomFilter::new(tree.len().max(1000), 0.01);
        for key in tree.entries.keys() {
            bloom.insert(key.as_bytes());
        }
        *self.bloom.lock().unwrap() = bloom;
        self.save_bloom()
    }

    /// Get bloom filter stats.
    pub fn bloom_stats(&self) -> (usize, usize, f64) {
        let bloom = self.bloom.lock().unwrap();
        (bloom.count(), bloom.num_bits(), bloom.estimated_fp_rate())
    }

    // ── Compaction ────────────────────────────────────────────

    /// Run compaction with the given policy on the current branch.
    /// Removes old commits and unreachable trees/blocks.
    pub fn compact(&self, policy: &CompactionPolicy) -> Result<CompactionResult> {
        let now = chrono::Utc::now();
        let log = self.log()?;
        let commits_with_ts: Vec<_> = log.iter().map(|c| (c.id.clone(), c.timestamp)).collect();

        let removable = find_removable_commits(&commits_with_ts, policy, now);
        if removable.is_empty() {
            return Ok(CompactionResult::default());
        }

        // Collect all reachable tree roots and block hashes from commits we're keeping
        let keep_commit_ids: HashSet<_> = log
            .iter()
            .map(|c| c.id.clone())
            .filter(|id| !removable.contains(id))
            .collect();

        // Also collect from all branches (not just current)
        let refs = self.load_refs()?;
        let mut all_reachable_commits = HashSet::new();
        for cid in refs.branches.values() {
            let mut current_id = Some(cid.clone());
            while let Some(id) = current_id {
                if !all_reachable_commits.insert(id.clone()) {
                    break; // already visited
                }
                if let Ok(c) = self.load_commit(&id) {
                    current_id = c.parent;
                } else {
                    break;
                }
            }
        }

        let mut reachable_trees = HashSet::new();
        for cid in &all_reachable_commits {
            if removable.contains(cid) && !keep_commit_ids.contains(cid) {
                continue;
            }
            if let Ok(c) = self.load_commit(cid) {
                reachable_trees.insert(c.tree_root.clone());
            }
        }

        let mut result = CompactionResult::default();

        // Remove commits
        for cid in &removable {
            // Only remove if not reachable from other branches
            if all_reachable_commits.contains(cid) && keep_commit_ids.contains(cid) {
                continue;
            }
            let path = self.root.join(COMMITS_DIR).join(cid);
            if path.exists() {
                // Rewrite parent pointer of child commit if needed
                fs::remove_file(&path)?;
                result.commits_removed += 1;
            }
        }

        // If we removed commits, fix the chain: find the oldest kept commit
        // and set its parent to None
        if result.commits_removed > 0 {
            let kept_commits: Vec<_> = log.iter().filter(|c| !removable.contains(&c.id)).collect();
            if let Some(oldest_kept) = kept_commits.last() {
                if let Some(ref parent_id) = oldest_kept.parent {
                    let parent_path = self.root.join(COMMITS_DIR).join(parent_id);
                    if !parent_path.exists() {
                        // Rewrite this commit with parent = None
                        let mut fixed = (*oldest_kept).clone();
                        fixed.parent = None;
                        self.save_commit(&fixed)?;
                    }
                }
            }
        }

        // Clean up unreachable trees
        let trees_dir = self.root.join(TREES_DIR);
        if trees_dir.exists() {
            for entry in fs::read_dir(&trees_dir)? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().to_string();
                if !reachable_trees.contains(&name) {
                    let size = entry.metadata()?.len();
                    fs::remove_file(entry.path())?;
                    result.trees_removed += 1;
                    result.bytes_reclaimed += size;
                }
            }
        }

        Ok(result)
    }

    // ── Stats ─────────────────────────────────────────────────

    /// Database statistics.
    pub fn stats(&self) -> Result<DbStats> {
        let tree = self.current_tree().unwrap_or_else(|_| Tree::empty());
        let commits = self.log()?;
        let branches = self.branches()?;
        let (bloom_items, bloom_bits, bloom_fp) = self.bloom_stats();
        let index_count = self.list_indexes().len();
        let wal_size = self.wal.lock().unwrap().size();
        Ok(DbStats {
            key_count: tree.len(),
            commit_count: commits.len(),
            branch_count: branches.len(),
            block_count: self.store.block_count()?,
            disk_usage: self.store.disk_usage()?,
            bloom_items,
            bloom_bits,
            bloom_fp_rate: bloom_fp,
            index_count,
            wal_size,
        })
    }

    // ── Internal ──────────────────────────────────────────────

    fn current_tree(&self) -> Result<Tree> {
        let commit = self.head_commit()?;
        self.load_tree(&commit.tree_root)
    }

    fn commit_tree(&self, tree: &Tree, message: &str) -> Result<Commit> {
        // Save tree
        self.save_tree(tree)?;

        // Save data blocks
        for v in tree.entries.values() {
            let block = Block::new(v.clone());
            self.store.put(&block)?;
        }

        // Create commit
        let parent = self.head_commit().ok().map(|c| c.id);
        let commit = Commit::new(parent, tree.root_hash.clone(), message.into());
        self.save_commit(&commit)?;

        // Update branch ref
        let mut refs = self.load_refs()?;
        refs.branches.insert(refs.head.clone(), commit.id.clone());
        self.save_refs(&refs)?;

        Ok(commit)
    }

    fn save_tree(&self, tree: &Tree) -> Result<()> {
        let path = self.root.join(TREES_DIR).join(&tree.root_hash);
        let data = serde_json::to_vec_pretty(tree)?;
        fs::write(path, data)?;
        Ok(())
    }

    fn load_tree(&self, root_hash: &str) -> Result<Tree> {
        let path = self.root.join(TREES_DIR).join(root_hash);
        if !path.exists() {
            return Err(IcebergError::Corruption(format!(
                "tree not found: {}",
                root_hash
            )));
        }
        let data = fs::read(path)?;
        Ok(serde_json::from_slice(&data)?)
    }

    fn save_commit(&self, commit: &Commit) -> Result<()> {
        let path = self.root.join(COMMITS_DIR).join(&commit.id);
        let data = serde_json::to_vec_pretty(commit)?;
        fs::write(path, data)?;
        Ok(())
    }

    fn load_commit(&self, id: &str) -> Result<Commit> {
        let path = self.root.join(COMMITS_DIR).join(id);
        if !path.exists() {
            return Err(IcebergError::CommitNotFound(id.into()));
        }
        let data = fs::read(path)?;
        Ok(serde_json::from_slice(&data)?)
    }

    fn refs_path(&self) -> PathBuf {
        self.root.join(REFS_DIR).join("refs.json")
    }

    fn load_refs(&self) -> Result<Refs> {
        let path = self.refs_path();
        if !path.exists() {
            return Ok(Refs {
                branches: HashMap::new(),
                head: "main".into(),
            });
        }
        let data = fs::read(path)?;
        Ok(serde_json::from_slice(&data)?)
    }

    fn save_refs(&self, refs: &Refs) -> Result<()> {
        let data = serde_json::to_vec_pretty(refs)?;
        fs::write(self.refs_path(), data)?;
        Ok(())
    }

    fn save_tag(&self, tag: &Tag) -> Result<()> {
        let path = self.root.join(TAGS_DIR).join(&tag.id);
        let data = serde_json::to_vec_pretty(tag)?;
        fs::write(path, data)?;
        Ok(())
    }

    fn load_tag_by_name(&self, name: &str) -> Result<Option<Tag>> {
        let dir = self.root.join(TAGS_DIR);
        if !dir.exists() {
            return Ok(None);
        }
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let data = fs::read(entry.path())?;
            let tag: Tag = serde_json::from_slice(&data)?;
            if tag.name == name {
                return Ok(Some(tag));
            }
        }
        Ok(None)
    }
}

/// Database statistics.
#[derive(Debug, Clone)]
pub struct DbStats {
    pub key_count: usize,
    pub commit_count: usize,
    pub branch_count: usize,
    pub block_count: usize,
    pub disk_usage: u64,
    pub bloom_items: usize,
    pub bloom_bits: usize,
    pub bloom_fp_rate: f64,
    pub index_count: usize,
    pub wal_size: u64,
}

impl std::fmt::Display for DbStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Keys:       {}", self.key_count)?;
        writeln!(f, "Commits:    {}", self.commit_count)?;
        writeln!(f, "Branches:   {}", self.branch_count)?;
        writeln!(f, "Blocks:     {}", self.block_count)?;
        writeln!(f, "Disk:       {} bytes", self.disk_usage)?;
        writeln!(
            f,
            "Bloom:      {} items, {} bits, {:.4}% FP",
            self.bloom_items,
            self.bloom_bits,
            self.bloom_fp_rate * 100.0
        )?;
        writeln!(f, "Indexes:    {}", self.index_count)?;
        writeln!(f, "WAL size:   {} bytes", self.wal_size)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> (tempfile::TempDir, Database) {
        let tmp = tempfile::tempdir().unwrap();
        let db = Database::init(tmp.path()).unwrap();
        (tmp, db)
    }

    #[test]
    fn put_and_get() {
        let (_tmp, db) = test_db();
        db.put("name", b"iceberg".to_vec(), None).unwrap();
        assert_eq!(db.get("name").unwrap(), b"iceberg");
    }

    #[test]
    fn key_not_found() {
        let (_tmp, db) = test_db();
        assert!(db.get("missing").is_err());
    }

    #[test]
    fn delete_key() {
        let (_tmp, db) = test_db();
        db.put("x", b"1".to_vec(), None).unwrap();
        db.delete("x", None).unwrap();
        assert!(db.get("x").is_err());
    }

    #[test]
    fn version_history() {
        let (_tmp, db) = test_db();
        db.put("a", b"1".to_vec(), Some("first")).unwrap();
        db.put("b", b"2".to_vec(), Some("second")).unwrap();

        let log = db.log().unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].message, "second");
        assert_eq!(log[1].message, "first");
    }

    #[test]
    fn time_travel() {
        let (_tmp, db) = test_db();
        let c1 = db.put("val", b"old".to_vec(), Some("v1")).unwrap();
        db.put("val", b"new".to_vec(), Some("v2")).unwrap();

        // Current version
        assert_eq!(db.get("val").unwrap(), b"new");
        // Old version
        assert_eq!(db.get_at("val", &c1.id).unwrap(), b"old");
    }

    #[test]
    fn branching() {
        let (_tmp, db) = test_db();
        db.put("shared", b"data".to_vec(), None).unwrap();

        db.create_branch("feature").unwrap();
        db.checkout("feature").unwrap();
        db.put("feature_key", b"feature_val".to_vec(), None)
            .unwrap();

        db.checkout("main").unwrap();
        assert!(db.get("feature_key").is_err()); // not on main
        assert_eq!(db.get("shared").unwrap(), b"data"); // shared data still there
    }

    #[test]
    fn merge_branches() {
        let (_tmp, db) = test_db();
        db.put("base", b"val".to_vec(), None).unwrap();

        db.create_branch("feat").unwrap();
        db.checkout("feat").unwrap();
        db.put("new_key", b"new_val".to_vec(), None).unwrap();

        db.checkout("main").unwrap();
        db.merge("feat", None).unwrap();
        assert_eq!(db.get("new_key").unwrap(), b"new_val");
        assert_eq!(db.get("base").unwrap(), b"val");
    }

    #[test]
    fn diff_versions() {
        let (_tmp, db) = test_db();
        let c1 = db.put("a", b"1".to_vec(), None).unwrap();
        let c2 = db.put("b", b"2".to_vec(), None).unwrap();

        let diff = db.diff(&c1.id, &c2.id).unwrap();
        assert_eq!(diff.added, vec!["b"]);
        assert!(diff.removed.is_empty());
    }

    #[test]
    fn scan_prefix_works() {
        let (_tmp, db) = test_db();
        db.put("user:1", b"alice".to_vec(), None).unwrap();
        db.put("user:2", b"bob".to_vec(), None).unwrap();
        db.put("order:1", b"o1".to_vec(), None).unwrap();

        let users = db.scan_prefix("user:").unwrap();
        assert_eq!(users.len(), 2);
    }

    #[test]
    fn stats_work() {
        let (_tmp, db) = test_db();
        db.put("k", b"v".to_vec(), None).unwrap();
        let stats = db.stats().unwrap();
        assert_eq!(stats.key_count, 1);
        assert_eq!(stats.commit_count, 1);
    }

    #[test]
    fn create_and_list_tags() {
        let (_tmp, db) = test_db();
        let c = db.put("k", b"v".to_vec(), None).unwrap();
        let tag = db
            .create_tag("v1.0", Some(&c.id), Some("first release"))
            .unwrap();
        assert_eq!(tag.name, "v1.0");
        assert_eq!(tag.commit_id, c.id);

        let tags = db.tags().unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0].name, "v1.0");
    }

    #[test]
    fn tag_current_head() {
        let (_tmp, db) = test_db();
        db.put("k", b"v".to_vec(), None).unwrap();
        let tag = db.create_tag("latest", None, None).unwrap();
        let head = db.head_commit().unwrap();
        assert_eq!(tag.commit_id, head.id);
    }

    #[test]
    fn duplicate_tag_fails() {
        let (_tmp, db) = test_db();
        db.put("k", b"v".to_vec(), None).unwrap();
        db.create_tag("v1", None, None).unwrap();
        assert!(db.create_tag("v1", None, None).is_err());
    }

    #[test]
    fn delete_tag() {
        let (_tmp, db) = test_db();
        db.put("k", b"v".to_vec(), None).unwrap();
        db.create_tag("v1", None, None).unwrap();
        db.delete_tag("v1").unwrap();
        assert!(db.tags().unwrap().is_empty());
    }

    #[test]
    fn cherry_pick_commit() {
        let (_tmp, db) = test_db();
        db.put("shared", b"data".to_vec(), None).unwrap();

        // Create a feature branch with a new key
        db.create_branch("feature").unwrap();
        db.checkout("feature").unwrap();
        let feat_commit = db.put("feat_key", b"feat_val".to_vec(), None).unwrap();

        // Switch back to main and cherry-pick
        db.checkout("main").unwrap();
        assert!(db.get("feat_key").is_err());

        db.cherry_pick(&feat_commit.id, Some("picked feature"))
            .unwrap();
        assert_eq!(db.get("feat_key").unwrap(), b"feat_val");
        assert_eq!(db.get("shared").unwrap(), b"data");
    }

    #[test]
    fn cherry_pick_delete() {
        let (_tmp, db) = test_db();
        db.put("a", b"1".to_vec(), None).unwrap();
        db.put("b", b"2".to_vec(), None).unwrap();

        db.create_branch("cleanup").unwrap();
        db.checkout("cleanup").unwrap();
        let del_commit = db.delete("a", None).unwrap();

        db.checkout("main").unwrap();
        assert_eq!(db.get("a").unwrap(), b"1"); // still there

        db.cherry_pick(&del_commit.id, None).unwrap();
        assert!(db.get("a").is_err()); // now gone
    }

    #[test]
    fn compact_with_max_versions() {
        let (_tmp, db) = test_db();
        for i in 0..5 {
            db.put("k", format!("v{}", i).into_bytes(), None).unwrap();
        }
        assert_eq!(db.log().unwrap().len(), 5);

        let policy = crate::compaction::CompactionPolicy {
            max_versions: 2,
            max_age_days: None,
        };
        let result = db.compact(&policy).unwrap();
        assert!(result.commits_removed > 0);

        // Current value should still work
        assert_eq!(db.get("k").unwrap(), b"v4");
    }

    #[test]
    fn compact_no_policy_removes_nothing() {
        let (_tmp, db) = test_db();
        db.put("a", b"1".to_vec(), None).unwrap();
        db.put("b", b"2".to_vec(), None).unwrap();

        let policy = crate::compaction::CompactionPolicy::default();
        let result = db.compact(&policy).unwrap();
        assert_eq!(result.commits_removed, 0);
        assert_eq!(db.log().unwrap().len(), 2);
    }

    #[test]
    fn delete_branch() {
        let (_tmp, db) = test_db();
        db.put("x", b"1".to_vec(), None).unwrap();
        db.create_branch("temp").unwrap();
        db.delete_branch("temp").unwrap();
        assert!(!db.branches().unwrap().contains(&"temp".to_string()));
    }

    #[test]
    fn bloom_filter_fast_negative() {
        let (_tmp, db) = test_db();
        db.put("exists", b"val".to_vec(), None).unwrap();
        // Bloom should say "maybe" for existing key
        assert!(db.get("exists").is_ok());
        // Non-existing key: bloom may short-circuit, but result is the same
        assert!(db.get("nope").is_err());
    }

    #[test]
    fn rebuild_bloom() {
        let (_tmp, db) = test_db();
        db.put("a", b"1".to_vec(), None).unwrap();
        db.put("b", b"2".to_vec(), None).unwrap();
        db.rebuild_bloom().unwrap();
        let (items, _, _) = db.bloom_stats();
        assert_eq!(items, 2);
    }

    #[test]
    fn rebase_branch() {
        let (_tmp, db) = test_db();
        // Setup: main has "base"
        db.put("base", b"val".to_vec(), Some("base commit"))
            .unwrap();

        // Create feature branch with a new key
        db.create_branch("feature").unwrap();
        db.checkout("feature").unwrap();
        db.put("feat", b"f1".to_vec(), Some("feat commit")).unwrap();

        // Add more to main
        db.checkout("main").unwrap();
        db.put("main_extra", b"m1".to_vec(), Some("main extra"))
            .unwrap();

        // Rebase feature onto main
        db.checkout("feature").unwrap();
        let rebased = db.rebase("main").unwrap();
        assert_eq!(rebased.len(), 1);

        // Feature branch should now have all keys
        assert_eq!(db.get("base").unwrap(), b"val");
        assert_eq!(db.get("feat").unwrap(), b"f1");
        assert_eq!(db.get("main_extra").unwrap(), b"m1");
    }

    #[test]
    fn rebase_onto_self_fails() {
        let (_tmp, db) = test_db();
        db.put("k", b"v".to_vec(), None).unwrap();
        assert!(db.rebase("main").is_err());
    }

    #[test]
    fn secondary_index_lifecycle() {
        let (_tmp, db) = test_db();
        // Create index
        db.create_index("city", "city").unwrap();
        assert_eq!(db.list_indexes(), vec!["city"]);

        // Put JSON values
        let zurich =
            serde_json::to_vec(&serde_json::json!({"city": "Zurich", "pop": 400000})).unwrap();
        let berlin =
            serde_json::to_vec(&serde_json::json!({"city": "Berlin", "pop": 3600000})).unwrap();
        db.put("ch:zurich", zurich, None).unwrap();
        db.put("de:berlin", berlin, None).unwrap();

        // Query index
        assert_eq!(db.query_index("city", "Zurich").unwrap(), vec!["ch:zurich"]);
        assert_eq!(db.query_index("city", "Berlin").unwrap(), vec!["de:berlin"]);
        assert!(db.query_index("city", "Paris").unwrap().is_empty());

        // Delete and verify index updated
        db.delete("ch:zurich", None).unwrap();
        assert!(db.query_index("city", "Zurich").unwrap().is_empty());

        // Drop index
        db.drop_index("city").unwrap();
        assert!(db.list_indexes().is_empty());
    }

    #[test]
    fn secondary_index_prefix_query() {
        let (_tmp, db) = test_db();
        db.create_index("city", "city").unwrap();

        let z1 = serde_json::to_vec(&serde_json::json!({"city": "Zurich"})).unwrap();
        let z2 = serde_json::to_vec(&serde_json::json!({"city": "Zug"})).unwrap();
        let b1 = serde_json::to_vec(&serde_json::json!({"city": "Berlin"})).unwrap();
        db.put("c:1", z1, None).unwrap();
        db.put("c:2", z2, None).unwrap();
        db.put("c:3", b1, None).unwrap();

        let results = db.query_index_prefix("city", "Zu").unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn wal_protects_writes() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let db = Database::init(tmp.path()).unwrap();
            db.put("key", b"value".to_vec(), None).unwrap();
        }
        // Reopen — WAL should recover cleanly
        let db = Database::open(tmp.path()).unwrap();
        assert_eq!(db.get("key").unwrap(), b"value");
    }
}
