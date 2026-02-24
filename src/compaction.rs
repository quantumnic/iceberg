use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Configuration for compaction / garbage collection.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompactionPolicy {
    /// Maximum number of versions to retain per branch (0 = unlimited).
    pub max_versions: usize,
    /// Maximum age of commits to retain (None = unlimited).
    pub max_age_days: Option<u64>,
}

/// Result of a compaction run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactionResult {
    /// Number of commits removed.
    pub commits_removed: usize,
    /// Number of trees removed.
    pub trees_removed: usize,
    /// Number of blocks removed.
    pub blocks_removed: usize,
    /// Bytes reclaimed.
    pub bytes_reclaimed: u64,
}

impl std::fmt::Display for CompactionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Commits removed: {}", self.commits_removed)?;
        writeln!(f, "Trees removed:   {}", self.trees_removed)?;
        writeln!(f, "Blocks removed:  {}", self.blocks_removed)?;
        writeln!(f, "Bytes reclaimed: {}", self.bytes_reclaimed)?;
        Ok(())
    }
}

/// Determine which commits to keep given a policy.
/// Returns the set of commit IDs to remove.
pub fn find_removable_commits(
    commits: &[(String, DateTime<Utc>)],
    policy: &CompactionPolicy,
    now: DateTime<Utc>,
) -> Vec<String> {
    let mut to_remove = Vec::new();

    for (i, (id, ts)) in commits.iter().enumerate() {
        let mut should_remove = false;

        // Check max_versions (commits are newest-first)
        if policy.max_versions > 0 && i >= policy.max_versions {
            should_remove = true;
        }

        // Check max_age
        if let Some(max_days) = policy.max_age_days {
            let age = now.signed_duration_since(*ts);
            if age.num_days() > max_days as i64 {
                should_remove = true;
            }
        }

        if should_remove {
            to_remove.push(id.clone());
        }
    }

    to_remove
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn no_policy_removes_nothing() {
        let now = Utc::now();
        let commits = vec![
            ("a".into(), now),
            ("b".into(), now - Duration::days(1)),
            ("c".into(), now - Duration::days(2)),
        ];
        let policy = CompactionPolicy::default();
        let removable = find_removable_commits(&commits, &policy, now);
        assert!(removable.is_empty());
    }

    #[test]
    fn max_versions_removes_old() {
        let now = Utc::now();
        let commits = vec![
            ("a".into(), now),
            ("b".into(), now - Duration::days(1)),
            ("c".into(), now - Duration::days(2)),
            ("d".into(), now - Duration::days(3)),
        ];
        let policy = CompactionPolicy {
            max_versions: 2,
            max_age_days: None,
        };
        let removable = find_removable_commits(&commits, &policy, now);
        assert_eq!(removable, vec!["c", "d"]);
    }

    #[test]
    fn max_age_removes_old() {
        let now = Utc::now();
        let commits = vec![
            ("a".into(), now),
            ("b".into(), now - Duration::days(5)),
            ("c".into(), now - Duration::days(10)),
        ];
        let policy = CompactionPolicy {
            max_versions: 0,
            max_age_days: Some(7),
        };
        let removable = find_removable_commits(&commits, &policy, now);
        assert_eq!(removable, vec!["c"]);
    }

    #[test]
    fn combined_policy() {
        let now = Utc::now();
        let commits = vec![
            ("a".into(), now),
            ("b".into(), now - Duration::days(1)),
            ("c".into(), now - Duration::days(30)),
        ];
        let policy = CompactionPolicy {
            max_versions: 5,
            max_age_days: Some(7),
        };
        let removable = find_removable_commits(&commits, &policy, now);
        assert_eq!(removable, vec!["c"]);
    }
}
