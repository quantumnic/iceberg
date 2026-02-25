use clap::{Parser, Subcommand};
use iceberg::compaction::CompactionPolicy;
use iceberg::db::Database;
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(
    name = "iceberg",
    version,
    about = "Immutable versioned database with git-like branching"
)]
struct Cli {
    /// Database path (default: ./iceberg.db)
    #[arg(long, default_value = "iceberg.db")]
    db: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new database
    Init,
    /// Store a key-value pair
    Put {
        key: String,
        value: String,
        /// Commit message
        #[arg(short, long)]
        message: Option<String>,
    },
    /// Retrieve a value by key
    Get {
        key: String,
        /// Get value at a specific commit
        #[arg(long)]
        at: Option<String>,
    },
    /// Delete a key
    Delete {
        key: String,
        #[arg(short, long)]
        message: Option<String>,
    },
    /// List keys matching a prefix
    Scan { prefix: String },
    /// Show version history
    Log {
        /// Max entries to show
        #[arg(short = 'n', long, default_value = "20")]
        limit: usize,
    },
    /// Create a new branch
    Branch { name: String },
    /// Switch to a branch
    Checkout { name: String },
    /// List all branches
    Branches,
    /// Delete a branch
    DeleteBranch { name: String },
    /// Diff between two commits
    Diff { commit_a: String, commit_b: String },
    /// Merge a branch into current
    Merge {
        branch: String,
        #[arg(short, long)]
        message: Option<String>,
    },
    /// Cherry-pick a commit onto the current branch
    CherryPick {
        /// Commit ID to cherry-pick
        commit: String,
        #[arg(short, long)]
        message: Option<String>,
    },
    /// Create a tag
    Tag {
        /// Tag name
        name: String,
        /// Commit to tag (default: HEAD)
        #[arg(long)]
        commit: Option<String>,
        /// Tag message
        #[arg(short, long)]
        message: Option<String>,
    },
    /// List all tags
    Tags,
    /// Delete a tag
    DeleteTag { name: String },
    /// Rebase current branch onto another branch
    Rebase {
        /// Target branch to rebase onto
        onto: String,
    },
    /// Create a secondary index on a JSON field
    CreateIndex {
        /// Index name
        name: String,
        /// JSON field path (e.g., "city" or "address.country")
        field: String,
    },
    /// Drop a secondary index
    DropIndex {
        /// Index name
        name: String,
    },
    /// Query a secondary index
    QueryIndex {
        /// Index name
        name: String,
        /// Value to search for
        value: String,
        /// Use prefix matching
        #[arg(long)]
        prefix: bool,
    },
    /// List secondary indexes
    Indexes,
    /// Run compaction / garbage collection
    Compact {
        /// Keep at most N versions (0 = unlimited)
        #[arg(long, default_value = "0")]
        max_versions: usize,
        /// Keep commits at most N days old
        #[arg(long)]
        max_age_days: Option<u64>,
    },
    /// Show database statistics
    Stats,
}

fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Init => cmd_init(&cli.db),
        Commands::Put {
            key,
            value,
            message,
        } => cmd_put(&cli.db, &key, &value, message.as_deref()),
        Commands::Get { key, at } => cmd_get(&cli.db, &key, at.as_deref()),
        Commands::Delete { key, message } => cmd_delete(&cli.db, &key, message.as_deref()),
        Commands::Scan { prefix } => cmd_scan(&cli.db, &prefix),
        Commands::Log { limit } => cmd_log(&cli.db, limit),
        Commands::Branch { name } => cmd_branch(&cli.db, &name),
        Commands::Checkout { name } => cmd_checkout(&cli.db, &name),
        Commands::Branches => cmd_branches(&cli.db),
        Commands::DeleteBranch { name } => cmd_delete_branch(&cli.db, &name),
        Commands::Diff { commit_a, commit_b } => cmd_diff(&cli.db, &commit_a, &commit_b),
        Commands::Merge { branch, message } => cmd_merge(&cli.db, &branch, message.as_deref()),
        Commands::CherryPick { commit, message } => {
            cmd_cherry_pick(&cli.db, &commit, message.as_deref())
        }
        Commands::Tag {
            name,
            commit,
            message,
        } => cmd_tag(&cli.db, &name, commit.as_deref(), message.as_deref()),
        Commands::Tags => cmd_tags(&cli.db),
        Commands::DeleteTag { name } => cmd_delete_tag(&cli.db, &name),
        Commands::Rebase { onto } => cmd_rebase(&cli.db, &onto),
        Commands::CreateIndex { name, field } => cmd_create_index(&cli.db, &name, &field),
        Commands::DropIndex { name } => cmd_drop_index(&cli.db, &name),
        Commands::QueryIndex {
            name,
            value,
            prefix,
        } => cmd_query_index(&cli.db, &name, &value, prefix),
        Commands::Indexes => cmd_indexes(&cli.db),
        Commands::Compact {
            max_versions,
            max_age_days,
        } => cmd_compact(&cli.db, max_versions, max_age_days),
        Commands::Stats => cmd_stats(&cli.db),
    };

    if let Err(e) = result {
        eprintln!("error: {}", e);
        std::process::exit(1);
    }
}

fn cmd_init(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    Database::init(path)?;
    println!("Initialized iceberg database at {}", path.display());
    Ok(())
}

fn cmd_put(
    path: &Path,
    key: &str,
    value: &str,
    msg: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    let commit = db.put(key, value.as_bytes().to_vec(), msg)?;
    println!("[{}] {}", &commit.id[..8], commit.message);
    Ok(())
}

fn cmd_get(path: &Path, key: &str, at: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    let value = match at {
        Some(commit_id) => db.get_at(key, commit_id)?,
        None => db.get(key)?,
    };
    println!("{}", String::from_utf8_lossy(&value));
    Ok(())
}

fn cmd_delete(path: &Path, key: &str, msg: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    let commit = db.delete(key, msg)?;
    println!("[{}] {}", &commit.id[..8], commit.message);
    Ok(())
}

fn cmd_scan(path: &Path, prefix: &str) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    let entries = db.scan_prefix(prefix)?;
    for (k, v) in entries {
        println!("{} = {}", k, String::from_utf8_lossy(&v));
    }
    Ok(())
}

fn cmd_log(path: &Path, limit: usize) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    let log = db.log()?;
    for commit in log.iter().take(limit) {
        println!(
            "{} {} {}",
            &commit.id[..8],
            commit.timestamp.format("%Y-%m-%d %H:%M:%S"),
            commit.message,
        );
    }
    if log.is_empty() {
        println!("(no commits yet)");
    }
    Ok(())
}

fn cmd_branch(path: &Path, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    db.create_branch(name)?;
    println!("Created branch '{}'", name);
    Ok(())
}

fn cmd_checkout(path: &Path, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    db.checkout(name)?;
    println!("Switched to branch '{}'", name);
    Ok(())
}

fn cmd_branches(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    let current = db.current_branch()?;
    let branches = db.branches()?;
    for b in branches {
        if b == current {
            println!("* {}", b);
        } else {
            println!("  {}", b);
        }
    }
    Ok(())
}

fn cmd_delete_branch(path: &Path, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    db.delete_branch(name)?;
    println!("Deleted branch '{}'", name);
    Ok(())
}

fn cmd_diff(path: &Path, a: &str, b: &str) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    let diff = db.diff(a, b)?;
    if diff.is_empty() {
        println!("No differences");
    } else {
        for k in &diff.added {
            println!("+ {}", k);
        }
        for k in &diff.removed {
            println!("- {}", k);
        }
        for k in &diff.modified {
            println!("~ {}", k);
        }
    }
    Ok(())
}

fn cmd_merge(
    path: &Path,
    branch: &str,
    msg: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    let commit = db.merge(branch, msg)?;
    println!("[{}] {}", &commit.id[..8], commit.message);
    Ok(())
}

fn cmd_cherry_pick(
    path: &Path,
    commit_id: &str,
    msg: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    let commit = db.cherry_pick(commit_id, msg)?;
    println!("[{}] {}", &commit.id[..8], commit.message);
    Ok(())
}

fn cmd_tag(
    path: &Path,
    name: &str,
    commit: Option<&str>,
    msg: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    let tag = db.create_tag(name, commit, msg)?;
    println!("Tagged {} → {}", tag.name, &tag.commit_id[..8]);
    Ok(())
}

fn cmd_tags(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    let tags = db.tags()?;
    if tags.is_empty() {
        println!("(no tags)");
    } else {
        for tag in &tags {
            let msg = tag
                .message
                .as_deref()
                .map(|m| format!(" — {}", m))
                .unwrap_or_default();
            println!(
                "{} → {} {}{}",
                tag.name,
                &tag.commit_id[..8],
                tag.created_at.format("%Y-%m-%d %H:%M:%S"),
                msg,
            );
        }
    }
    Ok(())
}

fn cmd_delete_tag(path: &Path, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    db.delete_tag(name)?;
    println!("Deleted tag '{}'", name);
    Ok(())
}

fn cmd_rebase(path: &Path, onto: &str) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    let commits = db.rebase(onto)?;
    if commits.is_empty() {
        println!("Nothing to rebase — already up to date.");
    } else {
        println!("Rebased {} commit(s) onto '{}':", commits.len(), onto);
        for c in &commits {
            println!("  [{}] {}", &c.id[..8], c.message);
        }
    }
    Ok(())
}

fn cmd_create_index(
    path: &Path,
    name: &str,
    field: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    db.create_index(name, field)?;
    println!("Created index '{}' on field '{}'", name, field);
    Ok(())
}

fn cmd_drop_index(path: &Path, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    db.drop_index(name)?;
    println!("Dropped index '{}'", name);
    Ok(())
}

fn cmd_query_index(
    path: &Path,
    name: &str,
    value: &str,
    prefix: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    let keys = if prefix {
        db.query_index_prefix(name, value)?
    } else {
        db.query_index(name, value)?
    };
    if keys.is_empty() {
        println!("(no matches)");
    } else {
        for k in &keys {
            println!("{}", k);
        }
    }
    Ok(())
}

fn cmd_indexes(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    let indexes = db.list_indexes();
    if indexes.is_empty() {
        println!("(no indexes)");
    } else {
        for name in &indexes {
            println!("{}", name);
        }
    }
    Ok(())
}

fn cmd_compact(
    path: &Path,
    max_versions: usize,
    max_age_days: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    let policy = CompactionPolicy {
        max_versions,
        max_age_days,
    };
    let result = db.compact(&policy)?;
    print!("{}", result);
    Ok(())
}

fn cmd_stats(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    let stats = db.stats()?;
    print!("{}", stats);
    Ok(())
}
