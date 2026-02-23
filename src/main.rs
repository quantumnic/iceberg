use clap::{Parser, Subcommand};
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
    /// Diff between two commits
    Diff { commit_a: String, commit_b: String },
    /// Merge a branch into current
    Merge {
        branch: String,
        #[arg(short, long)]
        message: Option<String>,
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
        Commands::Diff { commit_a, commit_b } => cmd_diff(&cli.db, &commit_a, &commit_b),
        Commands::Merge { branch, message } => cmd_merge(&cli.db, &branch, message.as_deref()),
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

fn cmd_stats(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(path)?;
    let stats = db.stats()?;
    print!("{}", stats);
    Ok(())
}
