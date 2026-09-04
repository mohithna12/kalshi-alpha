//! Captures build provenance so every Parquet partition can say which build
//! wrote it. A capture file that cannot identify its own writer is much harder
//! to trust months later.

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=build.rs");

    let sha = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|s| s.trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned());

    let dirty = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| !out.stdout.is_empty())
        .unwrap_or(false);

    // Exposed separately from the SHA so the daemon can refuse to run against
    // production from a dirty tree: data captured under a `-dirty` SHA cannot
    // be tied back to reproducible code.
    println!("cargo:rustc-env=KALSHI_GIT_DIRTY={dirty}");
    let sha = if dirty { format!("{sha}-dirty") } else { sha };
    println!("cargo:rustc-env=KALSHI_GIT_SHA={sha}");
}
