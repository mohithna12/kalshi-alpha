//! Captures build provenance so every Parquet partition can say which build
//! wrote it. A capture file that cannot identify its own writer is much harder
//! to trust months later.
//!
//! The SHA is stamped here, at compile time. Whether the tree is *clean* is
//! deliberately NOT stamped here -- see `session::Provenance`. A build-time
//! answer to a question about the working tree is only correct until the next
//! edit, and cargo cannot be made to notice every edit.

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    // `.git/HEAD` alone is not enough: it changes on checkout, but a commit on
    // the current branch moves `refs/heads/<branch>` and leaves HEAD holding
    // the same `ref:` line. Watch both, plus packed-refs for a repo whose
    // loose refs have been packed away.
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/packed-refs");
    if let Ok(head) = std::fs::read_to_string("../../.git/HEAD") {
        if let Some(reference) = head.trim().strip_prefix("ref: ") {
            println!("cargo:rerun-if-changed=../../.git/{reference}");
        }
    }

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

    // The suffix stays: it is a useful record of how the binary was produced,
    // and it is what gets written into session metadata on a deliberate
    // --allow-dirty-tree run. It is no longer what the guard consults.
    let sha = if dirty { format!("{sha}-dirty") } else { sha };
    println!("cargo:rustc-env=KALSHI_GIT_SHA={sha}");
}
