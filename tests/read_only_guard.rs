//! Structural guarantee: this repository contains no order-placement path.
//!
//! The primary guarantee is that the REST client exposes no method accepting a
//! request body, which removes the ability to reach any Kalshi write endpoint.
//! This test is a second, cruder net for the case where someone (including a
//! future version of me) adds one.
//!
//! Patterns are deliberately narrow -- API surface only. A guard that
//! false-positives on `Vec::fill` or `fill_buffer` gets disabled by week three,
//! which is worse than no guard at all.

use std::fs;
use std::path::{Path, PathBuf};

/// Kalshi order-placement API surface. Matching is case-sensitive and anchored
/// on real endpoint paths and SDK-style function names, not English words.
const FORBIDDEN: &[&str] = &[
    "portfolio/orders",
    "POST /portfolio",
    "create_order",
    "cancel_order",
    "amend_order",
    "batch_create",
];

fn is_skipped(path: &Path) -> bool {
    // This file necessarily contains every pattern it searches for.
    if path.file_name().is_some_and(|n| n == "read_only_guard.rs") {
        return true;
    }
    path.components().any(|c| {
        matches!(
            c.as_os_str().to_str(),
            Some(".git") | Some("target") | Some("data")
        )
    })
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if is_skipped(&path) {
            continue;
        }
        if path.is_dir() {
            collect(&path, out);
        } else if matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("rs") | Some("toml")
        ) {
            out.push(path);
        }
    }
}

#[test]
fn repository_contains_no_order_placement_surface() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    collect(root, &mut files);

    assert!(
        files.len() > 5,
        "guard scanned only {} files; the walk is broken and the guard is \
         silently passing",
        files.len()
    );

    let mut violations = Vec::new();
    for file in &files {
        let Ok(text) = fs::read_to_string(file) else {
            continue;
        };
        for (lineno, line) in text.lines().enumerate() {
            for pattern in FORBIDDEN {
                if line.contains(pattern) {
                    violations.push(format!(
                        "{}:{}: {pattern}",
                        file.strip_prefix(root).unwrap_or(file).display(),
                        lineno + 1
                    ));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "order-placement surface detected; this repository is read-only:\n  {}",
        violations.join("\n  ")
    );
}
