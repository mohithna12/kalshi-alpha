//! Detection and quarantine of Parquet files left incomplete by a crash.
//!
//! # A footerless Parquet file is unreadable, not truncated
//!
//! Parquet keeps its schema and row-group index in a footer written at close.
//! A file killed before that footer lands is not "missing the last few rows" —
//! it cannot be opened at all, and the entire row group is lost. Worse, a
//! reader that walks a directory will choke on it, so one bad file from a crash
//! three weeks ago can block reading everything around it.
//!
//! Two mitigations, together:
//!
//! 1. bound the roll interval (see `writer`), so an unflushed file holds
//!    minutes rather than hours;
//!
//! 2. scan at startup and move any footerless file aside, before the writer
//!    opens anything. Quarantining is deliberately non-destructive: the bytes
//!    are kept under `_quarantine/` for salvage, never deleted.

use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// Parquet's magic bytes. A complete file both starts and ends with these.
const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

/// Minimum plausible size: the two magics plus a footer length.
const MIN_PARQUET_LEN: u64 = 12;

#[derive(Debug, thiserror::Error)]
pub enum QuarantineError {
    #[error("scanning {path}")]
    Scan {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("quarantining {path}")]
    Move {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

/// Why a file was quarantined.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Defect {
    /// Too short to be a Parquet file at all.
    TooShort,
    /// Does not begin with `PAR1`.
    MissingHeaderMagic,
    /// Begins but does not end with `PAR1` — the classic crash signature: the
    /// writer was killed before it could write the footer.
    MissingFooterMagic,
}

impl Defect {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Defect::TooShort => "too_short",
            Defect::MissingHeaderMagic => "missing_header_magic",
            Defect::MissingFooterMagic => "missing_footer_magic",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Quarantined {
    pub original: PathBuf,
    pub moved_to: PathBuf,
    pub defect: Defect,
    pub bytes: u64,
}

/// Check whether a file has a complete Parquet footer.
///
/// Returns `Ok(None)` for a healthy file.
pub fn inspect(path: &Path) -> Result<Option<Defect>, QuarantineError> {
    let mut file = fs::File::open(path).map_err(|source| QuarantineError::Scan {
        path: path.display().to_string(),
        source,
    })?;
    let len = file
        .metadata()
        .map_err(|source| QuarantineError::Scan {
            path: path.display().to_string(),
            source,
        })?
        .len();

    if len < MIN_PARQUET_LEN {
        return Ok(Some(Defect::TooShort));
    }

    let mut head = [0u8; 4];
    file.read_exact(&mut head)
        .map_err(|source| QuarantineError::Scan {
            path: path.display().to_string(),
            source,
        })?;
    if &head != PARQUET_MAGIC {
        return Ok(Some(Defect::MissingHeaderMagic));
    }

    file.seek(SeekFrom::End(-4))
        .map_err(|source| QuarantineError::Scan {
            path: path.display().to_string(),
            source,
        })?;
    let mut tail = [0u8; 4];
    file.read_exact(&mut tail)
        .map_err(|source| QuarantineError::Scan {
            path: path.display().to_string(),
            source,
        })?;
    if &tail != PARQUET_MAGIC {
        return Ok(Some(Defect::MissingFooterMagic));
    }

    Ok(None)
}

/// Walk `root` for `.parquet` files and quarantine any that are incomplete.
///
/// Run at startup, **before** any writer opens a file. Returns what was moved
/// so the daemon can log and count it — a non-empty result means the previous
/// run did not shut down cleanly.
pub fn scan_and_quarantine(root: &Path) -> Result<Vec<Quarantined>, QuarantineError> {
    let mut quarantined = Vec::new();
    if !root.exists() {
        return Ok(quarantined);
    }
    let quarantine_dir = root.join("_quarantine");
    let mut candidates = Vec::new();
    collect_parquet_files(root, &quarantine_dir, &mut candidates)?;

    for path in candidates {
        let Some(defect) = inspect(&path)? else {
            continue;
        };
        let bytes = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        // Preserve the partition structure inside the quarantine directory so
        // a salvaged file can be identified later.
        let relative = path.strip_prefix(root).unwrap_or(&path);
        let destination = quarantine_dir.join(relative);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|source| QuarantineError::Move {
                path: destination.display().to_string(),
                source,
            })?;
        }
        fs::rename(&path, &destination).map_err(|source| QuarantineError::Move {
            path: path.display().to_string(),
            source,
        })?;

        warn!(
            file = %path.display(),
            moved_to = %destination.display(),
            defect = defect.as_str(),
            bytes,
            "quarantined an incomplete Parquet file left by a previous run; \
             its footer was never written, so it cannot be read. The bytes are \
             kept for salvage, not deleted."
        );
        quarantined.push(Quarantined {
            original: path,
            moved_to: destination,
            defect,
            bytes,
        });
    }

    if quarantined.is_empty() {
        info!("startup scan found no incomplete Parquet files");
    } else {
        warn!(
            count = quarantined.len(),
            "previous run did not shut down cleanly"
        );
    }
    Ok(quarantined)
}

fn collect_parquet_files(
    dir: &Path,
    skip: &Path,
    out: &mut Vec<PathBuf>,
) -> Result<(), QuarantineError> {
    if dir == skip {
        return Ok(());
    }
    let entries = fs::read_dir(dir).map_err(|source| QuarantineError::Scan {
        path: dir.display().to_string(),
        source,
    })?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_parquet_files(&path, skip, out)?;
        } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            out.push(path);
        }
    }
    Ok(())
}
