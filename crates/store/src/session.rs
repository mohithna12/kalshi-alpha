//! Session metadata: the record that makes a partition self-describing.
//!
//! # Why this exists, and why it is written everywhere
//!
//! A Parquet file of prices is not interpretable on its own. Reading it
//! correctly requires knowing which pricing convention the NO side is in, what
//! scale the integer columns use, and which build produced them. That context
//! lives here.
//!
//! It is written into **every partition directory**, not once at the data root.
//! A partition copied, rsynced, or partially restored months later must remain
//! self-describing without its siblings — a lone directory of Parquet files
//! with no session record is data whose meaning has to be guessed, and guessing
//! is exactly what this project refuses to do.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Version of the fixed-point representation used by the integer columns.
///
/// Bump this if `Px`, `Qty`, or `Notional` ever change scale. Analysis can then
/// branch on it instead of assuming today's scales held all season.
pub const PARSER_VERSION: u32 = 1;

/// The git commit that produced this binary, from `build.rs`.
pub const GIT_SHA: &str = env!("KALSHI_GIT_SHA");

/// The date the Kalshi API documentation was read and this code written
/// against. Kalshi's API changes; a file that records which spec it was
/// captured under is far easier to reconcile later.
pub const DOCS_SPEC_DATE: &str = "2026-08-28";

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("session field {field} must be set explicitly; capture refuses to guess it")]
    MissingField { field: &'static str },
}

/// Which environment the data came from. Recorded because demo prices are
/// synthetic and must never be mistaken for production data in analysis.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Environment {
    Demo,
    Prod,
}

impl Environment {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Environment::Demo => "demo",
            Environment::Prod => "prod",
        }
    }
}

/// Everything needed to interpret the rows written during one run.
///
/// There is no `Default`. Every field is a fact about the capture that must be
/// stated, not defaulted — most importantly `pricing_convention`, whose
/// exchange-side default is scheduled to flip.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionMetadata {
    /// Unique per process run.
    pub session_id: String,
    pub started_at: DateTime<Utc>,
    /// Set on clean shutdown. `None` in a file from a crashed run, which is
    /// itself diagnostic.
    pub ended_at: Option<DateTime<Utc>>,

    /// `"no_leg"` or `"yes_leg"`. See `kalshi_ingest::ws::PricingConvention`.
    ///
    /// Without this a NO-side price is uninterpretable, because the exchange's
    /// default is scheduled to flip and then be removed.
    pub pricing_convention: String,

    /// Version of the fixed-point scales used by the `*_micros` / `*_fp_units`
    /// columns.
    pub parser_version: u32,
    /// `$1.00` in price units. Written explicitly so a reader never has to
    /// assume 1e6.
    pub px_scale: i64,
    /// One contract in quantity units. Written explicitly; not the same as
    /// `px_scale`.
    pub qty_scale: i64,

    pub environment: Environment,
    pub git_sha: String,
    pub docs_spec_date: String,

    /// Markets per orderbook subscription during this run. Determines a gap's
    /// blast radius, so it belongs with the data.
    pub orderbook_shard_size: usize,
    pub channels: Vec<String>,
    pub ws_url: String,
}

impl SessionMetadata {
    /// Construct a session record. Fails rather than defaulting.
    pub fn new(
        pricing_convention: &str,
        environment: Environment,
        orderbook_shard_size: usize,
        channels: Vec<String>,
        ws_url: String,
        started_at: DateTime<Utc>,
    ) -> Result<SessionMetadata, SessionError> {
        if pricing_convention.is_empty() {
            return Err(SessionError::MissingField {
                field: "pricing_convention",
            });
        }
        if ws_url.is_empty() {
            return Err(SessionError::MissingField { field: "ws_url" });
        }
        if channels.is_empty() {
            return Err(SessionError::MissingField { field: "channels" });
        }
        Ok(SessionMetadata {
            session_id: format!(
                "{}-{}",
                started_at.format("%Y%m%dT%H%M%SZ"),
                GIT_SHA.chars().take(8).collect::<String>()
            ),
            started_at,
            ended_at: None,
            pricing_convention: pricing_convention.to_owned(),
            parser_version: PARSER_VERSION,
            px_scale: kalshi_common::PX_SCALE,
            qty_scale: kalshi_common::QTY_SCALE,
            environment,
            git_sha: GIT_SHA.to_owned(),
            docs_spec_date: DOCS_SPEC_DATE.to_owned(),
            orderbook_shard_size,
            channels,
            ws_url,
        })
    }

    /// Mark the session closed. Absence of this is how a crashed run is
    /// recognized later.
    pub fn close(&mut self, ended_at: DateTime<Utc>) {
        self.ended_at = Some(ended_at);
    }

    /// Key/value pairs embedded in each Parquet file's own footer metadata, so
    /// a single file remains self-describing even if separated from the
    /// sidecar JSON.
    #[must_use]
    pub fn as_key_value_pairs(&self) -> Vec<(String, String)> {
        vec![
            ("kalshi.session_id".to_owned(), self.session_id.clone()),
            (
                "kalshi.pricing_convention".to_owned(),
                self.pricing_convention.clone(),
            ),
            (
                "kalshi.parser_version".to_owned(),
                self.parser_version.to_string(),
            ),
            ("kalshi.px_scale".to_owned(), self.px_scale.to_string()),
            ("kalshi.qty_scale".to_owned(), self.qty_scale.to_string()),
            (
                "kalshi.environment".to_owned(),
                self.environment.as_str().to_owned(),
            ),
            ("kalshi.git_sha".to_owned(), self.git_sha.clone()),
            (
                "kalshi.docs_spec_date".to_owned(),
                self.docs_spec_date.clone(),
            ),
            (
                "kalshi.orderbook_shard_size".to_owned(),
                self.orderbook_shard_size.to_string(),
            ),
            ("kalshi.started_at".to_owned(), self.started_at.to_rfc3339()),
        ]
    }

    /// Filename of the sidecar written into every partition directory.
    #[must_use]
    pub fn sidecar_filename(&self) -> String {
        format!("_session-{}.json", self.session_id)
    }
}
