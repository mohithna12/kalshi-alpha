//! Append-only Parquet storage.
//!
//! Every price and quantity column is written twice: the raw wire string
//! (`*_raw`) beside the parsed integer (`*_micros` / `*_fp_units`). If the
//! parser is later found to be wrong, the raw column allows re-deriving
//! everything; without it a season of capture would be unrecoverable.
//!
//! Filled in by deliverable 7.

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod encode;
pub mod grid_history;
pub mod quarantine;
pub mod schema;
pub mod session;
pub mod sink;
pub mod writer;

pub use encode::{encode, expected_rows};
pub use grid_history::{GridHistory, GridSource, GridVerdict};
pub use schema::{Channel, DualColumn, Scale};
pub use session::{Environment, SessionMetadata};
pub use sink::{channel, Offered, StoreHandle, StoreRecord};
pub use writer::{ParquetStore, StoreStats, WriteError, WriterConfig};
