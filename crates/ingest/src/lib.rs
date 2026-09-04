//! Kalshi ingest: authentication, REST discovery, WebSocket transport, local book.
//!
//! # Read-only by construction
//!
//! Nothing in this crate can place, modify, or cancel an order. The REST client
//! (deliverable 4) exposes no method that accepts a request body, which removes
//! the ability to reach any Kalshi write endpoint -- those are all POST/PUT/DELETE
//! with bodies. This is a structural guarantee, not a runtime check or a flag.

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod auth; // deliverable 3
pub mod book; // deliverable 6
pub mod rest; // deliverable 4
pub mod wire;
pub mod ws; // deliverable 5
