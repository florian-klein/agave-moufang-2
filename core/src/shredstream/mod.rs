//! Jito Shredstream Integration
//!
//! This module provides integration with Jito's shredstream service to receive
//! shreds directly from Jito's block engine. This allows validators to receive
//! shreds with lower latency than waiting for normal turbine propagation.
//!
//! # Architecture
//!
//! ```text
//! [Jito Block Engine] <---(gRPC heartbeat)--- [ShredstreamService]
//!         |                                          |
//!         v (UDP shreds to registered socket)        | (registers TVU socket)
//! [TVU :8001 sockets] ---> [ShredFetchStage] ---> [SigVerify] ---> [Blockstore]
//!         ^
//!         | (normal turbine shreds)
//! [Leader Nodes]
//! ```
//!
//! The service sends periodic heartbeats to Jito's block engine via gRPC,
//! registering the validator's TVU socket address. Jito then sends shreds
//! directly to that socket via UDP, where they are processed alongside
//! normal turbine shreds by the existing ShredFetchStage and deduplication.

mod auth;
mod config;
pub(crate) mod file_log;
mod health;
mod heartbeat;
mod multi_heartbeat;
mod protos;
mod service;

pub use config::ShredstreamConfig;
pub use health::{ShredstreamHealth, ShredstreamHealthStatus};
pub use multi_heartbeat::create_multi_heartbeat_sockets;
pub use service::ShredstreamService;

use thiserror::Error;

/// Errors that can occur in the shredstream module
#[derive(Debug, Error)]
pub enum ShredstreamError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("connection error: {0}")]
    Connection(#[from] auth::BlockEngineConnectionError),
}
