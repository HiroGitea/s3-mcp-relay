//! Shared building blocks for the S3-relayed control channel.
//!
//! The system has two ends that never talk to each other directly:
//!
//! * the **controller** (an MCP server Claude drives) enqueues [`Command`]s and
//!   waits for [`Response`]s;
//! * the **agent** (installed on the isolated server that can only reach S3)
//!   dequeues [`Command`]s, runs them, and enqueues [`Response`]s.
//!
//! An S3-compatible bucket is the only shared medium. It is treated as a pure
//! *transit* buffer: every object is deleted by whoever consumes it, so nothing
//! is meant to persist. See [`transport`] for the object-key layout.

pub mod blob;
pub mod config;
pub mod crypto;
pub mod pairing;
pub mod protocol;
pub mod transport;

pub use config::{optional_env, required_env, S3Config};
pub use crypto::Crypto;
pub use protocol::{
    validate_agent_id, validate_transfer_id, Command, CommandKind, DirEntry, Doorbell, Heartbeat, LogChunk,
    Response, ResponsePayload,
};
pub use transport::Transport;

/// Current wire-protocol version. Bump on incompatible [`protocol`] changes so
/// a controller and agent can refuse to talk across a mismatch.
///
/// v2 added the bulk transfer commands and the directory management commands.
/// An older agent would reject those as an unknown variant only once one was
/// actually sent; failing at the version check instead makes a half-upgraded
/// deployment obvious immediately.
pub const PROTOCOL_VERSION: u32 = 2;
