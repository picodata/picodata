//! Sans-I/O network client for connecting to remote Tarantool server.
//!
//! Consists of:
//! - Runtime and transport agnostic [`protocol`] layer
//! - Async and coio based [`client`] layer
//!
//! More on Sans-I/O pattern can be found on the respective [wiki](https://sans-io.readthedocs.io/how-to-sans-io.html).
//!
//! This client implementation is not yet as feature rich as [`super::net_box`].
//! Though it is in active development and should eventually replace net_box.

#[cfg(feature = "network_client")]
pub mod client;
pub mod protocol;

#[cfg(feature = "network_client")]
pub use client::reconnect::Client as ReconnClient;
#[cfg(feature = "network_client")]
pub use client::{AsClient, Client, Error};
pub use protocol::Config;
