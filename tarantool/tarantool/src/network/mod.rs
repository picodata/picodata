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

pub use protocol::ProtocolError;

#[cfg(feature = "network_client")]
pub use client::reconnect::Client as ReconnClient;
#[cfg(feature = "network_client")]
pub use client::{AsClient, Client, ClientError};
pub use protocol::Config;

#[cfg(feature = "network_client")]
#[deprecated = "use `ClientError` instead"]
pub type Error = client::ClientError;

#[cfg(feature = "internal_test")]
#[cfg(feature = "network_client")]
mod tests {
    use super::*;
    use crate::test::util::listen_port;

    #[crate::test(tarantool = "crate")]
    async fn wrong_credentials() {
        // Wrong user
        {
            let mut config = Config::default();
            config.creds = Some(("no such user".into(), "password".into()));
            let client = ReconnClient::with_config("localhost".into(), listen_port(), config);

            let err = client.ping().await.unwrap_err();
            #[rustfmt::skip]
            assert_eq!(err.to_string(), "server responded with error: PasswordMismatch: User not found or supplied credentials are invalid");
        }

        // Wrong password
        {
            let mut config = Config::default();
            config.creds = Some(("test_user".into(), "wrong password".into()));
            let client = ReconnClient::with_config("localhost".into(), listen_port(), config);

            let err = client.ping().await.unwrap_err();
            #[rustfmt::skip]
            assert_eq!(err.to_string(), "server responded with error: PasswordMismatch: User not found or supplied credentials are invalid");
        }
    }
}
