use bitflags::_core::time::Duration;

use crate::error::Error;
use crate::net_box::Conn;

/// Most [Conn](struct.Conn.html) methods allow a `options` argument
///
/// Some options are applicable **only to some** methods (will be ignored otherwise).  
///
/// Which can be:
#[derive(Default, Clone)]
pub struct Options {
    /// For example, a method whose `options` argument is `{timeout: Some(Duration::from_secs_f32(1.5)})` will stop
    /// after 1.5 seconds on the local node, although this does not guarantee that execution will stop on the remote
    /// server node.
    pub timeout: Option<Duration>,

    /// The `offset` option specifies the number of rows to skip before starting to return rows from the query.
    ///
    /// Can be used with [select()](struct.RemoteIndex.html#method.select) method.
    /// Default: `0`
    pub offset: u32,

    /// The `limit` option specifies the number of rows to return after the `offset` option has been processed.
    ///
    /// Can be used with [select()](struct.RemoteIndex.html#method.select) method.
    /// Default: unlimited (if `None` specified)
    pub limit: Option<u32>,
}

/// Connection options; see [Conn::new()](struct.Conn.html#method.new)
#[derive(Default)]
pub struct ConnOptions {
    /// Authentication user name. If left empty, then the session user is `'guest'`
    /// (the `'guest'` user does not need a password).
    ///
    /// Example:
    /// ```rust
    /// # use tarantool_module::net_box::{Conn, ConnOptions};
    /// Conn::new(
    ///     "localhost:3301",
    ///     ConnOptions {
    ///         user: "username".to_string(),
    ///         password: "userpassword".to_string(),
    ///         ..ConnOptions::default()
    ///     }
    /// );
    /// ```
    pub user: String,

    /// Authentication password.
    pub password: String,

    /// If `reconnect_after` is greater than zero, then a [Conn](struct.Conn.html) instance will try to reconnect if a
    /// connection is broken or if a connection attempt fails.
    /// This makes transient network failures become transparent to the application.
    /// Reconnect happens automatically in the background, so requests that initially fail due to connectivity loss are
    /// transparently retried.
    /// The number of retries is unlimited, connection attempts are made after each specified interval
    /// When a connection is explicitly closed, or when connection object is dropped, then reconnect attempts stop.
    pub reconnect_after: Duration,

    /// Duration to wait before returning “error: Connection timed out”.
    pub connect_timeout: Duration,

    /// Triggers
    pub triggers: Option<Box<dyn ConnTriggers>>,
}

pub trait ConnTriggers {
    /// Defines a trigger for execution when a new connection is established, and authentication and schema fetch are
    /// completed due to an event such as `connect`.
    ///
    /// If the trigger execution fails and an exception happens, the connection’s state changes to ‘error’. In this
    /// case, the connection is terminated, regardless of the reconnect_after option’s value.
    fn on_connect(&self, conn: &Conn) -> Result<(), Error>;

    /// Define a trigger for execution after a connection is closed.
    fn on_disconnect(&self);

    /// Define a trigger executed when some operation has been performed on the remote server after schema has been
    /// updated. So, if a server request fails due to a schema version mismatch error, schema reload is triggered.
    fn on_schema_reload(&self, conn: &Conn);
}
