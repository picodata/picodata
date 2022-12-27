use std::time::Duration;

use crate::error::Error;
use crate::net_box::Conn;

/// Most [Conn](struct.Conn.html) methods allows to pass an `options` argument
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
    /// Treats as unlimited if `None` specified.
    /// Default: `None`
    pub limit: Option<u32>,
}

/// Connection options; see [Conn::new()](struct.Conn.html#method.new)
#[derive(Clone)]
pub struct ConnOptions {
    /// Authentication user name. If left empty, then the session user is `'guest'`
    /// (the `'guest'` user does not need a password).
    ///
    /// Example:
    /// ```no_run
    /// use tarantool::net_box::{Conn, ConnOptions};
    /// Conn::new(
    ///     "localhost:3301",
    ///     ConnOptions {
    ///         user: "username".to_string(),
    ///         password: "userpassword".to_string(),
    ///         ..ConnOptions::default()
    ///     },
    ///     None
    /// );
    /// ```
    pub user: String,

    /// Authentication password.
    pub password: String,

    /// If `reconnect_after` is greater than zero, then a [Conn](struct.Conn.html) instance will try to reconnect if a
    /// connection is broken or if a connection attempt fails.
    ///
    /// This makes transient network failures become transparent to the application.
    /// Reconnect happens automatically in the background, so requests that initially fail due to connectivity loss are
    /// transparently retried.
    /// The number of retries is unlimited, connection attempts are made after each specified interval
    /// When a connection is explicitly closed, or when connection object is dropped, then reconnect attempts stop.
    pub reconnect_after: Duration,

    /// Duration to wait before returning “error: Connection timed out”.
    pub connect_timeout: Duration,

    /// Send buffer flush interval enforced in case of intensive requests stream.
    ///
    /// Guarantied to be maximum while requests are going.
    /// Default: 10ms
    pub send_buffer_flush_interval: Duration,

    /// Send buffer soft limit. If limit is reached, fiber will block before buffer flush.
    ///
    /// Note: This mechanism will prevent buffer overflow in most cases (not at all). In case overflow, buffer
    /// reallocation will occurred, which may cause performance issues.
    /// Default: 64000  
    pub send_buffer_limit: usize,

    /// Reallocated capacity of send buffer
    ///
    /// Default: 65536
    pub send_buffer_size: usize,

    /// Reallocated capacity of receive buffer
    ///
    /// Default: 65536
    pub recv_buffer_size: usize,
}

impl Default for ConnOptions {
    fn default() -> Self {
        ConnOptions {
            user: "".to_string(),
            password: "".to_string(),
            reconnect_after: Default::default(),
            connect_timeout: Default::default(),
            send_buffer_flush_interval: Duration::from_millis(10),
            send_buffer_limit: 64000,
            send_buffer_size: 65536,
            recv_buffer_size: 65536,
        }
    }
}

/// Provides triggers for connect, disconnect and schema reload events.
pub trait ConnTriggers {
    /// Defines a trigger for execution when a new connection is established, and authentication and schema fetch are
    /// completed due to an event such as `connect`.
    ///
    /// If the trigger execution fails and an exception happens, the connection’s state changes to `error`. In this
    /// case, the connection is terminated.
    fn on_connect(&self, conn: &Conn) -> Result<(), Error>;

    /// Define a trigger for execution after a connection is closed.
    fn on_disconnect(&self);

    /// Define a trigger executed when some operation has been performed on the remote server after schema has been
    /// updated. So, if a server request fails due to a schema version mismatch error, schema reload is triggered.
    fn on_schema_reload(&self, conn: &Conn);
}
