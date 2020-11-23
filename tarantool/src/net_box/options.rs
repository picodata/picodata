use bitflags::_core::time::Duration;

/// Most [Conn](struct.Conn.html) methods allow a `options` argument
///
/// Which can be:
#[derive(Default)]
pub struct Options {
    /// For example, a method whose `options` argument is `{timeout: Some(Duration::from_secs_f32(1.5)})` will stop
    /// after 1.5 seconds on the local node, although this does not guarantee that execution will stop on the remote
    /// server node.
    pub timeout: Option<Duration>,
}

/// Connection options; see [Conn::new()](struct.Conn.html#method.new)
#[derive(Default)]
pub struct ConnOptions {
    pub user: String,
    /// You have two ways to connect to a remote host: using URI or using the options user and password.
    /// For example, instead of
    /// ```rust
    /// # use tarantool_module::net_box::{Conn, ConnOptions};
    /// # use url::Url;
    /// Conn::new(
    ///     Url::parse("username:userpassword@localhost:3301").unwrap(),
    ///     ConnOptions::default()
    /// );
    /// ```
    /// you can write
    /// ```rust
    /// # use tarantool_module::net_box::{Conn, ConnOptions};
    /// # use url::Url;
    /// Conn::new(
    ///     Url::parse("localhost:3301").unwrap(),
    ///     ConnOptions {
    ///         user: "username".to_string(),
    ///         password: "userpassword".to_string(),
    ///         ..ConnOptions::default()
    ///     }
    /// );
    /// ```
    pub password: String,

    /// If `reconnect_after` is greater than zero, then a [Conn](struct.Conn.html) instance will try to reconnect if a
    /// connection is broken or if a connection attempt fails.
    /// This makes transient network failures become transparent to the application.
    /// Reconnect happens automatically in the background, so requests that initially fail due to connectivity loss are
    /// transparently retried.
    /// The number of retries is unlimited, connection attempts are made after each specified interval
    /// When a connection is explicitly closed, or when connection object is dropped, then reconnect attempts stop.
    pub reconnect_after: Duration,
}
