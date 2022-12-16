use bitflags::_core::time::Duration;

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
}

impl Default for ConnOptions {
    fn default() -> Self {
        ConnOptions {
            user: "".to_string(),
            password: "".to_string(),
        }
    }
}
