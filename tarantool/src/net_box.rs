//! The `net_box` module contains connector to remote Tarantool server instances via a network.
//!
//! You can call the following methods:
//! - [Conn::new()](struct.Conn.html#method.new) to connect and get a connection object (named `conn` for examples in this section),
//! - other `net_box` routines, to execute requests on the remote database system,
//! - [conn.close()](struct.Conn.html#method.close) to disconnect.
//!
//! All [Conn](struct.Conn.html) methods are fiber-safe, that is, it is safe to share and use the same connection object
//! across multiple concurrent fibers. In fact that is perhaps the best programming practice with Tarantool. When
//! multiple fibers use the same connection, all requests are pipelined through the same network socket, but each fiber
//! gets back a correct response. Reducing the number of active sockets lowers the overhead of system calls and increases
//! the overall server performance. However for some cases a single connection is not enough — for example, when it is
//! necessary to prioritize requests or to use different authentication IDs.
//!
//! Most [Conn](struct.Conn.html) methods allow a `options` argument. See [Options](struct.Options.html) structure docs
//! for details.
//!
//! The diagram below shows possible connection states and transitions:
//!
//! ![img](https://hb.bizmrg.com/tarantool-io/doc-builds/tarantool/2.6/images_en/net_states.svg?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=5qdnUajcfXmhe1ME4C5DqG%2F20201118%2Fru-msk%2Fs3%2Faws4_request&X-Amz-Date=20201118T130426Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=d7df0b06513b11fa375875cfe6dc9de2dbc7073fe6ed1a11c8ce668b5fd02530)
//!
//! On this diagram:
//! - The state machine starts in the `initial` state.
//! - [Conn::new()](struct.Conn.html#method.new) method changes the state to `connecting` and spawns a worker fiber.
//! - If authentication and schema upload are required, it’s possible later on to re-enter the `fetch_schema` state
//! from `active` if a request fails due to a schema version mismatch error, so schema reload is triggered.
//! - [conn.close()](struct.Conn.html#method.close) method sets the state to `closed` and kills the worker. If the
//! transport is already in the `error` state, [close()](struct.Conn.html#method.close) does nothing.
//!
//! See also:
//! - [Lua reference: Module net.box](https://www.tarantool.io/en/doc/latest/reference/reference_lua/net_box/)

use bitflags::_core::time::Duration;
use url::Url;

use crate::error::Error;
use crate::tuple::{AsTuple, Tuple};

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

/// Connection to remote Tarantool server
pub struct Conn {}

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

impl Conn {
    /// Create a new connection.
    ///
    /// The connection is established on demand, at the time of the first request. It can be re-established
    /// automatically after a disconnect (see [reconnect_after](struct.ConnOptions.html#structfield.reconnect_after) option).
    /// The returned conn object supports methods for making remote requests, such as select, update or delete.
    ///
    /// See also: [ConnOptions]()
    pub fn new(url: Url, options: ConnOptions) -> Self {
        unimplemented!()
    }

    /// Wait for connection to be active or closed.
    pub fn wait_connected(&self, timeout: Option<Duration>) -> Result<(), Error> {
        unimplemented!()
    }

    /// Show whether connection is active or closed.
    pub fn is_connected(&self) -> bool {
        unimplemented!()
    }

    /// Execute a PING command.
    ///
    /// - `options` – the supported option is `timeout`
    pub fn ping(&self, options: &Options) -> Result<(), Error> {
        unimplemented!()
    }

    /// Close a connection.
    pub fn close(self) {
        unimplemented!()
    }

    /// Call a remote stored procedure.
    ///
    /// `conn.call("func", &("1", "2", "3"))` is the remote-call equivalent of `func('1', '2', '3')`.
    /// That is, `conn.call` is a remote stored-procedure call.
    /// The return from `conn.call` is whatever the function returns.
    pub fn call<T, R>(
        &self,
        function_name: &str,
        args: &T,
        options: &Options,
    ) -> Result<Option<Tuple>, Error>
    where
        T: AsTuple,
    {
        unimplemented!()
    }
}
