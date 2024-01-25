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
//! ```text
//! connecting -> initial +-> active
//!                        \
//!                         +-> auth -> fetch_schema <-> active
//!
//!  (any state, on error) -> error_reconnect -> connecting -> ...
//!                                           \
//!                                             -> [error]
//!  (any_state, but [error]) -> [closed]
//! ```
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
#![cfg(feature = "net_box")]

use core::time::Duration;
use std::net::ToSocketAddrs;
use std::rc::Rc;

pub use index::{RemoteIndex, RemoteIndexIterator};
use inner::ConnInner;
pub use options::{ConnOptions, ConnTriggers, Options};
use promise::Promise;
pub(crate) use protocol::ResponseError;
pub use space::RemoteSpace;

use crate::error::Error;
use crate::tuple::{Decode, ToTupleBuffer, Tuple};

mod index;
mod inner;
mod options;
pub mod promise;
mod protocol;
mod recv_queue;
mod schema;
mod send_queue;
mod space;
mod stream;

/// Connection to remote Tarantool server
pub struct Conn {
    inner: Rc<ConnInner>,
    is_master: bool,
}

impl Conn {
    /// Create a new connection.
    ///
    /// The connection is established on demand, at the time of the first request. It can be re-established
    /// automatically after a disconnect (see [reconnect_after](struct.ConnOptions.html#structfield.reconnect_after) option).
    /// The returned conn object supports methods for making remote requests, such as select, update or delete.
    ///
    /// See also: [ConnOptions](struct.ConnOptions.html)
    #[inline(always)]
    pub fn new(
        addr: impl ToSocketAddrs,
        options: ConnOptions,
        triggers: Option<Rc<dyn ConnTriggers>>,
    ) -> Result<Self, Error> {
        Ok(Conn {
            inner: ConnInner::new(addr.to_socket_addrs()?.collect(), options, triggers)?,
            is_master: true,
        })
    }

    #[inline(always)]
    fn downgrade(inner: Rc<ConnInner>) -> Self {
        Conn {
            inner,
            is_master: false,
        }
    }

    /// Wait for connection to be active or closed.
    ///
    /// Returns:
    /// - `Ok(true)`: if active
    /// - `Ok(true)`: if closed
    /// - `Err(...TimedOut...)`: on timeout
    pub fn wait_connected(&self, timeout: Option<Duration>) -> Result<bool, Error> {
        self.inner.wait_connected(timeout)
    }

    /// Show whether connection is active or closed.
    pub fn is_connected(&self) -> bool {
        self.inner.is_connected()
    }

    /// Close a connection.
    pub fn close(&self) {
        self.inner.close()
    }

    /// Execute a PING command.
    ///
    /// - `options` – the supported option is `timeout`
    pub fn ping(&self, options: &Options) -> Result<(), Error> {
        self.inner
            .request(protocol::encode_ping, |_, _| Ok(()), options)?;
        Ok(())
    }

    /// Call a remote stored procedure.
    ///
    /// `conn.call("func", &("1", "2", "3"))` is the remote-call equivalent of `func('1', '2', '3')`.
    /// That is, `conn.call` is a remote stored-procedure call.
    /// The return from `conn.call` is whatever the function returns.
    pub fn call<T>(
        &self,
        function_name: &str,
        args: &T,
        options: &Options,
    ) -> Result<Option<Tuple>, Error>
    where
        T: ToTupleBuffer,
        T: ?Sized,
    {
        self.inner.request(
            |buf, sync| protocol::encode_call(buf, sync, function_name, args),
            protocol::decode_call,
            options,
        )
    }

    /// Call a remote stored procedure without yielding.
    ///
    /// If enqueuing a request succeeded a [`Promise`] is returned which will be
    /// kept once a response is received.
    pub fn call_async<A, R>(&self, func: &str, args: A) -> crate::Result<Promise<R>>
    where
        A: ToTupleBuffer,
        R: for<'de> Decode<'de> + 'static,
    {
        self.inner.request_async(protocol::Call(func, args))
    }

    /// Evaluates and executes the expression in Lua-string, which may be any statement or series of statements.
    ///
    /// An execute privilege is required; if the user does not have it, an administrator may grant it with
    /// `box.schema.user.grant(username, 'execute', 'universe')`.
    ///
    /// To ensure that the return from `eval` is whatever the Lua expression returns, begin the Lua-string with the
    /// word `return`.
    pub fn eval<T>(
        &self,
        expression: &str,
        args: &T,
        options: &Options,
    ) -> Result<Option<Tuple>, Error>
    where
        T: ToTupleBuffer,
        T: ?Sized,
    {
        self.inner.request(
            |buf, sync| protocol::encode_eval(buf, sync, expression, args),
            protocol::decode_call,
            options,
        )
    }

    /// Executes a series of lua statements on a remote host without yielding.
    ///
    /// If enqueuing a request succeeded a [`Promise`] is returned which will be
    /// kept once a response is received.
    pub fn eval_async<A, R>(&self, expr: &str, args: A) -> crate::Result<Promise<R>>
    where
        A: ToTupleBuffer,
        R: for<'de> Decode<'de> + 'static,
    {
        self.inner.request_async(protocol::Eval(expr, args))
    }

    /// Search space by name on remote server
    pub fn space(&self, name: &str) -> Result<Option<RemoteSpace>, Error> {
        Ok(self
            .inner
            .lookup_space(name)?
            .map(|space_id| RemoteSpace::new(self.inner.clone(), space_id)))
    }

    /// Remote execute of sql query.
    pub fn execute<P>(
        &self,
        sql: &str,
        bind_params: &P,
        options: &Options,
    ) -> Result<Vec<Tuple>, Error>
    where
        P: ToTupleBuffer + ?Sized,
    {
        self.inner.request(
            |buf, sync| protocol::encode_execute(buf, sync, sql, bind_params),
            |buf, _| protocol::decode_multiple_rows(buf, None),
            options,
        )
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        if self.is_master {
            self.close();
        }
    }
}

#[cfg(feature = "internal_test")]
mod tests {
    use super::*;
    use crate::test::util::listen_port;

    fn test_user_conn() -> Conn {
        Conn::new(
            ("localhost", listen_port()),
            ConnOptions {
                user: "test_user".into(),
                password: "password".into(),
                ..ConnOptions::default()
            },
            None,
        )
        .unwrap()
    }

    #[crate::test(tarantool = "crate")]
    fn dont_drop_worker_join_handles() {
        struct UnexpectedIOError;
        impl ToTupleBuffer for UnexpectedIOError {
            fn write_tuple_data(&self, _: &mut impl std::io::Write) -> Result<(), Error> {
                Err(Error::other("some io error"))
            }
        }

        let conn = test_user_conn();

        let e = conn
            .eval("return ...", &UnexpectedIOError, &Default::default())
            .unwrap_err();
        assert_eq!(e.to_string(), "some io error");

        let e = conn
            .eval("return ...", &[1], &Default::default())
            .unwrap_err();
        assert_eq!(e.to_string(), "io error: not connected");

        // At this point we used to panic
        conn.close();
    }

    // TODO: this test currently blocks on the second call for some reason
    // #[crate::test(tarantool = "crate")]
    // fn two_errors_in_a_row_bug() {
    //     let conn = test_user_conn();
    //
    //     let e = conn
    //         .eval("error 'oops'", &(), &Default::default())
    //         .unwrap_err();
    //     assert_eq!(e.to_string(), "server responded with error: eval:1: oops");
    //
    //     let e = conn
    //         .eval("error 'oops'", &(), &Default::default())
    //         .unwrap_err();
    //     assert_eq!(e.to_string(), "server responded with error: eval:1: oops");
    // }

    #[cfg(feature = "picodata")]
    #[crate::test(tarantool = "crate")]
    async fn ldap_auth_method() {
        use crate::auth::AuthMethod;

        let username = "Worry";
        let password = "B Gone";

        let _guard = crate::unwrap_ok_or!(
            crate::test::util::setup_ldap_auth(username, password),
            Err(e) => {
                println!("{e}, skipping ldap test");
                return;
            }
        );

        // Successfull connection
        {
            let conn = Conn::new(
                ("localhost", listen_port()),
                ConnOptions {
                    user: username.into(),
                    password: password.into(),
                    auth_method: AuthMethod::Ldap,
                    ..ConnOptions::default()
                },
                None,
            )
            .unwrap();

            conn.eval(
                "print('\\x1b[32mit works!\\x1b[0m')",
                &(),
                &Default::default(),
            )
            .unwrap();
        }

        // Wrong password
        {
            let conn = Conn::new(
                ("localhost", listen_port()),
                ConnOptions {
                    user: username.into(),
                    password: "wrong password".into(),
                    auth_method: AuthMethod::Ldap,
                    ..ConnOptions::default()
                },
                None,
            )
            .unwrap();

            let err = conn
                .eval("return", &(), &Default::default())
                .unwrap_err()
                .to_string();
            assert_eq!(
                err,
                "server responded with error: User not found or supplied credentials are invalid"
            );
        }

        // Wrong auth method
        {
            let conn = Conn::new(
                ("localhost", listen_port()),
                ConnOptions {
                    user: username.into(),
                    password: "wrong password".into(),
                    auth_method: AuthMethod::ChapSha1,
                    ..ConnOptions::default()
                },
                None,
            )
            .unwrap();

            let err = conn
                .eval("return", &(), &Default::default())
                .unwrap_err()
                .to_string();
            assert_eq!(
                err,
                "server responded with error: User not found or supplied credentials are invalid"
            );
        }
    }
}
