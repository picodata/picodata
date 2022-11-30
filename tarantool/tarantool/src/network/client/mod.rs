//! Tarantool based client.
//! Can be used only from inside tarantool.

mod index;
mod inner;
mod promise;
mod recv_queue;
mod space;
mod stream;

use std::net::ToSocketAddrs;
use std::rc::Rc;
use std::time::Duration;

use self::promise::Promise;

use super::protocol::codec;
use super::protocol::options::{ConnOptions, Options};
use crate::error::Error;
use crate::tuple::{Decode, ToTupleBuffer, Tuple};
use inner::ConnInner;

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
}

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
    pub fn new(
        addr: impl ToSocketAddrs,
        options: ConnOptions,
        triggers: Option<Rc<dyn ConnTriggers>>,
    ) -> Result<Self, Error> {
        Ok(Conn {
            inner: ConnInner::new(addr.to_socket_addrs()?.collect(), options, triggers),
            is_master: true,
        })
    }

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
            .request(codec::encode_ping, |_, _| Ok(()), options)?;
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
            |buf, sync| codec::encode_call(buf, sync, function_name, args),
            codec::decode_call,
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
        self.inner.request_async(codec::Call(func, args))
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
            |buf, sync| codec::encode_eval(buf, sync, expression, args),
            codec::decode_call,
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
        self.inner.request_async(codec::Eval(expr, args))
    }

    /// Remote execute of sql query.
    pub fn execute(
        &self,
        sql: &str,
        bind_params: &impl ToTupleBuffer,
        options: &Options,
    ) -> Result<Vec<Tuple>, Error> {
        self.inner.request(
            |buf, sync| codec::encode_execute(buf, sync, sql, bind_params),
            |buf, _| codec::decode_multiple_rows(buf, None),
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
