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

use core::time::Duration;
use std::io::Cursor;
use std::net::ToSocketAddrs;
use std::rc::Rc;

use inner::{ConnInner, ConnState};
pub use options::{ConnOptions, Options};
pub(crate) use protocol::ResponseError;

use crate::error::Error;
use crate::index::IteratorType;
use crate::tuple::{AsTuple, Tuple};

mod inner;
mod options;
mod protocol;

/// Connection to remote Tarantool server
pub struct Conn {
    inner: Rc<ConnInner>,
}

impl Conn {
    /// Create a new connection.
    ///
    /// The connection is established on demand, at the time of the first request. It can be re-established
    /// automatically after a disconnect (see [reconnect_after](struct.ConnOptions.html#structfield.reconnect_after) option).
    /// The returned conn object supports methods for making remote requests, such as select, update or delete.
    ///
    /// See also: [ConnOptions](struct.ConnOptions.html)
    pub fn new(addr: &str, options: ConnOptions) -> Result<Self, Error> {
        Ok(Conn {
            inner: Rc::new(ConnInner::new(addr.to_socket_addrs()?.collect(), options)),
        })
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
        matches!(self.inner.state(), ConnState::Active)
    }

    /// Close a connection.
    pub fn close(&self) {
        self.inner.close()
    }

    /// Execute a PING command.
    ///
    /// - `options` – the supported option is `timeout`
    pub fn ping(&self, options: &Options) -> Result<(), Error> {
        let buf = Vec::new();
        let mut cur = Cursor::new(buf);

        let sync = self.inner.next_sync();
        protocol::encode_ping(&mut cur, sync)?;
        self.inner.communicate(&cur.into_inner(), sync, options)?;
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
        T: AsTuple,
    {
        let buf = Vec::new();
        let mut cur = Cursor::new(buf);

        let sync = self.inner.next_sync();
        protocol::encode_call(&mut cur, sync, function_name, args)?;
        let response = self.inner.communicate(&cur.into_inner(), sync, options)?;
        Ok(response.into_tuple()?)
    }

    pub fn space(&self, name: &str) -> Result<Option<RemoteSpace>, Error> {
        Ok(self.inner.lookup_space(name)?.map(|space_id| RemoteSpace {
            conn_inner: self.inner.clone(),
            space_id,
        }))
    }
}

pub struct RemoteSpace {
    conn_inner: Rc<ConnInner>,
    space_id: u32,
}

impl RemoteSpace {
    pub fn index(&self, name: &str) -> Result<Option<RemoteIndex>, Error> {
        Ok(self
            .conn_inner
            .lookup_index(name, self.space_id)?
            .map(|index_id| RemoteIndex {
                index_id,
                ..self.primary_key()
            }))
    }

    #[inline(always)]
    pub fn primary_key(&self) -> RemoteIndex {
        RemoteIndex {
            conn_inner: self.conn_inner.clone(),
            space_id: self.space_id,
            index_id: 0,
        }
    }

    pub fn select<K>(
        &self,
        iterator_type: IteratorType,
        key: &K,
    ) -> Result<RemoteIndexIterator, Error>
    where
        K: AsTuple,
    {
        self.primary_key().select(iterator_type, key)
    }
}

pub struct RemoteIndex {
    conn_inner: Rc<ConnInner>,
    space_id: u32,
    index_id: u32,
}

impl RemoteIndex {
    pub fn select<K>(
        &self,
        iterator_type: IteratorType,
        key: &K,
    ) -> Result<RemoteIndexIterator, Error>
    where
        K: AsTuple,
    {
        let buf = Vec::new();
        let mut cur = Cursor::new(buf);

        let sync = self.conn_inner.next_sync();
        protocol::encode_select(
            &mut cur,
            sync,
            self.space_id,
            self.index_id,
            u32::max_value(),
            0,
            iterator_type,
            key,
        )?;
        let response = self
            .conn_inner
            .communicate(&cur.into_inner(), sync, &Options::default())?;

        Ok(RemoteIndexIterator {
            inner: response.into_iter()?,
        })
    }
}

pub struct RemoteIndexIterator {
    inner: Option<protocol::ResponseIterator>,
}

impl<'a> Iterator for RemoteIndexIterator {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner {
            None => None,
            Some(ref mut inner) => inner.next_tuple(),
        }
    }
}
