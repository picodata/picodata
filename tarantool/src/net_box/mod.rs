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

use core::time::Duration;
use std::io::{self, Cursor};
use std::net::ToSocketAddrs;
use std::rc::Rc;

use inner::{ConnInner, ConnState};
pub use options::{ConnOptions, Options};
pub(crate) use protocol::ResponseError;

use crate::clock::time;
use crate::error::Error;
use crate::index::IteratorType;
use crate::space::SystemSpace;
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
        let begin_ts = time();
        loop {
            let state = self.inner.state();
            return match state {
                ConnState::Active => Ok(true),
                ConnState::Closed => Ok(false),
                _ => {
                    let timeout = match timeout {
                        None => None,
                        Some(timeout) => {
                            timeout.checked_sub(Duration::from_secs_f64(time() - begin_ts))
                        }
                    };
                    if self.inner.wait_state(timeout) {
                        continue;
                    }

                    Err(io::Error::from(io::ErrorKind::TimedOut).into())
                }
            };
        }
    }

    /// Show whether connection is active or closed.
    pub fn is_connected(&self) -> bool {
        matches!(self.inner.state(), ConnState::Active)
    }

    /// Close a connection.
    pub fn close(self) {
        unimplemented!()
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
        let space_name_idx = RemoteIndex {
            conn_inner: self.inner.clone(),
            space_id: SystemSpace::Space as u32,
            index_id: 2, // the "name" index
        };

        let space_record = space_name_idx.select(IteratorType::Eq, &(name,))?.next();
        Ok(match space_record {
            None => None,
            Some(space_record) => {
                let space_id = space_record.into_struct::<(u32,)>()?.0;
                Some(RemoteSpace {
                    conn_inner: self.inner.clone(),
                    space_id,
                })
            }
        })
    }
}

pub struct RemoteSpace {
    conn_inner: Rc<ConnInner>,
    space_id: u32,
}

impl RemoteSpace {
    pub fn select<K>(
        &self,
        iterator_type: IteratorType,
        key: &K,
    ) -> Result<RemoteIndexIterator, Error>
    where
        K: AsTuple,
    {
        let primary_key = RemoteIndex {
            conn_inner: self.conn_inner.clone(),
            space_id: self.space_id,
            index_id: 0,
        };
        primary_key.select(iterator_type, key)
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
