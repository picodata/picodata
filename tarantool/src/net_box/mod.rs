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

use core::cell::{Cell, RefCell};
use core::time::Duration;
use std::collections::HashMap;
use std::io::{self, Cursor, Write};
use std::net::{SocketAddr, ToSocketAddrs};

pub use error::NetBoxError;
pub use options::{ConnOptions, Options};

use crate::coio::CoIOStream;
use crate::error::Error;
use crate::fiber::{set_cancellable, Cond, Fiber, Latch};
use crate::net_box::protocol::Response;
use crate::tuple::{AsTuple, Tuple};

mod error;
mod options;
mod protocol;

/// Connection to remote Tarantool server
pub struct Conn<'a> {
    addrs: Vec<SocketAddr>,
    options: ConnOptions,
    sync: Cell<u64>,
    recv_fiber: RefCell<Fiber<'a, *mut ConnSession>>,
    session: RefCell<Box<ConnSession>>,
}

#[derive(Copy, Clone, Debug)]
enum ConnState {
    Init,
    Connecting,
    Auth,
    Active,
}

struct ConnSession {
    state: ConnState,
    stream: Option<CoIOStream>,
    active_requests: HashMap<u64, RequestState>,
    send_lock: Latch,
    recv_lock: Latch,
}

struct RequestState {
    recv_cond: Cond,
    response: Option<Response>,
}

impl<'a> Conn<'a> {
    /// Create a new connection.
    ///
    /// The connection is established on demand, at the time of the first request. It can be re-established
    /// automatically after a disconnect (see [reconnect_after](struct.ConnOptions.html#structfield.reconnect_after) option).
    /// The returned conn object supports methods for making remote requests, such as select, update or delete.
    ///
    /// See also: [ConnOptions](struct.ConnOptions.html)
    pub fn new(addr: &str, options: ConnOptions) -> Result<Self, Error> {
        let mut recv_fiber = Fiber::new("_recv", &mut Conn::recv_fiber_main);
        recv_fiber.set_joinable(true);

        Ok(Conn {
            options,
            addrs: addr.to_socket_addrs()?.collect(),
            sync: Cell::new(0),
            recv_fiber: RefCell::new(recv_fiber),
            session: RefCell::new(Box::new(ConnSession {
                state: ConnState::Init,
                stream: None,
                send_lock: Latch::new(),
                recv_lock: Latch::new(),
                active_requests: Default::default(),
            })),
        })
    }

    /// Wait for connection to be active or closed.
    pub fn wait_connected(&self, _timeout: Option<Duration>) -> Result<(), Error> {
        unimplemented!()
    }

    /// Show whether connection is active or closed.
    pub fn is_connected(&self) -> bool {
        unimplemented!()
    }

    /// Close a connection.
    pub fn close(self) {
        unimplemented!()
    }

    /// Execute a PING command.
    ///
    /// - `options` – the supported option is `timeout`
    pub fn ping(&self, _options: &Options) -> Result<(), Error> {
        let buf = Vec::new();
        let mut cur = Cursor::new(buf);

        let sync = self.next_sync();
        protocol::encode_ping(&mut cur, sync)?;
        self.communicate(&cur.into_inner(), sync)?;
        // TBD
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
        _options: &Options,
    ) -> Result<Option<Tuple>, Error>
    where
        T: AsTuple,
    {
        let buf = Vec::new();
        let mut cur = Cursor::new(buf);

        let sync = self.next_sync();
        protocol::encode_call(&mut cur, sync, function_name, args)?;
        self.communicate(&cur.into_inner(), sync)?;
        // TBD
        Ok(None)
    }

    fn communicate(&self, request: &Vec<u8>, sync: u64) -> Result<(), Error> {
        let state = self.session.borrow().state;
        if let ConnState::Init = state {
            self.connect()?;
        }

        self.send_request(request, sync)?;
        self.recv_response(sync)?;
        Ok(())
    }

    fn connect(&self) -> Result<(), Error> {
        self.session.borrow_mut().state = ConnState::Connecting;
        let mut stream = CoIOStream::connect(&*self.addrs)?;
        let salt = protocol::decode_greeting(&mut stream)?;

        if !self.options.user.is_empty() {
            self.session.borrow_mut().state = ConnState::Auth;
            self.auth(&mut stream, &salt)?;
        }

        {
            let mut session = self.session.borrow_mut();
            session.state = ConnState::Active;
            session.stream = Some(stream);
        }

        self.recv_fiber
            .borrow_mut()
            .start(&mut **self.session.borrow_mut());

        Ok(())
    }

    fn auth(&self, stream: &mut CoIOStream, salt: &Vec<u8>) -> Result<(), Error> {
        let buf = Vec::new();
        let mut cur = Cursor::new(buf);
        let sync = self.next_sync();
        protocol::encode_auth(
            &mut cur,
            self.options.user.as_str(),
            self.options.password.as_str(),
            salt,
            sync,
        )?;
        stream.write_all(&cur.into_inner())?;
        protocol::decode_response(stream)?;

        Ok(())
    }

    fn send_request(&self, data: &Vec<u8>, sync: u64) -> Result<(), io::Error> {
        let mut session = self.session.borrow_mut();
        {
            let _lock = session.recv_lock.lock();
            session.active_requests.insert(
                sync,
                RequestState {
                    recv_cond: Cond::new(),
                    response: None,
                },
            );
        }
        {
            let _lock = session.send_lock.lock();
            let stream = session.stream.as_mut().unwrap();
            stream.write_all(data)
        }
    }

    fn recv_response(&self, sync: u64) -> Result<(), Error> {
        let mut session = self.session.borrow_mut();
        session.active_requests.get(&sync).unwrap().recv_cond.wait();
        let response = {
            let _lock = session.recv_lock.lock();
            session.active_requests.remove(&sync)
        };

        let _ = response.unwrap().response.unwrap();
        // TBD

        Ok(())
    }

    fn recv_fiber_main(conn: Box<*mut ConnSession>) -> i32 {
        set_cancellable(true);

        let conn = unsafe { (*conn).as_mut() }.unwrap();
        loop {
            match protocol::decode_response(&mut conn.stream.as_mut().unwrap()) {
                Ok(response) => {
                    let _lock = conn.recv_lock.lock();
                    if let Some(request_state) =
                        conn.active_requests.get_mut(&(response.sync as u64))
                    {
                        request_state.response = Some(response);
                        request_state.recv_cond.signal();
                    }
                }

                Err(_) => {
                    break;
                }
            }
        }

        0
    }

    fn next_sync(&self) -> u64 {
        let sync = self.sync.get();
        self.sync.set(sync + 1);
        sync
    }
}

impl<'a> Drop for Conn<'a> {
    fn drop(&mut self) {
        let mut fiber = self.recv_fiber.borrow_mut();
        fiber.cancel();
        fiber.join();
    }
}
