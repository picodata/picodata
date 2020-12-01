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

pub use options::{ConnOptions, Options};
pub(crate) use protocol::ResponseError;

use crate::coio::CoIOStream;
use crate::error::Error;
use crate::fiber::{is_cancelled, set_cancellable, sleep, Cond, Fiber, Latch};
use crate::tuple::{AsTuple, Tuple};

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

#[derive(Copy, Clone, PartialEq, Debug)]
enum ConnState {
    Init,
    Connecting,
    Auth,
    Active,
    Error,
    ErrorReconnect,
    Closed,
}

struct ConnSession {
    state: ConnState,
    state_change_cond: Cond,
    stream: Option<CoIOStream>,
    active_requests: HashMap<u64, RequestState>,
    send_lock: Latch,
    recv_lock: Latch,
    recv_error: Option<Error>,
    last_io_error: Option<io::Error>,
}

impl ConnSession {
    fn update_state(&mut self, state: ConnState) {
        if self.state != state {
            self.state = state;
            self.state_change_cond.signal();
        }
    }
}

struct RequestState {
    recv_cond: Cond,
    response: Option<protocol::Response>,
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
                state_change_cond: Cond::new(),
                stream: None,
                send_lock: Latch::new(),
                recv_lock: Latch::new(),
                active_requests: Default::default(),
                recv_error: None,
                last_io_error: None,
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
    pub fn ping(&self, options: &Options) -> Result<(), Error> {
        let buf = Vec::new();
        let mut cur = Cursor::new(buf);

        let sync = self.next_sync();
        protocol::encode_ping(&mut cur, sync)?;
        self.communicate(&cur.into_inner(), sync, options)?;
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

        let sync = self.next_sync();
        protocol::encode_call(&mut cur, sync, function_name, args)?;
        let response = self.communicate(&cur.into_inner(), sync, options)?;
        Ok(response.into_tuple()?)
    }

    fn communicate(
        &self,
        request: &Vec<u8>,
        sync: u64,
        options: &Options,
    ) -> Result<protocol::Response, Error> {
        loop {
            let state = self.session.borrow().state;
            match state {
                ConnState::Init => {
                    // start recv fiber
                    self.recv_fiber
                        .borrow_mut()
                        .start(&mut **self.session.borrow_mut());

                    // try to connect
                    if let Err(err) = self.connect() {
                        self.handle_error(err)?;
                    }
                }
                ConnState::Active => {
                    if let Err(err) = self.send_request(request, sync, options) {
                        self.handle_error(err.into())?;
                    }
                    return Ok(self.recv_response(sync, options)?.unwrap());
                }
                ConnState::Error => self.disconnect(),
                ConnState::ErrorReconnect => self.reconnect_or_fail()?,
                ConnState::Closed => return Err(io::Error::from(io::ErrorKind::BrokenPipe).into()),
                _ => {
                    self.session.borrow().state_change_cond.wait();
                }
            };
        }
    }

    fn connect(&self) -> Result<(), Error> {
        self.update_state(ConnState::Connecting);

        // connect
        let connect_timeout = self.options.connect_timeout;
        let mut stream = if connect_timeout.subsec_millis() == 0 && connect_timeout.as_secs() == 0 {
            CoIOStream::connect(&*self.addrs)?
        } else {
            CoIOStream::connect_timeout(self.addrs.first().unwrap(), connect_timeout)?
        };

        // recv greeting msg
        let salt = protocol::decode_greeting(&mut stream)?;

        // auth if required
        if !self.options.user.is_empty() {
            self.update_state(ConnState::Auth);
            self.auth(&mut stream, &salt)?;
        }

        // if ok: save stream to session
        {
            let mut session = self.session.borrow_mut();
            session.stream = Some(stream);
            session.last_io_error = None;
            session.update_state(ConnState::Active);
        }

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

    fn send_request(
        &self,
        data: &Vec<u8>,
        sync: u64,
        options: &Options,
    ) -> Result<usize, io::Error> {
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
            stream.write_with_timeout(data, options.timeout)
        }
    }

    fn recv_response(
        &self,
        sync: u64,
        options: &Options,
    ) -> Result<Option<protocol::Response>, Error> {
        let mut session = self.session.borrow_mut();
        Ok(
            if let Some(request_state) = session.active_requests.get(&sync) {
                match options.timeout {
                    None => request_state.recv_cond.wait(),
                    Some(timeout) => request_state.recv_cond.wait_timeout(timeout),
                };
                {
                    let _lock = session.recv_lock.lock();
                    session.active_requests.remove(&sync)
                }
                .unwrap()
                .response
            } else {
                None
            },
        )
    }

    fn recv_fiber_main(conn: Box<*mut ConnSession>) -> i32 {
        set_cancellable(true);

        let session = unsafe { (*conn).as_mut() }.unwrap();
        loop {
            if is_cancelled() {
                return 0;
            }

            match session.state {
                ConnState::Active => {
                    match protocol::decode_response(&mut session.stream.as_mut().unwrap()) {
                        Ok(response) => {
                            match session.active_requests.get_mut(&(response.sync as u64)) {
                                None => continue,
                                Some(request_state) => {
                                    let _lock = session.recv_lock.lock();
                                    request_state.response = Some(response);
                                    request_state.recv_cond.signal();
                                }
                            }
                        }
                        Err(err) => {
                            if is_cancelled() {
                                return 0;
                            }

                            session.recv_error = Some(err);
                            session.update_state(ConnState::Error);
                        }
                    };
                }
                ConnState::Closed => return 0,
                _ => {
                    session.state_change_cond.wait();
                }
            }
        }
    }

    fn handle_error(&self, err: Error) -> Result<(), Error> {
        let mut session = self.session.borrow_mut();
        match err {
            Error::IO(err) => {
                session.stream = None;
                session.last_io_error = Some(err);
                session.update_state(ConnState::ErrorReconnect);
                Ok(())
            }
            err => {
                session.update_state(ConnState::Error);
                Err(err)
            }
        }
    }

    fn reconnect_or_fail(&self) -> Result<(), Error> {
        let reconnect_after = self.options.reconnect_after;
        if reconnect_after.as_secs() == 0 && reconnect_after.subsec_nanos() == 0 {
            self.update_state(ConnState::Error);
        } else {
            sleep(reconnect_after.as_secs_f64());
            if let Err(err) = self.connect() {
                self.handle_error(err)?;
            }
        }
        Ok(())
    }

    fn disconnect(&self) {
        let mut session = self.session.borrow_mut();
        session.stream = None;
        session.update_state(ConnState::Closed);
    }

    fn update_state(&self, state: ConnState) {
        self.session.borrow_mut().update_state(state);
    }

    fn next_sync(&self) -> u64 {
        let sync = self.sync.get();
        self.sync.set(sync + 1);
        sync
    }
}

impl<'a> Drop for Conn<'a> {
    fn drop(&mut self) {
        self.disconnect();
        let mut fiber = self.recv_fiber.borrow_mut();
        fiber.cancel();
        fiber.join();
    }
}
