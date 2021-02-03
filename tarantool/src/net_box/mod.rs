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

use core::cell::{Cell, RefCell};
use core::time::Duration;
use std::io;
use std::io::Cursor;

use std::net::{SocketAddr, ToSocketAddrs};
use std::os::unix::io::AsRawFd;
use std::rc::{Rc, Weak};

pub use index::{RemoteIndex, RemoteIndexIterator};
pub use options::{ConnOptions, ConnTriggers, Options};
pub(crate) use protocol::ResponseError;
pub use space::RemoteSpace;

use crate::coio::CoIOStream;
use crate::error::Error;
use crate::fiber::{is_cancelled, set_cancellable, Cond, Fiber};
use crate::net_box::protocol::encode_ping;
use crate::net_box::recv_queue::RecvQueue;
use crate::net_box::send_queue::SendQueue;
use crate::tuple::{AsTuple, Tuple};

mod index;
mod inner;
mod options;
mod protocol;
mod recv_queue;
mod send_queue;
mod space;

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
    pub fn new(addr: &str, options: ConnOptions) -> Result<Self, Error> {
        Ok(Conn {
            inner: ConnInner::new(addr.to_socket_addrs()?.collect(), options),
            is_master: true,
        })
    }

    /// Wait for connection to be active or closed.
    ///
    /// Returns:
    /// - `Ok(true)`: if active
    /// - `Ok(true)`: if closed
    /// - `Err(...TimedOut...)`: on timeout
    pub fn wait_connected(&self, timeout: Option<Duration>) -> Result<bool, Error> {
        unimplemented!()
    }

    /// Show whether connection is active or closed.
    pub fn is_connected(&self) -> bool {
        unimplemented!()
    }

    /// Close a connection.
    pub fn close(&self) {
        self.inner.close()
    }

    /// Execute a PING command.
    ///
    /// - `options` – the supported option is `timeout`
    pub fn ping(&self, options: &Options) -> Result<(), Error> {
        self.inner.request(encode_ping, |_| Ok(()), options)?;
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
        unimplemented!()
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
        T: AsTuple,
    {
        unimplemented!()
    }

    /// Search space by name on remote server
    pub fn space(&self, name: &str) -> Result<Option<RemoteSpace>, Error> {
        unimplemented!()
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        if self.is_master {
            self.close();
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ConnState {
    Init,
    Connecting,
    Auth,
    FetchSchema,
    Active,
    Error,
    ErrorReconnect,
    Closed,
}

struct ConnInner {
    addrs: Vec<SocketAddr>,
    options: ConnOptions,
    state: Cell<ConnState>,
    state_change_cond: Cond,
    send_queue: SendQueue,
    recv_queue: RecvQueue,
    send_fiber: RefCell<Fiber<'static, Rc<ConnInner>>>,
    recv_fiber: RefCell<Fiber<'static, Rc<ConnInner>>>,
    self_ref: RefCell<Option<Weak<ConnInner>>>,
    session: RefCell<Option<Rc<ConnSession>>>,
}

impl ConnInner {
    pub fn new(addrs: Vec<SocketAddr>, options: ConnOptions) -> Rc<Self> {
        let mut recv_fiber = Fiber::new("_recv_worker", &mut recv_worker);
        recv_fiber.set_joinable(true);

        let mut send_fiber = Fiber::new("_send_worker", &mut send_worker);
        send_fiber.set_joinable(true);

        let session = Rc::new(ConnInner {
            addrs,
            options,
            state: Cell::new(ConnState::Init),
            state_change_cond: Cond::new(),
            send_queue: SendQueue::new(1024),
            recv_queue: RecvQueue::new(1024),
            send_fiber: RefCell::new(send_fiber),
            recv_fiber: RefCell::new(recv_fiber),
            self_ref: RefCell::new(None),
            session: RefCell::new(None),
        });
        session.self_ref.replace(Some(Rc::downgrade(&session)));
        session
    }

    pub fn request<Fp, Fc, R>(
        &self,
        request_producer: Fp,
        response_consumer: Fc,
        options: &Options,
    ) -> Result<R, Error>
    where
        Fp: FnOnce(&mut Cursor<Vec<u8>>, u64) -> Result<(), Error>,
        Fc: FnOnce(&mut Cursor<Vec<u8>>) -> Result<R, Error>,
    {
        loop {
            let state = self.state();
            match state {
                ConnState::Init => {
                    self.init(self.self_ref.replace(None).unwrap())?;
                }
                ConnState::Active => {
                    return match self.send_queue.send(request_producer) {
                        Ok(sync) => self.recv_queue.recv(sync, response_consumer, options),
                        Err(err) => Err(self.handle_error(err.into()).err().unwrap()),
                    };
                }
                ConnState::Error => self.disconnect(),
                ConnState::ErrorReconnect => self.reconnect_or_fail()?,
                ConnState::Closed => {
                    return Err(io::Error::from(io::ErrorKind::NotConnected).into())
                }
                _ => {
                    self.wait_state_changed();
                }
            };
        }
    }

    pub fn close(&self) {
        self.disconnect();
        let mut fiber = self.recv_fiber.borrow_mut();
        fiber.cancel();
        fiber.join();
    }

    fn init(&self, self_ref: Weak<ConnInner>) -> Result<(), Error> {
        // try to connect
        match self.connect() {
            Ok(_) => {}
            Err(err) => {
                return self.handle_error(err);
            }
        };

        // start send/recv fibers
        {
            let self_ref = self_ref.upgrade().unwrap();
            self.send_fiber.borrow_mut().start(self_ref.clone());
            self.recv_fiber.borrow_mut().start(self_ref.clone());
        }

        Ok(())
    }

    fn connect(&self) -> Result<(), Error> {
        // let _lock = self.session_lock.lock();
        self.update_state(ConnState::Connecting);

        // connect
        let connect_timeout = self.options.connect_timeout;
        let mut stream = if connect_timeout.subsec_nanos() == 0 && connect_timeout.as_secs() == 0 {
            CoIOStream::connect(&*self.addrs)?
        } else {
            CoIOStream::connect_timeout(self.addrs.first().unwrap(), connect_timeout)?
        };

        // recv greeting msg
        let salt = protocol::decode_greeting(&mut stream)?;

        // if ok: save stream to session
        let stream = Rc::new(ConnSession::new(stream)?);
        self.session.replace(Some(stream));
        self.update_state(ConnState::Active);

        Ok(())
    }

    fn state(&self) -> ConnState {
        // let _lock = self.session_lock.lock();
        self.state.get()
    }

    fn update_state(&self, state: ConnState) {
        self.state.set(state);
        self.state_change_cond.broadcast();
    }

    fn wait_state_changed(&self) {
        self.state_change_cond.wait();
    }

    fn handle_error(&self, err: Error) -> Result<(), Error> {
        unimplemented!()
    }

    fn reconnect_or_fail(&self) -> Result<(), Error> {
        unimplemented!()
    }

    fn disconnect(&self) {
        self.update_state(ConnState::Closed);
    }
}

struct ConnSession {
    primary_stream: RefCell<CoIOStream>,
    secondary_stream: RefCell<CoIOStream>,
}

impl ConnSession {
    fn new(primary_stream: CoIOStream) -> Result<Self, Error> {
        let secondary_fd = unsafe { libc::dup(primary_stream.as_raw_fd()) };
        Ok(ConnSession {
            primary_stream: RefCell::new(primary_stream),
            secondary_stream: RefCell::new(CoIOStream::new(secondary_fd)?),
        })
    }
}

fn send_worker(conn: Box<Rc<ConnInner>>) -> i32 {
    set_cancellable(true);
    let conn = *conn;
    let session = conn.session.borrow().as_ref().unwrap().clone();

    loop {
        if is_cancelled() {
            return 0;
        }

        match conn.state() {
            ConnState::Active => {
                let mut stream = session.secondary_stream.borrow_mut();
                conn.send_queue.flush_to_stream(&mut *stream);
            }
            ConnState::Closed => return 0,
            _ => {
                conn.wait_state_changed();
            }
        }
    }
}

fn recv_worker(conn: Box<Rc<ConnInner>>) -> i32 {
    set_cancellable(true);
    let conn = *conn;
    let session = conn.session.borrow().as_ref().unwrap().clone();

    loop {
        if is_cancelled() {
            return 0;
        }

        match conn.state() {
            ConnState::Active => {
                let mut stream = session.primary_stream.borrow_mut();
                conn.recv_queue.pull(&mut *stream);
            }
            ConnState::Closed => return 0,
            _ => {
                conn.wait_state_changed();
            }
        }
    }
}
