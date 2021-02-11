use core::cell::RefCell;
use std::cell::Cell;
use std::io;
use std::io::{Cursor, Write};
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::rc::Rc;
use std::time::Duration;

use crate::coio::CoIOStream;
use crate::error::Error;
use crate::fiber::{is_cancelled, set_cancellable, sleep, time, Cond, Fiber, Latch};

use super::options::{ConnOptions, Options};
use super::protocol;
use super::recv_queue::{self, RecvQueue};
use super::schema::ConnSchema;
use super::send_queue::{self, SendQueue};

#[derive(Debug, Clone)]
enum ConnState {
    Init,
    Connecting,
    Auth,
    Active,
    Error,
    ErrorReconnect,
    Closed,
}

pub struct ConnInner {
    addrs: Vec<SocketAddr>,
    options: ConnOptions,
    state: RefCell<ConnState>,
    state_lock: Latch,
    state_change_cond: Cond,
    schema: RefCell<ConnSchema>,
    schema_version: Cell<Option<u32>>,
    schema_lock: Latch,
    session: RefCell<Option<Rc<ConnSession>>>,
    send_queue: SendQueue,
    recv_queue: RecvQueue,
    send_fiber: RefCell<Fiber<'static, Rc<ConnInner>>>,
    recv_fiber: RefCell<Fiber<'static, Rc<ConnInner>>>,
    error: RefCell<Option<io::Error>>,
}

impl ConnInner {
    pub fn new(addrs: Vec<SocketAddr>, options: ConnOptions) -> Rc<Self> {
        // init recv fiber
        let mut recv_fiber = Fiber::new("_recv_worker", &mut recv_worker);
        recv_fiber.set_joinable(true);

        // init send fiber
        let mut send_fiber = Fiber::new("_send_worker", &mut send_worker);
        send_fiber.set_joinable(true);

        // construct object
        let conn_inner = Rc::new(ConnInner {
            addrs,
            options,
            state: RefCell::new(ConnState::Init),
            state_lock: Latch::new(),
            state_change_cond: Cond::new(),
            schema: RefCell::new(Default::default()),
            schema_version: Cell::new(None),
            schema_lock: Latch::new(),
            session: RefCell::new(None),
            send_queue: SendQueue::new(1024),
            recv_queue: RecvQueue::new(1024),
            send_fiber: RefCell::new(send_fiber),
            recv_fiber: RefCell::new(recv_fiber),
            error: RefCell::new(None),
        });

        // start send/recv fibers
        conn_inner.send_fiber.borrow_mut().start(conn_inner.clone());
        conn_inner.recv_fiber.borrow_mut().start(conn_inner.clone());

        conn_inner
    }

    pub fn is_connected(&self) -> bool {
        matches!(self.state(), ConnState::Active)
    }

    pub fn wait_connected(&self, timeout: Option<Duration>) -> Result<bool, Error> {
        let begin_ts = time();
        loop {
            let state = self.state();
            match state {
                ConnState::Init => {
                    self.init()?;
                }
                ConnState::Active => return Ok(true),
                ConnState::Closed => return Ok(false),
                _ => {
                    let timeout = match timeout {
                        None => None,
                        Some(timeout) => {
                            timeout.checked_sub(Duration::from_secs_f64(time() - begin_ts))
                        }
                    };

                    if !self.wait_state_changed(timeout) {
                        return Err(io::Error::from(io::ErrorKind::TimedOut).into());
                    }
                }
            };
        }
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
                    self.init()?;
                }
                ConnState::Active => {
                    return match self.send_queue.send(request_producer) {
                        Ok(sync) => self
                            .recv_queue
                            .recv(sync, response_consumer, options)
                            .and_then(|response| {
                                self.schema_version
                                    .set(Some(response.header.schema_version));
                                Ok(response.payload)
                            }),
                        Err(err) => Err(self.handle_error(err.into()).err().unwrap()),
                    };
                }
                ConnState::Error => self.disconnect(),
                ConnState::ErrorReconnect => self.reconnect_or_fail()?,
                ConnState::Closed => {
                    return Err(io::Error::from(io::ErrorKind::NotConnected).into())
                }
                _ => {
                    self.wait_state_changed(None);
                }
            };
        }
    }

    pub fn lookup_space(&self, name: &str) -> Result<Option<u32>, Error> {
        self.sync_schema()?;
        Ok(self.schema.borrow().lookup_space(name))
    }

    pub fn lookup_index(&self, name: &str, space_id: u32) -> Result<Option<u32>, Error> {
        self.sync_schema()?;
        Ok(self.schema.borrow().lookup_index(name, space_id))
    }

    pub fn close(&self) {
        self.disconnect();

        let mut send_fiber = self.send_fiber.borrow_mut();
        send_fiber.cancel();
        send_fiber.join();

        let mut recv_fiber = self.recv_fiber.borrow_mut();
        recv_fiber.cancel();
        recv_fiber.join();
    }

    fn init(&self) -> Result<(), Error> {
        match self.connect() {
            Ok(_) => (),
            Err(err) => {
                return self.handle_error(err);
            }
        };

        Ok(())
    }

    fn connect(&self) -> Result<(), Error> {
        self.update_state(ConnState::Connecting);

        // connect
        let connect_timeout = self.options.connect_timeout;
        let mut stream = if connect_timeout.subsec_nanos() == 0 && connect_timeout.as_secs() == 0 {
            CoIOStream::connect(&*self.addrs)?
        } else {
            CoIOStream::connect_timeout(self.addrs.first().unwrap(), connect_timeout)?
        };

        // receive greeting msg
        let salt = protocol::decode_greeting(&mut stream)?;

        // auth if required
        if !self.options.user.is_empty() {
            self.update_state(ConnState::Auth);
            self.auth(&mut stream, &salt)?;
        }

        // if ok: save stream to session
        self.session
            .replace(Some(Rc::new(ConnSession::new(stream)?)));
        self.update_state(ConnState::Active);
        Ok(())
    }

    fn auth(&self, stream: &mut CoIOStream, salt: &Vec<u8>) -> Result<(), Error> {
        let buf = Vec::new();
        let mut cur = Cursor::new(buf);

        // send auth request
        let sync = self.send_queue.next_sync();
        send_queue::write_to_buffer(&mut cur, sync, |buf, sync| {
            protocol::encode_auth(
                buf,
                self.options.user.as_str(),
                self.options.password.as_str(),
                salt,
                sync,
            )
        })?;
        stream.write(cur.get_ref())?;

        // handle response
        let response_len = rmp::decode::read_u32(stream)?;
        recv_queue::recv_message(stream, &mut cur, response_len as usize)?;
        let header = protocol::decode_header(&mut cur)?;
        if header.status_code != 0 {
            return Err(protocol::decode_error(stream)?.into());
        }

        Ok(())
    }

    fn sync_schema(&self) -> Result<(), Error> {
        self.wait_connected(Some(self.options.connect_timeout))?;
        let _lock = self.schema_lock.lock();

        let is_schema_outdated = match self.schema_version.get() {
            None => true,
            Some(actual_version) => self.schema.borrow().version < actual_version,
        };

        if is_schema_outdated {
            self.schema.borrow_mut().update(self)
        } else {
            Ok(())
        }
    }

    fn state(&self) -> ConnState {
        let _lock = self.state_lock.lock();
        self.state.borrow().clone()
    }

    fn get_session(&self) -> Rc<ConnSession> {
        let _lock = self.state_lock.lock();
        match self.state.borrow().clone() {
            ConnState::Active => self.session.borrow().as_ref().unwrap().clone(),
            _ => panic!("Invalid sate"),
        }
    }

    fn update_state(&self, state: ConnState) {
        {
            let _lock = self.state_lock.lock();
            self.state.replace(state)
        };
        self.state_change_cond.broadcast();
    }

    fn wait_state_changed(&self, timeout: Option<Duration>) -> bool {
        match timeout {
            Some(timeout) => self.state_change_cond.wait_timeout(timeout),
            None => self.state_change_cond.wait(),
        }
    }

    fn handle_error(&self, err: Error) -> Result<(), Error> {
        match err {
            Error::IO(err) => {
                self.error.replace(Some(err));
                self.update_state(ConnState::ErrorReconnect);
                Ok(())
            }
            err => {
                self.update_state(ConnState::Error);
                Err(err)
            }
        }
    }

    fn reconnect_or_fail(&self) -> Result<(), Error> {
        let error = self.error.take().unwrap();
        let reconnect_after = self.options.reconnect_after;
        if reconnect_after.as_secs() == 0 && reconnect_after.subsec_nanos() == 0 {
            self.update_state(ConnState::Error);
            return Err(error.into());
        } else {
            sleep(reconnect_after.as_secs_f64());
            match self.connect() {
                Ok(_) => {}
                Err(err) => {
                    self.handle_error(err)?;
                }
            }
        }
        Ok(())
    }

    fn disconnect(&self) {
        self.session.replace(None);
        self.update_state(ConnState::Closed);
        self.send_queue.close();
        self.recv_fiber.borrow().wakeup();
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

    loop {
        if is_cancelled() {
            return 0;
        }

        match conn.state() {
            ConnState::Active => {
                let session = conn.get_session();
                let mut stream = session.secondary_stream.borrow_mut();
                conn.send_queue.flush_to_stream(&mut *stream);
            }
            ConnState::Closed => return 0,
            _ => {
                conn.wait_state_changed(None);
            }
        }
    }
}

fn recv_worker(conn: Box<Rc<ConnInner>>) -> i32 {
    set_cancellable(true);
    let conn = *conn;

    loop {
        if is_cancelled() {
            return 0;
        }

        match conn.state() {
            ConnState::Active => {
                let session = conn.get_session();
                let mut stream = session.primary_stream.borrow_mut();
                conn.recv_queue.pull(&mut *stream);
            }
            ConnState::Closed => return 0,
            _ => {
                conn.wait_state_changed(None);
            }
        }
    }
}
