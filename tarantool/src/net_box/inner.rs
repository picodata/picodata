use core::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::io::Cursor;
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::rc::Rc;

use crate::coio::CoIOStream;
use crate::error::Error;
use crate::fiber::{is_cancelled, set_cancellable, sleep, Cond, Fiber, Latch};
use crate::net_box::protocol::Response;
use crate::net_box::recv_queue::RecvQueue;
use crate::net_box::send_queue::SendQueue;
use crate::net_box::{recv_queue, send_queue};

use super::options::{ConnOptions, Options};
use super::protocol;

#[derive(Default)]
pub struct Schema {
    version: u32,
    space_ids: HashMap<String, u32>,
    index_ids: HashMap<(u32, String), u32>,
}

impl Schema {
    fn update(
        &mut self,
        spaces_response: Response,
        indexes_response: Response,
    ) -> Result<(), Error> {
        let schema_version = spaces_response.schema_version;

        self.space_ids.clear();
        let mut iter = spaces_response.into_iter()?.unwrap();
        while let Some(item) = iter.next_tuple() {
            let (id, _, name) = item.into_struct::<(u32, u32, String)>()?;
            self.space_ids.insert(name, id);
        }

        self.index_ids.clear();
        let mut iter = indexes_response.into_iter()?.unwrap();
        while let Some(item) = iter.next_tuple() {
            let (space_id, index_id, name) = item.into_struct::<(u32, u32, String)>()?;
            self.index_ids.insert((space_id, name), index_id);
        }

        self.version = schema_version;
        Ok(())
    }

    fn lookup_space(&self, name: &str) -> Option<u32> {
        self.space_ids.get(name).map(|id| id.clone())
    }

    fn lookup_index(&self, name: &str, space_id: u32) -> Option<u32> {
        self.index_ids
            .get(&(space_id, name.to_string()))
            .map(|id| id.clone())
    }
}

#[derive(Clone)]
enum ConnState {
    Init,
    Connecting,
    Auth,
    FetchSchema,
    Active(Rc<ConnSession>),
    Error,
    ErrorReconnect(Rc<RefCell<Option<io::Error>>>),
    Closed,
}

pub struct ConnInner {
    addrs: Vec<SocketAddr>,
    options: ConnOptions,
    state: RefCell<ConnState>,
    state_lock: Latch,
    state_change_cond: Cond,
    send_queue: SendQueue,
    recv_queue: RecvQueue,
    send_fiber: RefCell<Fiber<'static, Rc<ConnInner>>>,
    recv_fiber: RefCell<Fiber<'static, Rc<ConnInner>>>,
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
            send_queue: SendQueue::new(1024),
            recv_queue: RecvQueue::new(1024),
            send_fiber: RefCell::new(send_fiber),
            recv_fiber: RefCell::new(recv_fiber),
        });

        // start send/recv fibers
        conn_inner.send_fiber.borrow_mut().start(conn_inner.clone());
        conn_inner.recv_fiber.borrow_mut().start(conn_inner.clone());

        conn_inner
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
                ConnState::Active(_) => {
                    return match self.send_queue.send(request_producer) {
                        Ok(sync) => self.recv_queue.recv(sync, response_consumer, options),
                        Err(err) => Err(self.handle_error(err.into()).err().unwrap()),
                    };
                }
                ConnState::Error => self.disconnect(),
                ConnState::ErrorReconnect(err) => self.reconnect_or_fail(err.take().unwrap())?,
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

        let mut send_fiber = self.send_fiber.borrow_mut();
        send_fiber.cancel();
        send_fiber.join();

        let mut recv_fiber = self.recv_fiber.borrow_mut();
        recv_fiber.cancel();
        recv_fiber.join();
    }

    fn init(&self) -> Result<(), Error> {
        match self.connect() {
            Ok(_) => {}
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
        let session = Rc::new(ConnSession::new(stream)?);
        self.update_state(ConnState::Active(session));
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
        });

        // handle response
        let response_len = rmp::decode::read_u32(stream)?;
        recv_queue::recv_message(stream, &mut cur, response_len as usize)?;
        protocol::decode_header(&mut cur)?;

        Ok(())
    }

    fn state(&self) -> ConnState {
        let _lock = self.state_lock.lock();
        self.state.borrow().clone()
    }

    fn update_state(&self, state: ConnState) {
        {
            let _lock = self.state_lock.lock();
            self.state.replace(state)
        };
        self.state_change_cond.broadcast();
    }

    fn wait_state_changed(&self) {
        self.state_change_cond.wait();
    }

    fn handle_error(&self, err: Error) -> Result<(), Error> {
        match err {
            Error::IO(err) => {
                self.update_state(ConnState::ErrorReconnect(Rc::new(RefCell::new(Some(err)))));
                Ok(())
            }
            err => {
                self.update_state(ConnState::Error);
                Err(err)
            }
        }
    }

    fn reconnect_or_fail(&self, error: io::Error) -> Result<(), Error> {
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
            ConnState::Active(session) => {
                let session = session.clone();
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

    loop {
        if is_cancelled() {
            return 0;
        }

        match conn.state() {
            ConnState::Active(session) => {
                let session = session.clone();
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
