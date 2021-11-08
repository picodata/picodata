use core::cell::RefCell;
use std::cell::Cell;
use std::io::{self, Cursor, Read, Write};
use std::net::SocketAddr;
use std::rc::{Rc, Weak};
use std::time::Duration;

use crate::coio::CoIOStream;
use crate::error::Error;
use crate::fiber::{is_cancelled, set_cancellable, sleep, time, Cond, Fiber};
use crate::net_box::stream::ConnStream;

use super::options::{ConnOptions, ConnTriggers, Options};
use super::protocol::{self, Header};
use super::recv_queue::RecvQueue;
use super::schema::ConnSchema;
use super::send_queue::{self, SendQueue};
use super::Conn;

#[derive(Debug, Copy, Clone)]
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
    state: Cell<ConnState>,
    state_change_cond: Cond,
    schema: Rc<ConnSchema>,
    schema_version: Cell<Option<u32>>,
    stream: RefCell<Option<ConnStream>>,
    send_queue: SendQueue,
    recv_queue: RecvQueue,
    send_fiber: RefCell<Fiber<'static, Rc<ConnInner>>>,
    recv_fiber: RefCell<Fiber<'static, Rc<ConnInner>>>,
    triggers: RefCell<Option<ConnTriggersWrapper>>,
    error: RefCell<Option<io::Error>>,
}

impl ConnInner {
    pub fn new(
        addrs: Vec<SocketAddr>,
        options: ConnOptions,
        triggers: Option<Rc<dyn ConnTriggers>>,
    ) -> Rc<Self> {
        // init recv fiber
        let mut recv_fiber = Fiber::new("_recv_worker", &mut recv_worker);
        recv_fiber.set_joinable(true);

        // init send fiber
        let mut send_fiber = Fiber::new("_send_worker", &mut send_worker);
        send_fiber.set_joinable(true);

        // construct object
        let conn_inner = Rc::new(ConnInner {
            state: Cell::new(ConnState::Init),
            state_change_cond: Cond::new(),
            schema: ConnSchema::acquire(&addrs),
            schema_version: Cell::new(None),
            stream: RefCell::new(None),
            send_queue: SendQueue::new(
                options.send_buffer_size,
                options.send_buffer_limit,
                options.send_buffer_flush_interval,
            ),
            recv_queue: RecvQueue::new(options.recv_buffer_size),
            send_fiber: RefCell::new(send_fiber),
            recv_fiber: RefCell::new(recv_fiber),
            triggers: RefCell::new(None),
            error: RefCell::new(None),
            addrs,
            options,
        });

        // setup triggers
        if let Some(triggers) = triggers {
            conn_inner.triggers.replace(Some(ConnTriggersWrapper {
                callbacks: triggers,
                self_ref: Rc::downgrade(&conn_inner),
            }));
        }

        // start send/recv fibers
        conn_inner.send_fiber.borrow_mut().start(conn_inner.clone());
        conn_inner.recv_fiber.borrow_mut().start(conn_inner.clone());

        conn_inner
    }

    pub fn is_connected(&self) -> bool {
        matches!(self.state.get(), ConnState::Active)
    }

    pub fn wait_connected(&self, timeout: Option<Duration>) -> Result<bool, Error> {
        let begin_ts = time();
        loop {
            let state = self.state.get();
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
        Fc: FnOnce(&mut Cursor<Vec<u8>>, &Header) -> Result<R, Error>,
    {
        loop {
            let state = self.state.get();
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
        self.refresh_schema()?;
        Ok(self.schema.lookup_space(name))
    }

    pub fn lookup_index(&self, name: &str, space_id: u32) -> Result<Option<u32>, Error> {
        self.refresh_schema()?;
        Ok(self.schema.lookup_index(name, space_id))
    }

    pub fn close(&self) {
        let state = self.state.get();
        if matches!(state, ConnState::Connecting) || matches!(state, ConnState::Auth) {
            let _ = self.wait_connected(None);
        }

        if !matches!(self.state.get(), ConnState::Closed) {
            self.disconnect();

            let mut send_fiber = self.send_fiber.borrow_mut();
            send_fiber.cancel();
            send_fiber.join();

            let mut recv_fiber = self.recv_fiber.borrow_mut();
            recv_fiber.cancel();
            recv_fiber.join();
        }
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

        // if ok: put stream to result + set state to active
        self.stream.replace(Some(ConnStream::new(stream)?));
        self.update_state(ConnState::Active);

        // call trigger (if available)
        if let Some(triggers) = self.triggers.borrow().as_ref() {
            triggers.callbacks.on_connect(&Conn {
                inner: triggers.self_ref.upgrade().unwrap(),
                is_master: false,
            })?;
        }

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
        {
            let buffer = cur.get_mut();
            buffer.clear();
            buffer.reserve(response_len as usize);
            stream.take(response_len as u64).read_to_end(buffer)?;
            cur.set_position(0);
        }

        let header = protocol::decode_header(&mut cur)?;
        if header.status_code != 0 {
            return Err(protocol::decode_error(stream)?.into());
        }

        Ok(())
    }

    fn refresh_schema(&self) -> Result<(), Error> {
        self.wait_connected(Some(self.options.connect_timeout))?;

        // synchronize
        if self.schema.refresh(self, self.schema_version.get())? {
            // call trigger
            if let Some(triggers) = self.triggers.borrow().as_ref() {
                triggers.callbacks.on_schema_reload(&Conn {
                    inner: triggers.self_ref.upgrade().unwrap(),
                    is_master: false,
                });
            }
        }
        Ok(())
    }

    fn update_state(&self, state: ConnState) {
        self.state.set(state);
        self.state_change_cond.broadcast();
    }

    fn wait_state_changed(&self, timeout: Option<Duration>) -> bool {
        match timeout {
            Some(timeout) => self.state_change_cond.wait_timeout(timeout),
            None => self.state_change_cond.wait(),
        }
    }

    fn handle_error(&self, err: Error) -> Result<(), Error> {
        if matches!(self.state.get(), ConnState::Closed) {
            return Ok(());
        }

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
        if matches!(self.state.get(), ConnState::Closed) {
            return Ok(());
        }

        let error = self.error.replace(None).unwrap();
        let reconnect_after = self.options.reconnect_after;
        if reconnect_after.as_secs() == 0 && reconnect_after.subsec_nanos() == 0 {
            self.update_state(ConnState::Error);
            return Err(error.into());
        } else {
            sleep(reconnect_after);
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
        if matches!(self.state.get(), ConnState::Closed) {
            return;
        }

        self.update_state(ConnState::Closed);
        if let Some(stream) = self.stream.borrow().as_ref() {
            if stream.is_reader_acquired() {
                self.recv_fiber.borrow().wakeup();
            }
        }

        self.recv_queue.close();
        self.send_queue.close();
        self.stream.replace(None);

        if let Some(triggers) = self.triggers.replace(None) {
            triggers.callbacks.on_disconnect();
        }
    }
}

struct ConnTriggersWrapper {
    callbacks: Rc<dyn ConnTriggers>,
    self_ref: Weak<ConnInner>,
}

fn send_worker(conn: Box<Rc<ConnInner>>) -> i32 {
    set_cancellable(true);
    let conn = *conn;

    loop {
        if is_cancelled() {
            return 0;
        }

        match conn.state.get() {
            ConnState::Active => {
                let mut writer = conn.stream.borrow().as_ref().unwrap().acquire_writer();
                if let Err(e) = conn.send_queue.flush_to_stream(&mut writer) {
                    if is_cancelled() {
                        return 0;
                    }
                    conn.handle_error(e.into()).unwrap();
                }
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

        match conn.state.get() {
            ConnState::Active => {
                let result = {
                    let mut reader = conn.stream.borrow().as_ref().unwrap().acquire_reader();
                    conn.recv_queue.pull(&mut reader)
                };
                match result {
                    Err(e) => {
                        if is_cancelled() {
                            return 0;
                        }
                        conn.handle_error(e).unwrap();
                    }
                    Ok(is_data_pulled) => {
                        if !is_data_pulled {
                            if conn.is_connected() {
                                conn.disconnect();
                            }
                        }
                    }
                }
            }
            ConnState::Closed => return 0,
            _ => {
                conn.wait_state_changed(None);
            }
        }
    }
}
