use core::cell::RefCell;
use std::cell::Cell;
use std::io::{self, Cursor, Read, Write};
use std::net::SocketAddr;
use std::rc::{Rc, Weak};
use std::time::Duration;

use crate::clock::INFINITY;
use crate::coio::CoIOStream;
use crate::error::Error;
use crate::fiber;
use crate::fiber::is_cancelled;
use crate::fiber::Cond;
use crate::net_box::stream::ConnStream;
use crate::time::Instant;
use crate::tuple::Decode;
use crate::unwrap_or;

use super::options::{ConnOptions, ConnTriggers, Options};
use super::promise::Promise;
use super::protocol::{self, Header, Request};
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
    schema_version: Cell<Option<u64>>,
    stream: RefCell<Option<ConnStream>>,
    send_queue: SendQueue,
    recv_queue: RecvQueue,
    send_worker_join_handle: Cell<Option<fiber::JoinHandle<'static, ()>>>,
    receive_worker_join_handle: Cell<Option<fiber::JoinHandle<'static, ()>>>,
    triggers: RefCell<Option<Rc<dyn ConnTriggers>>>,
    error: RefCell<Option<io::Error>>,
}

impl ConnInner {
    /// Contructs a new `ConnInner` instance. Does not actually connect to
    /// anything, only initializes the internal data structures and worker
    /// fibers.
    ///
    /// Returns an error if starting a worker fiber failed.
    #[inline(always)]
    #[track_caller]
    pub fn new(
        addrs: Vec<SocketAddr>,
        options: ConnOptions,
        triggers: Option<Rc<dyn ConnTriggers>>,
    ) -> Result<Rc<Self>, Error> {
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

            send_worker_join_handle: Cell::new(None),
            receive_worker_join_handle: Cell::new(None),

            triggers: RefCell::new(triggers),
            error: RefCell::new(None),
            addrs,
            options,
        });

        // init recv fiber
        let weak_conn = Rc::downgrade(&conn_inner);
        let jh = fiber::Builder::new()
            .name("_recv_worker")
            .func(|| recv_worker(weak_conn))
            // This yields but than almost immediately return control back to us.
            .start()?;
        conn_inner.receive_worker_join_handle.set(Some(jh));

        // init send fiber
        let weak_conn = Rc::downgrade(&conn_inner);
        let jh = fiber::Builder::new()
            .name("_send_worker")
            .func(|| send_worker(weak_conn))
            // This yields but than almost immediately return control back to us.
            .start()?;
        conn_inner.send_worker_join_handle.set(Some(jh));

        Ok(conn_inner)
    }

    pub fn is_connected(&self) -> bool {
        matches!(self.state.get(), ConnState::Active)
    }

    pub fn wait_connected(self: &Rc<Self>, timeout: Option<Duration>) -> Result<bool, Error> {
        let timeout = timeout.unwrap_or(INFINITY);
        let deadline = fiber::clock().saturating_add(timeout);
        loop {
            let state = self.state.get();
            match state {
                ConnState::Init => {
                    self.init()?;
                }
                ConnState::Active => return Ok(true),
                ConnState::Closed => return Ok(false),
                _ => {
                    if !self.wait_state_changed(Some(deadline)) {
                        return Err(io::Error::from(io::ErrorKind::TimedOut).into());
                    }
                }
            };
        }
    }

    pub fn request<Fp, Fc, R>(
        self: &Rc<Self>,
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
                        Ok(sync) => {
                            self.recv_queue
                                .recv(sync, response_consumer, options)
                                .map(|response| {
                                    self.schema_version
                                        .set(Some(response.header.schema_version));
                                    response.payload
                                })
                        }
                        Err(err) => Err(self.handle_error(err).err().unwrap()),
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

    pub(crate) fn request_async<I, O>(self: &Rc<Self>, request: I) -> crate::Result<Promise<O>>
    where
        I: Request,
        O: for<'de> Decode<'de> + 'static,
    {
        loop {
            match self.state.get() {
                ConnState::Init => {
                    self.init()?;
                }
                ConnState::Active => {
                    let sync = self
                        .send_queue
                        .send(protocol::request_producer(request))
                        .map_err(|err| self.handle_error(err).err().unwrap())?;
                    let promise = Promise::new(Rc::downgrade(self));
                    self.recv_queue.add_consumer(sync, promise.downgrade());
                    return Ok(promise);
                }
                ConnState::Error => self.disconnect(),
                ConnState::ErrorReconnect => self.reconnect_or_fail()?,
                ConnState::Closed => {
                    return Err(io::Error::from(io::ErrorKind::NotConnected).into())
                }
                _ => {
                    self.wait_state_changed(None);
                }
            }
        }
    }

    pub fn lookup_space(self: &Rc<Self>, name: &str) -> Result<Option<u32>, Error> {
        self.refresh_schema()?;
        Ok(self.schema.lookup_space(name))
    }

    pub fn lookup_index(self: &Rc<Self>, name: &str, space_id: u32) -> Result<Option<u32>, Error> {
        self.refresh_schema()?;
        Ok(self.schema.lookup_index(name, space_id))
    }

    pub fn close(self: &Rc<Self>) {
        let state = self.state.get();
        if matches!(state, ConnState::Connecting) || matches!(state, ConnState::Auth) {
            let _ = self.wait_connected(None);
        }

        if !matches!(self.state.get(), ConnState::Closed) {
            self.disconnect();

            if let Some(jh) = self.send_worker_join_handle.take() {
                jh.cancel();
                jh.join();
            }

            if let Some(jh) = self.receive_worker_join_handle.take() {
                jh.cancel();
                jh.join();
            }
        }
    }

    fn init(self: &Rc<Self>) -> Result<(), Error> {
        match self.connect() {
            Ok(_) => (),
            Err(err) => {
                return self.handle_error(err);
            }
        };

        Ok(())
    }

    fn connect(self: &Rc<Self>) -> Result<(), Error> {
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
            triggers.on_connect(&Conn::downgrade(self.clone()))?;
        }

        Ok(())
    }

    fn auth(&self, stream: &mut CoIOStream, salt: &[u8]) -> Result<(), Error> {
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
        stream.write_all(cur.get_ref())?;

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

    fn refresh_schema(self: &Rc<Self>) -> Result<(), Error> {
        self.wait_connected(Some(self.options.connect_timeout))?;

        // synchronize
        if self.schema.refresh(self, self.schema_version.get())? {
            // call trigger
            if let Some(triggers) = self.triggers.borrow().as_ref() {
                triggers.on_schema_reload(&Conn::downgrade(self.clone()));
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn update_state(&self, state: ConnState) {
        self.state.set(state);
        self.state_change_cond.broadcast();
    }

    #[inline(always)]
    fn wait_state_changed(&self, deadline: Option<Instant>) -> bool {
        if let Some(deadline) = deadline {
            self.state_change_cond.wait_deadline(deadline)
        } else {
            self.state_change_cond.wait()
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

    fn reconnect_or_fail(self: &Rc<Self>) -> Result<(), Error> {
        if matches!(self.state.get(), ConnState::Closed) {
            return Ok(());
        }

        let error = self.error.replace(None).unwrap();
        let reconnect_after = self.options.reconnect_after;
        if reconnect_after.as_secs() == 0 && reconnect_after.subsec_nanos() == 0 {
            self.update_state(ConnState::Error);
            return Err(error.into());
        } else {
            fiber::sleep(reconnect_after);
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
                let jh_ptr = self.receive_worker_join_handle.as_ptr();
                // SAFETY: safe as long as this is only called from tx thread.
                if let Some(jh) = unsafe { &*jh_ptr } {
                    jh.wakeup();
                }
            }
        }

        self.recv_queue.close();
        self.send_queue.close();
        self.stream.replace(None);

        if let Some(triggers) = self.triggers.replace(None) {
            triggers.on_disconnect();
        }
    }
}

fn send_worker(weak_conn: Weak<ConnInner>) {
    loop {
        if is_cancelled() {
            return;
        }

        let conn = unwrap_or!(weak_conn.upgrade(), return);

        match conn.state.get() {
            ConnState::Active => {
                let mut writer = conn.stream.borrow().as_ref().unwrap().acquire_writer();
                if let Err(e) = conn.send_queue.flush_to_stream(&mut writer) {
                    if is_cancelled() {
                        return;
                    }
                    conn.handle_error(e.into()).unwrap();
                }
            }
            ConnState::Closed => return,
            _ => {
                conn.wait_state_changed(None);
            }
        }
    }
}

fn recv_worker(weak_conn: Weak<ConnInner>) {
    loop {
        if is_cancelled() {
            return;
        }

        let conn = unwrap_or!(weak_conn.upgrade(), return);

        match conn.state.get() {
            ConnState::Active => {
                let result = {
                    let mut reader = conn.stream.borrow().as_ref().unwrap().acquire_reader();
                    conn.recv_queue.pull(&mut reader)
                };
                match result {
                    Err(e) => {
                        if is_cancelled() {
                            return;
                        }
                        conn.handle_error(e).unwrap();
                    }
                    Ok(is_data_pulled) => {
                        if !is_data_pulled && conn.is_connected() {
                            conn.disconnect();
                        }
                    }
                }
            }
            ConnState::Closed => return,
            _ => {
                conn.wait_state_changed(None);
            }
        }
    }
}
