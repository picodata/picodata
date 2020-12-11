use core::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::io;
use std::io::{Cursor, Write};
use std::net::SocketAddr;
use std::time::Duration;

use crate::coio::CoIOStream;
use crate::error::Error;
use crate::fiber::{is_cancelled, set_cancellable, sleep, time, Cond, Fiber, Latch};
use crate::index::IteratorType;
use crate::space::SystemSpace;

use super::options::{ConnOptions, Options};
use super::protocol;

pub struct ConnSession {
    state: ConnState,
    state_change_cond: Cond,
    stream: Option<CoIOStream>,
    active_requests: HashMap<u64, RequestState>,
    send_lock: Latch,
    recv_lock: Latch,
    recv_error: Option<Error>,
    last_io_error: Option<io::Error>,
    schema_version: u32,
    schema_space_ids: HashMap<String, u32>,
}

impl ConnSession {
    fn update_state(&mut self, state: ConnState) {
        if self.state != state {
            self.state = state;
            self.state_change_cond.broadcast();
        }
    }

    fn lookup_space(&self, name: &str) -> Option<u32> {
        self.schema_space_ids.get(name).map(|id| id.clone())
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
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

struct RequestState {
    recv_cond: Cond,
    response: Option<protocol::Response>,
}

pub struct ConnInner {
    addrs: Vec<SocketAddr>,
    options: ConnOptions,
    sync: Cell<u64>,
    recv_fiber: RefCell<Fiber<'static, *mut ConnSession>>,
    session: RefCell<Box<ConnSession>>,
}

impl ConnInner {
    pub fn new(addrs: Vec<SocketAddr>, options: ConnOptions) -> Self {
        let mut recv_fiber = Fiber::new("_recv", &mut recv_fiber_main);
        recv_fiber.set_joinable(true);

        ConnInner {
            options,
            addrs,
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
                schema_version: 0,
                schema_space_ids: Default::default(),
            })),
        }
    }

    pub fn wait_connected(&self, timeout: Option<Duration>) -> Result<bool, Error> {
        let begin_ts = time();
        loop {
            let state = self.state();
            return match state {
                ConnState::Init => {
                    self.init()?;
                    continue;
                }
                ConnState::Active => Ok(true),
                ConnState::Closed => Ok(false),
                _ => {
                    let timeout = match timeout {
                        None => None,
                        Some(timeout) => {
                            timeout.checked_sub(Duration::from_secs_f64(time() - begin_ts))
                        }
                    };
                    if self.wait_state(timeout) {
                        continue;
                    }

                    Err(io::Error::from(io::ErrorKind::TimedOut).into())
                }
            };
        }
    }

    pub fn communicate(
        &self,
        request: &Vec<u8>,
        sync: u64,
        options: &Options,
    ) -> Result<protocol::Response, Error> {
        loop {
            let state = self.session.borrow().state;
            match state {
                ConnState::Init => {
                    self.init()?;
                }
                ConnState::Active => {
                    if let Err(err) = self.send_request(request, sync, options) {
                        self.handle_error(err.into())?;
                    }

                    let response = self.recv_response(sync, options)?.unwrap();
                    if self.state() == ConnState::FetchSchema {
                        self.sync_schema()?;
                    }

                    return Ok(response);
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

    fn init(&self) -> Result<(), Error> {
        // try to connect
        if let Err(err) = self.connect() {
            self.handle_error(err)?;
        }

        // start recv fiber
        self.recv_fiber
            .borrow_mut()
            .start(&mut **self.session.borrow_mut());

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
        }

        // synchronise schema
        self.sync_schema()?;

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

    fn sync_schema(&self) -> Result<(), Error> {
        self.update_state(ConnState::FetchSchema);
        let buf = Vec::new();
        let mut cur = Cursor::new(buf);
        protocol::encode_select(
            &mut cur,
            self.next_sync(),
            SystemSpace::VSpace as u32,
            0,
            u32::max_value(),
            0,
            IteratorType::GT,
            &(SystemSpace::SystemIdMax as u32,),
        )?;

        let response = {
            let mut session = self.session.borrow_mut();
            let stream = session.as_mut().stream.as_mut().unwrap();
            stream.write_all(&cur.into_inner())?;
            protocol::decode_response(stream)?
        };

        let schema_version = response.schema_version;
        let mut iter = response.into_iter()?.unwrap();
        {
            let mut session = self.session.borrow_mut();
            session.schema_version = schema_version;

            let schema_space_ids = &mut session.as_mut().schema_space_ids;
            schema_space_ids.clear();

            while let Some(item) = iter.next_tuple() {
                let (id, _, name) = item.into_struct::<(u32, u32, String)>()?;
                schema_space_ids.insert(name, id);
            }
        }

        self.update_state(ConnState::Active);
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
        if let Some(request_state) = session.active_requests.get(&sync) {
            let wait_is_successful = match options.timeout {
                None => request_state.recv_cond.wait(),
                Some(timeout) => request_state.recv_cond.wait_timeout(timeout),
            };

            if wait_is_successful {
                Ok({
                    let _lock = session.recv_lock.lock();
                    session.active_requests.remove(&sync)
                }
                .unwrap()
                .response)
            } else {
                Err(io::Error::from(io::ErrorKind::TimedOut).into())
            }
        } else {
            Err(session.recv_error.take().unwrap())
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

    pub fn state(&self) -> ConnState {
        self.session.borrow().state
    }

    pub fn update_state(&self, state: ConnState) {
        self.session.borrow_mut().update_state(state);
    }

    pub fn wait_state(&self, timeout: Option<Duration>) -> bool {
        match timeout {
            None => self.session.borrow().state_change_cond.wait(),
            Some(timeout) => self
                .session
                .borrow()
                .state_change_cond
                .wait_timeout(timeout),
        }
    }

    #[inline(always)]
    pub fn lookup_space(&self, name: &str) -> Result<Option<u32>, Error> {
        self.wait_connected(Some(self.options.connect_timeout))?;
        Ok(self.session.borrow().lookup_space(name))
    }

    pub fn next_sync(&self) -> u64 {
        let sync = self.sync.get();
        self.sync.set(sync + 1);
        sync
    }
}

impl Drop for ConnInner {
    fn drop(&mut self) {
        let was_started = !matches!(self.state(), ConnState::Init);
        self.disconnect();
        if was_started {
            let mut fiber = self.recv_fiber.borrow_mut();
            fiber.cancel();
            fiber.join();
        }
    }
}

pub fn recv_fiber_main(conn: Box<*mut ConnSession>) -> i32 {
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
                        if response.schema_version != session.schema_version {
                            session.update_state(ConnState::FetchSchema);
                        }

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
