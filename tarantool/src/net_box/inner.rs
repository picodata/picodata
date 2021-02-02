#![allow(unused)]
use core::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::io;
use std::io::{Cursor, Write};
use std::net::SocketAddr;
use std::rc::{Rc, Weak};
use std::time::Duration;

use crate::coio::CoIOStream;
use crate::error::Error;
use crate::fiber::{is_cancelled, set_cancellable, sleep, time, Cond, Fiber, Latch};
use crate::index::IteratorType;
use crate::net_box::protocol::Response;
use crate::net_box::{Conn, ConnTriggers};
use crate::space::SystemSpace;

use super::options::{ConnOptions, Options};
use super::protocol;

pub struct Session {
    state: ConnState,
    state_change_cond: Rc<Cond>,
    stream: Option<CoIOStream>,
    active_requests: HashMap<u64, RequestState>,
    recv_error: Option<Error>,
    last_io_error: Option<io::Error>,
    schema: Schema,
}

impl Session {
    fn update_state(&mut self, state: ConnState) {
        if self.state != state {
            self.state = state;
            self.state_change_cond.broadcast();
        }
    }
}

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

struct Triggers {
    callbacks: Box<dyn ConnTriggers>,
    self_ref: Weak<ConnInner>,
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
    recv_fiber: RefCell<Fiber<'static, *mut Session>>,
    session: RefCell<Box<Session>>,
    session_lock: Latch,
    triggers: RefCell<Option<Triggers>>,
}

impl ConnInner {
    pub fn new(addrs: Vec<SocketAddr>, mut options: ConnOptions) -> Rc<Self> {
        let mut recv_fiber = Fiber::new("_recv", &mut recv_fiber_main);
        recv_fiber.set_joinable(true);

        let triggers_callbacks = options.triggers.take();

        let self_ref = Rc::new(ConnInner {
            options,
            addrs,
            sync: Cell::new(0),
            recv_fiber: RefCell::new(recv_fiber),
            session: RefCell::new(Box::new(Session {
                state: ConnState::Init,
                state_change_cond: Rc::new(Cond::new()),
                stream: None,
                active_requests: Default::default(),
                recv_error: None,
                last_io_error: None,
                schema: Default::default(),
            })),
            session_lock: Latch::new(),
            triggers: RefCell::new(None),
        });

        if let Some(callbacks) = triggers_callbacks {
            self_ref.triggers.replace(Some(Triggers {
                callbacks,
                self_ref: Rc::downgrade(&self_ref),
            }));
        }

        self_ref
    }

    pub fn wait_connected(&self, timeout: Option<Duration>) -> Result<bool, Error> {
        let begin_ts = time();
        let state_change_cond = {
            let _lock = self.session_lock.lock();
            self.session.borrow().state_change_cond.clone()
        };

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

                    let is_signalled = match timeout {
                        None => state_change_cond.wait(),
                        Some(timeout) => state_change_cond.wait_timeout(timeout),
                    };

                    if is_signalled {
                        continue;
                    } else {
                        Err(io::Error::from(io::ErrorKind::TimedOut).into())
                    }
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
        let state_change_cond = {
            let _lock = self.session_lock.lock();
            self.session.borrow().state_change_cond.clone()
        };

        loop {
            let state = self.state();
            match state {
                ConnState::Init => {
                    self.init()?;
                }
                ConnState::Active => {
                    if let Err(err) = self.send_request(request, sync, options) {
                        self.handle_error(err.into())?;
                    }

                    let response = self.recv_response(sync, options)?;
                    if self.state() == ConnState::FetchSchema {
                        self.sync_schema()?;
                    }

                    return Ok(response);
                }
                ConnState::Error => self.disconnect(),
                ConnState::ErrorReconnect => self.reconnect_or_fail()?,
                ConnState::Closed => {
                    return Err(io::Error::from(io::ErrorKind::NotConnected).into())
                }
                _ => {
                    state_change_cond.wait();
                }
            };
        }
    }

    fn init(&self) -> Result<(), Error> {
        // try to connect
        match self.connect() {
            Ok(_) => {
                self.sync_schema()?;
            }
            Err(err) => {
                self.handle_error(err)?;
            }
        }

        // start recv fiber
        self.recv_fiber
            .borrow_mut()
            .start(&mut **self.session.borrow_mut());

        Ok(())
    }

    fn connect(&self) -> Result<(), Error> {
        let _lock = self.session_lock.lock();
        let mut session = self.session.borrow_mut();
        session.update_state(ConnState::Connecting);

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
            session.update_state(ConnState::Auth);
            self.auth(&mut stream, &salt)?;
        }

        // if ok: save stream to session
        session.stream = Some(stream);
        session.last_io_error = None;
        session.update_state(ConnState::Active);

        // call trigger (if available)
        // if let Some(triggers) = self.triggers.borrow().as_ref() {
        //     triggers.callbacks.on_connect(&Conn {
        //         inner: triggers.self_ref.upgrade().unwrap(),
        //         is_master: false,
        //     })?;
        // }

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
        let _lock = self.session_lock.lock();
        let mut session = self.session.borrow_mut();

        session.update_state(ConnState::FetchSchema);
        let stream = session.stream.as_mut().unwrap();
        let spaces_response = self.fetch_schema_spaces(stream)?;
        let indexes_response = self.fetch_schema_indexes(stream)?;
        session.schema.update(spaces_response, indexes_response)?;

        // if let Some(triggers) = self.triggers.borrow().as_ref() {
        //     triggers.callbacks.on_schema_reload(&Conn {
        //         inner: triggers.self_ref.upgrade().unwrap(),
        //         is_master: false,
        //     });
        // }

        session.update_state(ConnState::Active);
        Ok(())
    }

    fn fetch_schema_spaces(&self, stream: &mut CoIOStream) -> Result<Response, Error> {
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

        stream.write_all(&cur.into_inner())?;
        Ok(protocol::decode_response(stream)?)
    }

    fn fetch_schema_indexes(&self, stream: &mut CoIOStream) -> Result<Response, Error> {
        let buf = Vec::new();
        let mut cur = Cursor::new(buf);
        protocol::encode_select(
            &mut cur,
            self.next_sync(),
            SystemSpace::VIndex as u32,
            0,
            u32::max_value(),
            0,
            IteratorType::All,
            &Vec::<()>::new(),
        )?;

        stream.write_all(&cur.into_inner())?;
        Ok(protocol::decode_response(stream)?)
    }

    fn send_request(
        &self,
        data: &Vec<u8>,
        sync: u64,
        options: &Options,
    ) -> Result<usize, io::Error> {
        let _lock = self.session_lock.lock();
        let mut session = self.session.borrow_mut();

        session.active_requests.insert(
            sync,
            RequestState {
                recv_cond: Cond::new(),
                response: None,
            },
        );

        let stream = session.stream.as_mut().unwrap();
        stream.write_with_timeout(data, options.timeout)
    }

    fn recv_response(&self, sync: u64, options: &Options) -> Result<protocol::Response, Error> {
        let _lock = self.session_lock.lock();
        let mut session = self.session.borrow_mut();

        let request_state = session
            .active_requests
            .get(&sync)
            .ok_or(io::Error::from(io::ErrorKind::TimedOut))?;

        let wait_is_completed = request_state
            .response
            .as_ref()
            .map(|_| true)
            .or_else(|| {
                Some(match options.timeout {
                    None => request_state.recv_cond.wait(),
                    Some(timeout) => request_state.recv_cond.wait_timeout(timeout),
                })
            })
            .unwrap();

        if wait_is_completed {
            Ok(session
                .active_requests
                .remove(&sync)
                .unwrap()
                .response
                .unwrap())
        } else {
            Err(io::Error::from(io::ErrorKind::TimedOut).into())
        }
    }

    fn handle_error(&self, err: Error) -> Result<(), Error> {
        let _lock = self.session_lock.lock();
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
            let _lock = self.session_lock.lock();
            let mut session = self.session.borrow_mut();
            session.update_state(ConnState::Error);

            if let Some(err) = session.last_io_error.take() {
                return Err(err.into());
            }
        } else {
            sleep(reconnect_after.as_secs_f64());
            match self.connect() {
                Ok(_) => {
                    self.sync_schema()?;
                }
                Err(err) => {
                    self.handle_error(err)?;
                }
            }
        }
        Ok(())
    }

    pub fn close(&self) {
        let fiber_is_running = !matches!(self.state(), ConnState::Init | ConnState::Closed);
        self.disconnect();
        if fiber_is_running {
            let mut fiber = self.recv_fiber.borrow_mut();
            fiber.cancel();
            fiber.join();
        }
    }

    fn disconnect(&self) {
        let _lock = self.session_lock.lock();
        let mut session = self.session.borrow_mut();
        session.stream = None;
        session.update_state(ConnState::Closed);

        if let Some(triggers) = self.triggers.borrow().as_ref() {
            triggers.callbacks.on_disconnect();
        }
    }

    pub fn state(&self) -> ConnState {
        let _lock = self.session_lock.lock();
        self.session.borrow().state
    }

    #[inline(always)]
    pub fn lookup_space(&self, name: &str) -> Result<Option<u32>, Error> {
        self.wait_connected(Some(self.options.connect_timeout))?;
        Ok({
            let _lock = self.session_lock.lock();
            self.session.borrow().schema.lookup_space(name)
        })
    }

    #[inline(always)]
    pub fn lookup_index(&self, name: &str, space_id: u32) -> Result<Option<u32>, Error> {
        self.wait_connected(Some(self.options.connect_timeout))?;
        Ok({
            let _lock = self.session_lock.lock();
            self.session.borrow().schema.lookup_index(name, space_id)
        })
    }

    pub fn next_sync(&self) -> u64 {
        let sync = self.sync.get();
        self.sync.set(sync + 1);
        sync
    }
}

impl Drop for ConnInner {
    fn drop(&mut self) {
        self.close()
    }
}

pub fn recv_fiber_main(conn: Box<*mut Session>) -> i32 {
    set_cancellable(true);

    let session = unsafe { (*conn).as_mut() }.unwrap();
    let state_change_cond = session.state_change_cond.clone();

    loop {
        if is_cancelled() {
            return 0;
        }

        match session.state {
            ConnState::Active => {
                match protocol::decode_response(&mut session.stream.as_mut().unwrap()) {
                    Ok(response) => {
                        if response.schema_version != session.schema.version {
                            session.update_state(ConnState::FetchSchema);
                        }

                        match session.active_requests.get_mut(&(response.sync as u64)) {
                            None => continue,
                            Some(request_state) => {
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
                state_change_cond.wait();
            }
        }
    }
}
