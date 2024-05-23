use std::io::Cursor;
use std::{
    cell::{Cell, UnsafeCell},
    io,
    rc::{Rc, Weak},
    time::Duration,
};

use super::inner::ConnInner;
use crate::error::TarantoolError;
use crate::network::protocol;
use crate::{clock::INFINITY, error::Error, fiber::Cond, time::Instant, tuple::Decode, Result};

type StdResult<T, E> = std::result::Result<T, E>;

/// An asynchronous [`net_box::Conn`](crate::net_box::Conn) response.
pub struct Promise<T> {
    inner: Rc<InnerPromise<T>>,
}

impl<T> Promise<T> {
    #[inline]
    pub(crate) fn new(conn: Weak<ConnInner>) -> Self {
        Self {
            inner: Rc::new(InnerPromise {
                conn,
                cond: UnsafeCell::default(),
                data: Cell::new(None),
            }),
        }
    }

    #[inline]
    pub(crate) fn downgrade(&self) -> Weak<InnerPromise<T>> {
        Rc::downgrade(&self.inner)
    }

    #[inline]
    fn is_connected(&self) -> bool {
        self.inner
            .conn
            .upgrade()
            .map(|c| c.is_connected())
            .unwrap_or(false)
    }

    #[inline]
    fn check_connection(&self) -> Result<()> {
        if self.is_connected() {
            Ok(())
        } else {
            Err(io::Error::from(io::ErrorKind::NotConnected).into())
        }
    }

    /// Check if the promise is kept. Returns an error if one was received or if
    /// connection is closed.
    #[inline]
    pub fn state(&self) -> State {
        if let Some(res) = self.inner.data.take() {
            let is_ok = res.is_ok();
            self.inner.data.set(Some(res));
            if is_ok {
                State::Kept
            } else {
                State::ReceivedError
            }
        } else if self.is_connected() {
            State::Pending
        } else {
            State::Disconnected
        }
    }

    /// Check if the promise is kept and return the value. Consumes `self`.
    /// If you only need to check the state of the promise, use the
    /// [`state`](`Self::state`) method.
    ///
    /// Does not yield.
    ///
    /// Returns:
    /// - [`Ok`]`(value)` if value is available.
    /// - [`Err`]`(error)` if
    ///     - received a response with error
    ///     - connection was closed
    /// - [`Pending`]`(self)` otherwise
    ///
    /// [`Ok`]: TryGet::Ok
    /// [`Err`]: TryGet::Err
    /// [`Pending`]: TryGet::Pending
    #[inline]
    pub fn try_get(self) -> TryGet<T, Error> {
        match (self.inner.data.take(), self.check_connection()) {
            (Some(Ok(v)), _) => TryGet::Ok(v),
            (Some(Err(e)), _) | (None, Err(e)) => TryGet::Err(e),
            (None, Ok(())) => TryGet::Pending(self),
        }
    }

    /// Waits indefinitely until the promise is kept or the connection is
    /// closed. Consumes `self`.
    #[inline]
    pub fn wait(self) -> Result<T> {
        match self.wait_timeout(INFINITY) {
            TryGet::Ok(v) => Ok(v),
            TryGet::Err(e) => Err(e),
            TryGet::Pending(_) => unreachable!("100 years have passed, wake up"),
        }
    }

    /// Waits for the promise to be kept. Consumes `self`.
    ///
    /// Assume this yields.
    ///
    /// Returns:
    /// - [`Ok`]`(value)` if promise was successfully kept within time limit.
    /// - [`Err`]`(error)`
    ///     - received a response with error
    ///     - connection was closed
    /// - [`Pending`](self) on timeout
    ///
    /// [`Ok`]: TryGet::Ok
    /// [`Err`]: TryGet::Err
    /// [`Pending`]: TryGet::Pending
    pub fn wait_timeout(self, timeout: Duration) -> TryGet<T, Error> {
        if let Some(res) = self.inner.data.take() {
            return res.into();
        }

        let deadline = Instant::now_fiber().saturating_add(timeout);
        loop {
            if let Err(e) = self.check_connection() {
                break TryGet::Err(e);
            }

            unsafe { &*self.inner.cond.get() }.wait_deadline(deadline);

            if let Some(res) = self.inner.data.take() {
                break res.into();
            }

            if Instant::now_fiber() >= deadline {
                break TryGet::Pending(self);
            }
        }
    }

    /// Replaces the contained `Cond` used for [`wait`] & [`wait_timeout`]
    /// methods with the provided one. Useful if several promises need to be
    /// waited on.
    ///
    /// # Example
    /// ```no_run
    /// use tarantool::{fiber::Cond, net_box::{Conn, promise::{Promise, State}}};
    /// use std::rc::Rc;
    ///
    /// # fn get_conn(addr: &str) -> Conn { todo!() }
    /// let c1: Conn = get_conn("addr1");
    /// let mut p1: Promise<()> = c1.call_async("foo", ()).unwrap();
    /// let c2: Conn = get_conn("addr2");
    /// let mut p2: Promise<()> = c2.call_async("foo", ()).unwrap();
    /// let cond = Rc::new(Cond::new());
    /// p1.replace_cond(cond.clone());
    /// p2.replace_cond(cond.clone());
    /// cond.wait();
    /// assert!(
    ///     matches!(p1.state(), State::Kept | State::ReceivedError) ||
    ///     matches!(p2.state(), State::Kept | State::ReceivedError)
    /// )
    /// ```
    ///
    /// [`wait`]: Self::wait
    /// [`wait_timeout`]: Self::wait_timeout
    pub fn replace_cond(&mut self, cond: Rc<Cond>) -> Rc<Cond> {
        unsafe { std::ptr::replace(self.inner.cond.get(), cond) }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum State {
    Kept,
    ReceivedError,
    Pending,
    Disconnected,
}

/// Represents all possible value that can be returned from [`Promise::try_get`]
/// or [`Promise::wait_timeout`] methods.
#[derive(Debug)]
pub enum TryGet<T, E> {
    /// Promise was kept successfully.
    Ok(T),
    /// Promise will never be kept due to an error.
    Err(E),
    /// Promise yet is unresolved.
    Pending(Promise<T>),
}

impl<T, E> TryGet<T, E> {
    pub fn ok(self) -> Option<T> {
        match self {
            Self::Ok(v) => Some(v),
            _ => None,
        }
    }

    pub fn err(self) -> Option<E> {
        match self {
            Self::Err(e) => Some(e),
            _ => None,
        }
    }

    pub fn pending(self) -> Option<Promise<T>> {
        match self {
            Self::Pending(p) => Some(p),
            _ => None,
        }
    }

    /// Converts `self` into a nested [`Result`].
    ///
    /// Returns
    /// - `Ok(Ok(value))` in case of [`TryGet::Ok`]`(value)`.
    /// - `Ok(Err(error))` in case of [`TryGet::Err`]`(error)`.
    /// - `Err(promise)` in case of [`TryGet::Pending`]`(promise)`.
    ///
    /// This function basically checks if the promise is resolved (`Ok`) or not
    /// yet (`Err`).
    ///
    /// [`Result`]: std::result::Result
    #[inline(always)]
    pub fn into_res(self) -> StdResult<StdResult<T, E>, Promise<T>> {
        match self {
            Self::Ok(v) => Ok(Ok(v)),
            Self::Err(e) => Ok(Err(e)),
            Self::Pending(p) => Err(p),
        }
    }
}

impl<T, E> From<StdResult<T, E>> for TryGet<T, E> {
    fn from(r: StdResult<T, E>) -> Self {
        match r {
            Ok(v) => Self::Ok(v),
            Err(e) => Self::Err(e),
        }
    }
}

impl<T, E> From<TryGet<T, E>> for StdResult<StdResult<T, E>, Promise<T>> {
    #[inline(always)]
    fn from(r: TryGet<T, E>) -> Self {
        r.into_res()
    }
}

use std::fmt;
impl<T> fmt::Debug for Promise<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Promise")
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

pub struct InnerPromise<T> {
    conn: Weak<ConnInner>,
    cond: UnsafeCell<Rc<Cond>>,
    data: Cell<Option<Result<T>>>,
}

impl<T> InnerPromise<T> {
    fn signal(&self) {
        unsafe { &*self.cond.get() }.signal();
    }
}

impl<T> Consumer for InnerPromise<T>
where
    T: for<'de> Decode<'de>,
{
    fn handle_error(&self, error: Error) {
        self.data.set(Some(Err(error)));
        self.signal();
    }

    fn handle_disconnect(&self) {
        self.signal();
    }

    fn consume_data(&self, data: &[u8]) {
        self.data.set(Some(T::decode(data)));
        self.signal();
    }
}

////////////////////////////////////////////////////////////////////////////////
// Consumer
////////////////////////////////////////////////////////////////////////////////

pub trait Consumer {
    /// Is called to handle a single response consisting of a header and a body.
    ///
    /// The default implementation is suitable for most cases, so you probably
    /// only need to implement [`consume_data`] and [`handle_error`].
    ///
    /// **Must not yield**
    ///
    /// [`consume_data`]: Self::consume_data
    /// [`handle_error`]: Self::handle_error
    fn consume(&self, header: &protocol::Header, body: &[u8]) {
        let consume_impl = || {
            let mut cursor = Cursor::new(body);

            let mut data = None;
            let mut error: Option<TarantoolError> = None;

            let map_len = rmp::decode::read_map_len(&mut cursor)?;
            for _ in 0..map_len {
                let key = rmp::decode::read_pfix(&mut cursor)?;

                let value_start = cursor.position() as usize;
                crate::msgpack::skip_value(&mut cursor)?;
                let value_end = cursor.position() as usize;
                let mut value = &body[value_start..value_end];

                // dbg!((IProtoKey::try_from(key), rmp_serde::from_slice::<rmpv::Value>(value)));
                match key {
                    protocol::iproto_key::DATA => {
                        if data.is_some() {
                            crate::say_verbose!("duplicate IPROTO_DATA key in repsonse");
                        }
                        data = Some(value);
                    }
                    protocol::iproto_key::ERROR => {
                        let error = error.get_or_insert_with(Default::default);
                        let message = protocol::decode_string(&mut value)?;
                        error.message = Some(message.into());
                        error.code = header.error_code;
                    }
                    protocol::iproto_key::ERROR_EXT => {
                        if let Some(e) = protocol::decode_extended_error(&mut value)? {
                            error = Some(e);
                        }
                    }
                    other => self.consume_other(other, value),
                }
            }

            if let Some(data) = data {
                self.consume_data(data);
            }

            if let Some(error) = error {
                self.handle_error(Error::Remote(error));
            }

            Ok(())
        };

        if let Err(e) = consume_impl() {
            self.handle_error(e)
        }
    }

    /// Handles key-value pairs other than `IPROTO_DATA` and `IPROTO_ERROR_24`.
    /// The default implementation ignores them, so if nothing needs to be done
    /// for those, don't implement this method.
    ///
    /// **Must not yield**
    fn consume_other(&self, key: u8, other: &[u8]) {
        let (_, _) = (key, other);
    }

    /// If an error happens during the consumption of the response or if the
    /// response contains the `IPROTO_ERROR_24` key this function is called with
    /// the corresponding error value.
    ///
    /// **Must not yield**
    fn handle_error(&self, error: Error);

    /// Called when the connection is closed before the response was received.
    ///
    /// The default implementation calls [`handle_error`] with a
    /// [`NotConnected`](std::io::ErrorKind::NotConnected) error kind.
    ///
    /// **Must not yield**
    ///
    /// [`handle_error`]: Self::handle_error
    fn handle_disconnect(&self) {
        self.handle_error(io::Error::from(io::ErrorKind::NotConnected).into())
    }

    /// Handles a slice that covers a msgpack value corresponding to the
    /// `IPROTO_DATA` key.
    ///
    /// **Must not yield**
    fn consume_data(&self, data: &[u8]);
}
