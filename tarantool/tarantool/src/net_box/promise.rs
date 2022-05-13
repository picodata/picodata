use std::{
    cell::{Cell, UnsafeCell},
    io,
    rc::{Rc, Weak},
    time::{Duration, Instant},
};

use crate::{
    clock::INFINITY,
    error::Error,
    fiber::Cond,
    Result,
    tuple::Decode,
};

use super::{
    inner::ConnInner,
    protocol::Consumer,
};

type StdResult<T, E> = std::result::Result<T, E>;

/// An asynchronous [`net_box::Conn`](crate::net_box::Conn) response.
pub struct Promise<T> {
    inner: Rc<InnerPromise<T>>,
}

impl<T> Promise<T> {
    #[inline]
    pub(crate) fn new(conn: Weak<ConnInner>) -> Self {
        Self {
            inner: Rc::new(
                InnerPromise {
                    conn,
                    cond: UnsafeCell::default(),
                    data: Cell::new(None),
                }
           )
        }
    }

    #[inline]
    pub(crate) fn downgrade(&self) -> Weak<InnerPromise<T>> {
        Rc::downgrade(&self.inner)
    }

    #[inline]
    fn is_connected(&self) -> bool {
        self.inner.conn.upgrade().map(|c| c.is_connected()).unwrap_or(false)
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
    pub fn wait_timeout(self, mut timeout: Duration) -> TryGet<T, Error> {
        if let Some(res) = self.inner.data.take() {
            return res.into();
        }

        loop {
            if let Err(e) = self.check_connection() {
                break TryGet::Err(e);
            }

            let last_awake = Instant::now();
            unsafe { &*self.inner.cond.get() }.wait_timeout(timeout);

            if let Some(res) = self.inner.data.take() {
                break res.into();
            }

            timeout = timeout.saturating_sub(last_awake.elapsed());
            if timeout.is_zero() {
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
    Ok(T),
    Err(E),
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
}

impl<T, E> From<StdResult<T, E>> for TryGet<T, E> {
    fn from(r: StdResult<T, E>) -> Self {
        match r {
            Ok(v) => Self::Ok(v),
            Err(e) => Self::Err(e),
        }
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
    T: Decode,
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

