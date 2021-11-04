use std::{
    cell::Cell,
    marker::PhantomData,
    mem::MaybeUninit,
    rc::Rc,
    time::Duration,
};

use crate::{
    error::TarantoolErrorCode,
    ffi::tarantool as ffi,
    StdResult,
};

////////////////////////////////////////////////////////////////////////////////
// Channel
////////////////////////////////////////////////////////////////////////////////

pub(super) struct Channel<T> {
    inner: *mut ffi::fiber_channel,
    tx_count: Cell<usize>,
    rx_count: Cell<usize>,
    marker: PhantomData<T>,
}

impl<T> Channel<T> {
    pub fn new(size: u32) -> Self {
        let inner = unsafe { ffi::fiber_channel_new(size) };
        Self {
            inner,
            tx_count: Cell::new(0),
            rx_count: Cell::new(0),
            marker: PhantomData,
        }
    }

    /// Send a message `t` over the channel.
    ///
    /// In case the channel was closed or the current fiber was cancelled the
    /// function returns `Err(t)`, so that the caller has an option to reuse the
    /// value.
    ///
    /// This function may perform a **yield** in case the channel buffer is full
    /// and there are no readers ready to receive the message.
    pub fn send(&self, t: T, timeout: Option<Duration>) -> StdResult<(), SendError<T>> {
        if self.rx_count.get() == 0 {
            // There's no way to create new receivers once their count gets to 0
            return Err(SendError::Disconnected(t))
        }
        unsafe {
            let ipc_value_ptr = ffi::ipc_value_new();
            let ipc_value = &mut *ipc_value_ptr;
            let t_box_ptr = Box::into_raw(Box::new(t));
            ipc_value.data_union.data = t_box_ptr.cast();
            ipc_value.base.destroy = Some(Self::destroy_msg);

            let ret_code = ffi::fiber_channel_put_msg_timeout(
                self.inner,
                ipc_value_ptr.cast(),
                timeout.map(|t| t.as_secs_f64())
                    .unwrap_or(ffi::TIMEOUT_INFINITY),
            );

            if ret_code < 0 {
                // No need to call ipc_value.base.destroy, because the actual
                // value is returned back to the sender
                ffi::ipc_value_delete(ipc_value_ptr.cast());
                let t = *Box::from_raw(t_box_ptr);
                // XXX: this is the cheapest way to check if the timeout
                // happened, because of how errors are implemented inside
                // tarantool. To make sure this is the actually timeout system
                // error and not something else we could also check that
                // box_error_message returns "time out"
                if TarantoolErrorCode::last() == TarantoolErrorCode::System {
                    Err(SendError::Timeout(t))
                } else {
                    Err(SendError::Disconnected(t))
                }
            } else {
                Ok(())
            }
        }
    }

    /// Receive a message from the channel.
    ///
    /// In case the channel was closed or the current fiber was cancelled the
    /// function returns `None`.
    ///
    /// This function may perform a **yield** in case there is no message ready.
    pub fn recv(&self, timeout: Option<Duration>) -> StdResult<T, RecvError> {
        let is_empty = unsafe { ffi::fiber_channel_is_empty(self.inner) };
        if self.tx_count.get() == 0 && is_empty {
            // There's no way to create new senders once their count gets to 0
            return Err(RecvError::Disconnected)
        }
        unsafe {
            let mut ipc_msg_ptr_uninit = MaybeUninit::uninit();
            let ret_code = ffi::fiber_channel_get_msg_timeout(
                self.inner,
                ipc_msg_ptr_uninit.as_mut_ptr(),
                timeout.map(|t| t.as_secs_f64())
                    .unwrap_or(ffi::TIMEOUT_INFINITY),
            );

            if ret_code < 0 {
                // XXX: this is the cheapest way to check if the timeout
                // happened, because of how errors are implemented inside
                // tarantool. To make sure this is the actually timeout system
                // error and not something else we could also check that
                // box_error_message returns "time out"
                if TarantoolErrorCode::last() == TarantoolErrorCode::System {
                    Err(RecvError::Timeout)
                } else {
                    Err(RecvError::Disconnected)
                }
            } else {
                let ipc_msg_ptr = ipc_msg_ptr_uninit.assume_init();
                let ipc_value = &mut *ipc_msg_ptr.cast::<ffi::ipc_value>();
                let t_box = Box::from_raw(ipc_value.data_union.data.cast());
                ffi::ipc_value_delete(ipc_msg_ptr);
                Ok(*t_box)
            }
        }
    }

    pub unsafe extern "C" fn destroy_msg(msg: *mut ffi::ipc_msg) {
        let ipc_value = msg.cast::<ffi::ipc_value>();
        let value_ptr = (&mut *ipc_value).data_union.data.cast::<T>();
        drop(Box::from_raw(value_ptr));
        ffi::ipc_value_delete(msg)
    }
}

impl<T> Drop for Channel<T> {
    fn drop(&mut self) {
        assert_eq!(self.tx_count.get(), 0, "Channel dropped with live senders");
        assert_eq!(self.rx_count.get(), 0, "Channel dropped with live receivers");
        unsafe { ffi::fiber_channel_delete(self.inner) }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Sender
////////////////////////////////////////////////////////////////////////////////

pub struct Sender<T> {
    chan: Rc<Channel<T>>,
}

impl<T> Sender<T> {
    pub(super) fn new(chan: Rc<Channel<T>>) -> Self {
        let old = chan.tx_count.get();
        chan.tx_count.set(old + 1);
        Self { chan }
    }

    pub fn send(&self, t: T) -> StdResult<(), T> {
        self.chan.send(t, None)
            .map_err(|e|
                match e {
                    SendError::Timeout(_) => {
                        unreachable!("100 years have passed, wake up!")
                    }
                    SendError::Disconnected(t) => t,
                }
            )
    }

    pub fn send_timeout(&self, t: T, timeout: Duration) -> StdResult<(), SendError<T>> {
        self.chan.send(t, Some(timeout))
    }

    pub fn try_send(&self, t: T) -> StdResult<(), TrySendError<T>> {
        self.send_timeout(t, Duration::ZERO).map_err(From::from)
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self::new(self.chan.clone())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let new = self.chan.tx_count.get().checked_sub(1)
            .expect("We went bellow zero somehow");
        self.chan.tx_count.set(new);
        if new == 0 {
            unsafe { ffi::fiber_channel_close(self.chan.inner) };
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SendError<T> {
    Timeout(T),
    Disconnected(T),
}

impl<T> SendError<T> {
    pub fn into_inner(self) -> T {
        match self {
            Self::Timeout(t) | Self::Disconnected(t) => t,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TrySendError<T> {
    Full(T),
    Disconnected(T),
}

impl<T> TrySendError<T> {
    pub fn into_inner(self) -> T {
        match self {
            Self::Full(t) | Self::Disconnected(t) => t,
        }
    }
}

impl<T> From<SendError<T>> for TrySendError<T> {
    fn from(e: SendError<T>) -> Self {
        match e {
            SendError::Disconnected(t) => Self::Disconnected(t),
            SendError::Timeout(t) => Self::Full(t),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Receiver
////////////////////////////////////////////////////////////////////////////////

pub struct Receiver<T> {
    // TODO: now both Receiver and Sender have a strong reference to the Channel
    // and as a result the number of Rc::strong(&chan) is always equal to
    // chan.tx_count + chan.rx_count, which means that we store some redundant
    // info. It's not such a big deal, but if we wanted to be crazy efficient we
    // would have to implement our custom Rc whith tx_count and rx_count instead
    // of strong and weak.
    chan: Rc<Channel<T>>,
}

impl<T> Receiver<T> {
    pub(super) fn new(chan: Rc<Channel<T>>) -> Self {
        let old = chan.rx_count.get();
        chan.rx_count.set(old + 1);
        Self { chan }
    }

    pub fn recv(&self) -> Option<T> {
        match self.chan.recv(None) {
            Err(RecvError::Timeout) => {
                unreachable!("100 years have passed, wake up!")
            }
            res => res.ok(),
        }
    }

    pub fn recv_timeout(&self, timeout: Duration) -> StdResult<T, RecvError> {
        self.chan.recv(Some(timeout))
    }

    pub fn try_recv(&self) -> StdResult<T, TryRecvError> {
        self.recv_timeout(Duration::ZERO).map_err(From::from)
    }

    pub fn iter(&self) -> Iter<'_, T> {
        Iter { rx: self }
    }

    pub fn try_iter(&self) -> TryIter<'_, T> {
        TryIter { rx: self }
    }
}

pub struct Iter<'a, T: 'a> {
    rx: &'a Receiver<T>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.rx.recv()
    }
}

impl<'a, T> IntoIterator for &'a Receiver<T> {
    type Item = T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Iter<'a, T> {
        self.iter()
    }
}

pub struct TryIter<'a, T: 'a> {
    rx: &'a Receiver<T>,
}

impl<'a, T> Iterator for TryIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.rx.try_recv().ok()
    }
}

pub struct IntoIter<T> {
    rx: Receiver<T>,
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.rx.recv()
    }
}

impl<T> IntoIterator for Receiver<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> IntoIter<T> {
        IntoIter { rx: self }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self::new(self.chan.clone())
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let new = self.chan.rx_count.get().checked_sub(1)
            .expect("We went bellow zero somehow");
        self.chan.rx_count.set(new);
        if new == 0 {
            unsafe { ffi::fiber_channel_close(self.chan.inner) }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum RecvError {
    Timeout,
    Disconnected,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TryRecvError {
    Empty,
    Disconnected,
}

impl From<RecvError> for TryRecvError {
    fn from(e: RecvError) -> Self {
        match e {
            RecvError::Disconnected => Self::Disconnected,
            RecvError::Timeout => Self::Empty,
        }
    }
}

