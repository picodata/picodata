use std::{
    marker::PhantomData,
    mem::MaybeUninit,
    ptr::NonNull,
    rc::Rc,
    time::Duration,
};

use crate::{
    error::TarantoolErrorCode,
    ffi::tarantool as ffi,
};

////////////////////////////////////////////////////////////////////////////////
// Channel
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct Channel<T>(Rc<ChannelBox<T>>);

impl<T> Channel<T> {
    pub fn new(size: u32) -> Self {
        let inner_raw = unsafe { ffi::fiber_channel_new(size) };
        let inner = NonNull::new(inner_raw)
            .expect("Memory allocation failure when creating fiber::Channel");
        Self(Rc::new(ChannelBox { inner, marker: PhantomData }))
    }

    fn as_ptr(&self) -> *mut ffi::fiber_channel {
        self.0.inner.as_ptr()
    }

    pub fn close(self) {
        unsafe { ffi::fiber_channel_close(self.as_ptr()) }
    }

    pub fn is_closed(&self) -> bool {
        unsafe { ffi::fiber_channel_is_closed(self.as_ptr()) }
    }

    pub fn is_empty(&self) -> bool {
        unsafe { ffi::fiber_channel_is_empty(self.as_ptr()) }
    }

    pub fn size(&self) -> u32 {
        unsafe { ffi::fiber_channel_size(self.as_ptr()) }
    }

    pub fn count(&self) -> u32 {
        unsafe { ffi::fiber_channel_count(self.as_ptr()) }
    }

    pub fn has_readers(&self) -> bool {
        unsafe { ffi::fiber_channel_has_readers(self.as_ptr()) }
    }

    pub fn has_writers(&self) -> bool {
        unsafe { ffi::fiber_channel_has_writers(self.as_ptr()) }
    }
}

impl<T> SendTimeout<T> for Channel<T> {
    fn send_maybe_timeout(&self, t: T, timeout: Option<Duration>) -> Result<(), SendError<T>>
    where
        T: 'static,
    {
        unsafe {
            let ipc_value_ptr = ffi::ipc_value_new();
            let ipc_value = &mut *ipc_value_ptr;
            let t_box_ptr = Box::into_raw(Box::new(t));
            ipc_value.data_union.data = t_box_ptr.cast();
            ipc_value.base.destroy = Some(Self::destroy_msg);

            let ret_code = ffi::fiber_channel_put_msg_timeout(
                self.as_ptr(),
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
}

impl<T> RecvTimeout<T> for Channel<T> {
    fn recv_maybe_timeout(&self, timeout: Option<Duration>) -> Result<T, RecvError> {
        unsafe {
            let mut ipc_msg_ptr_uninit = MaybeUninit::uninit();
            let ret_code = ffi::fiber_channel_get_msg_timeout(
                self.as_ptr(),
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
}

impl<T> Channel<T> {
    /// # Safety
    /// `msg` must have been created with `ffi::ipc_value_new`
    pub unsafe extern "C" fn destroy_msg(msg: *mut ffi::ipc_msg) {
        let ipc_value = msg.cast::<ffi::ipc_value>();
        let value_ptr = (*ipc_value).data_union.data.cast::<T>();
        drop(Box::from_raw(value_ptr));
        ffi::ipc_value_delete(msg)
    }
}

pub trait SendTimeout<T> {
    /// Send a message `t` over the channel.
    ///
    /// In case the channel was closed or the current fiber was cancelled the
    /// function returns `SendError<T>` which contains the original message, so
    /// that the caller has an option to reuse the value.
    ///
    /// This function may perform a **yield** in case the channel buffer is full
    /// and there are no readers ready to receive the message.
    fn send_maybe_timeout(
        &self,
        t: T,
        timeout: Option<Duration>,
    ) -> Result<(), SendError<T>>
    where
        T: 'static;

    fn send(&self, t: T) -> Result<(), T>
    where
        T: 'static,
    {
        match self.send_maybe_timeout(t, None) {
            Ok(()) => Ok(()),
            Err(SendError::Disconnected(t)) => Err(t),
            Err(SendError::Timeout(_)) => {
                unreachable!("100 years have passed, wake up!")
            }
        }
    }

    fn send_timeout(&self, t: T, timeout: Duration) -> Result<(), SendError<T>>
    where
        T: 'static,
    {
        self.send_maybe_timeout(t, Some(timeout))
    }

    fn try_send(&self, t: T) -> Result<(), TrySendError<T>>
    where
        T: 'static,
    {
        self.send_timeout(t, Duration::ZERO).map_err(From::from)
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

pub trait RecvTimeout<T> {
    /// Receive a message from the channel.
    ///
    /// In case the channel was closed or the current fiber was cancelled the
    /// function returns `None`.
    ///
    /// This function may perform a **yield** in case there is no message ready.
    fn recv_maybe_timeout(&self, timeout: Option<Duration>) -> Result<T, RecvError>;

    fn recv(&self) -> Option<T> {
        match self.recv_maybe_timeout(None) {
            Err(RecvError::Timeout) => {
                unreachable!("100 years have passed, wake up!")
            }
            res => res.ok(),
        }
    }

    fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvError> {
        self.recv_maybe_timeout(Some(timeout))
    }

    fn try_recv(&self) -> Result<T, TryRecvError> {
        self.recv_timeout(Duration::ZERO).map_err(From::from)
    }
}

impl<T> Channel<T> {
    pub fn iter(&self) -> Iter<'_, T> {
        Iter(self)
    }

    pub fn try_iter(&self) -> TryIter<'_, T> {
        TryIter(self)
    }
}

// These reimplementations are here just so that we don't have to
// `use tarantool::fiber::{SendTimeout, RecvTimeout}` every time you want to
// use the channel
impl<T> Channel<T> {
    #[inline(always)]
    pub fn send(&self, t: T) -> Result<(), T>
    where
        T: 'static,
    {
        SendTimeout::send(self, t)
    }

    #[inline(always)]
    pub fn send_timeout(&self, t: T, timeout: Duration) -> Result<(), SendError<T>>
    where
        T: 'static,
    {
        SendTimeout::send_timeout(self, t, timeout)
    }

    #[inline(always)]
    pub fn try_send(&self, t: T) -> Result<(), TrySendError<T>>
    where
        T: 'static,
    {
        SendTimeout::try_send(self, t)
    }

    #[inline(always)]
    pub fn recv(&self) -> Option<T> {
        RecvTimeout::recv(self)
    }

    #[inline(always)]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvError> {
        RecvTimeout::recv_timeout(self, timeout)
    }

    #[inline(always)]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        RecvTimeout::try_recv(self)
    }
}

macro_rules! iter_struct {
    (
        $(
            $struct:ident $( [ $( $tp:tt )* ] )? ( $of:ty )
            $([ where $( $where:tt )+ ])?  | $self:ident | { $( $body:tt )+ }
        )+
    ) => {
        $(
            pub struct $struct $( < $($tp)* > )? ( $of ) $(where $($where)+)?;

            impl $( < $($tp)* > )? Iterator for $struct $( < $($tp)* > )? {
                type Item = T;

                fn next(&mut $self) -> Option<T> {
                    $( $body )*
                }
            }
        )+
    }
}

iter_struct!{
    Iter['a, T](&'a Channel<T>) [where T: 'a] |self| { self.0.recv() }
    TryIter['a, T](&'a Channel<T>) [where T: 'a] |self| { self.0.try_recv().ok() }
    IntoIter[T](Channel<T>) |self| { self.0.recv() }
}

impl<'a, T> IntoIterator for &'a Channel<T> {
    type Item = T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Iter<'a, T> {
        self.iter()
    }
}

impl<T> IntoIterator for Channel<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> IntoIter<T> {
        IntoIter(self)
    }
}

struct ChannelBox<T> {
    inner: NonNull<ffi::fiber_channel>,
    marker: PhantomData<T>,
}

impl<T> Drop for ChannelBox<T> {
    fn drop(&mut self) {
        unsafe { ffi::fiber_channel_delete(self.inner.as_ptr()) }
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

