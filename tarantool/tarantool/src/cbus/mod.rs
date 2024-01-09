#![cfg(any(feature = "picodata", doc))]

//! Tarantool cbus integration and channels.
//!
//! Original cbus provides a means of communication between separate threads:
//! - cpipe - channel between two cords. Cord is a separate thread with `libev` event loop inside it.
//! - lcpipe - channel between any arbitrary thread and cord.
//!
//! For the purposes of communication tx thread (where code of stored proc is working) and external threads
//! we should use a `lcpipe`.
//!
//! ## lcpipe schema
//!
//! Let's see how `lcpipe` woks, there are a number of entities participating in the exchange of messages:
//! - message - a unit of communication, may have a user defined payload
//! - message hop - defines how message will be handled on consumer side
//! - endpoint - message consumer, identified by name. Any endpoint occupies a single fiber for execute a cbus loop
//! - lcpipe - delivery message from producer to the consumer (endpoint), delivery never blocks consumer or producer
//!
//! Now schematically:
//!
//! ```text
//!                                            TX thread
//!                                      ┌────────────────────────┐
//! ┌───────────────┐ msg 1              │                        │
//! │               ├─────────┐  lcpipe 1│   ┌───────────┐        │
//! │   thread 1    │ msg 2   ├──────────┼─► │endpoint 1 │fiber 1 │
//! │               ├─────────│          │   └───────────┘        │
//! └───────────────┘         │          │                        │
//!                           │  lcpipe 2│   ┌───────────┐        │
//! ┌───────────────┐         ├──────────┼─► │endpoint 2 │fiber 2 │
//! │               │ msg 3   │          │   └───────────┘        │
//! │   thread 2    ├─────────┤          │                        │
//! │               ├─────────┤          │                        │
//! └───────────────┘ msg 4   │          │       ....             │
//!                           │          │                        │
//! ┌───────────────┐ msg 5   │          │   ┌───────────┐        │
//! │               ├─────────┤          │   │endpoint N │fiber N │
//! │   thread 3    ├─────────┘          │   └───────────┘        │
//! │               │ msg 6              │                        │
//! └───────────────┘                    │                        │
//!                                      └────────────────────────┘
//!```
//!
//! ## Cbus based channels
//!
//! The main idea of cbus based channels - use `lcpipe` to send a message about the need to unlock the consumer fiber.
//! Unlock consumer always means that there is a new data for consuming, but consumer not always locking
//! on try to receiver, if data is already available - lock is redundant.
//! For implementing a consumer lock and unlock a [`crate::fiber::Cond`] is used.

pub mod oneshot;
pub mod sync;
pub mod unbounded;

use crate::ffi;
use crate::ffi::tarantool::{
    cbus_endpoint_delete, cbus_endpoint_new, cbus_loop, lcpipe_delete, lcpipe_new, lcpipe_push_now,
};
use crate::fiber::Cond;
use std::ffi::CString;
use std::os::raw::c_void;
use std::ptr;

#[derive(Debug, thiserror::Error)]
pub enum RecvError {
    #[error("sending half of a channel is disconnected")]
    Disconnected,
}

pub struct SendError<T>(pub T);

#[derive(Debug, thiserror::Error)]
pub enum CbusError {
    #[error("endpoint with given name already registered")]
    EndpointAlreadyExists,
}

#[repr(C)]
struct STailQEntry {
    next: *const STailQEntry,
}

/// One hop in a message travel route. Next destination defined by `_pipe` field,
/// but for `lcpipe` there is only one hop supported, so `_pipe` field must always be NULL.
#[repr(C)]
pub struct MessageHop {
    f: unsafe fn(*mut c_void),
    _pipe: *const c_void,
}

/// A message traveling between thread and cord.
#[repr(C)]
pub struct Message<T> {
    fifo: STailQEntry,
    route: *mut MessageHop,
    hop: *mut MessageHop,
    callback: Option<T>,
}

impl<F> Message<F>
where
    F: FnOnce() + Send + 'static,
{
    unsafe fn trampoline(msg: *mut c_void) {
        let msg = msg.cast::<Self>();
        let mut msg = Box::from_raw(msg);
        if let Some(callback) = msg.callback.take() {
            callback();
        }
    }

    /// Create a new cbus message.
    ///
    /// # Arguments
    ///
    /// * `callback`: executes when the message reaches destination endpoint
    pub fn new(callback: F) -> Self {
        let hop = MessageHop {
            f: Self::trampoline,
            _pipe: std::ptr::null(),
        };
        let hop = Box::new(hop);
        let hop = Box::into_raw(hop);
        Self {
            fifo: STailQEntry { next: ptr::null() },
            route: hop,
            hop,
            callback: Some(callback),
        }
    }
}

impl<T> Drop for Message<T> {
    fn drop(&mut self) {
        let hop = self.hop.cast::<MessageHop>();
        drop(unsafe { Box::from_raw(hop) });
    }
}

/// Cbus endpoint. Endpoint is a message consumer on a cord side.
pub struct Endpoint {
    endpoint: *const (),
}

impl Endpoint {
    /// Create a new cbus endpoint
    ///
    /// # Arguments
    ///
    /// * `name`: endpoint name
    pub fn new(name: &str) -> Result<Self, CbusError> {
        let mut endpoint: *mut () = ptr::null_mut();
        let endpoint_ptr: *mut *mut () = &mut endpoint;
        let name = CString::new(name).expect("endpoint name may not contain interior null bytes");
        let err = unsafe { cbus_endpoint_new(endpoint_ptr as *mut *mut c_void, name.as_ptr()) };
        if err != 0 {
            return Err(CbusError::EndpointAlreadyExists);
        }

        Ok(Endpoint { endpoint })
    }

    /// Run the message delivery loop until the current fiber is cancelled.
    pub fn cbus_loop(&self) {
        unsafe { cbus_loop(self.endpoint as *mut c_void) }
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        // return value is ignored cause, currently, there is no situation when deleting may fail
        unsafe { cbus_endpoint_delete(self.endpoint as *mut c_void) };
    }
}

/// A uni-directional FIFO queue from any thread to cord.
pub struct LCPipe {
    pipe: *mut ffi::tarantool::LCPipe,
}

// It is safe to send lcpipe to not-owning thread.
unsafe impl Send for LCPipe {}

impl LCPipe {
    /// Create and initialize a pipe and connect it to the consumer.
    /// The call returns only when the consumer, identified by endpoint name, has joined the bus.
    pub fn new(endpoint_name: &str) -> Self {
        let endpoint =
            CString::new(endpoint_name).expect("endpoint name may not contain interior null bytes");

        Self {
            pipe: unsafe { lcpipe_new(endpoint.as_ptr()) },
        }
    }

    /// Push a new message into pipe. Message will be flushed to consumer queue (but not handled) immediately.
    pub fn push_message<T>(&mut self, msg: Message<T>) {
        let msg = Box::new(msg);
        // leaks a message, there is no `Box::from_raw` later, because it will happen implicitly
        // when [`MessageHop::f`] called
        let msg = Box::leak(msg);
        unsafe { lcpipe_push_now(self.pipe, msg as *mut Message<T> as *mut c_void) }
    }
}

impl Drop for LCPipe {
    fn drop(&mut self) {
        unsafe { lcpipe_delete(self.pipe) };
    }
}

/// This is a wrapper over a [`Cond`] for sending it between threads.
///
/// # Safety.
/// `UnsafeCond` must be dereferenced and dropped only in the cord thread, dropping this structure
/// in thread that not created an underline `Cond` it will lead to an application crash.
struct UnsafeCond(Cond);

impl UnsafeCond {
    /// Return a reference to underline `Cond`.
    ///
    /// # Safety.
    /// It is safe to use a `Cond` only in a tarantool cord threads (TX thread in most cases).
    unsafe fn as_ref(&self) -> &Cond {
        &self.0
    }
}

unsafe impl Send for UnsafeCond {}

unsafe impl Sync for UnsafeCond {}

#[cfg(feature = "internal_test")]
mod tests {
    use crate::cbus;
    use crate::cbus::Message;
    use crate::fiber;
    use crate::fiber::Cond;
    use std::thread;
    use std::thread::ThreadId;

    pub(super) fn run_cbus_endpoint(endpoint_name: &'static str) -> fiber::FiberId {
        fiber::Builder::new()
            .name("cbus_fiber")
            .func(move || {
                let cbus_endpoint = cbus::Endpoint::new(endpoint_name).unwrap();
                cbus_endpoint.cbus_loop();
            })
            .start_non_joinable()
            .expect("failed to start the cbus_fiber")
    }

    #[crate::test(tarantool = "crate")]
    pub fn cbus_send_message_test() {
        static mut TX_THREAD_ID: Option<ThreadId> = None;
        static mut SENDER_THREAD_ID: Option<ThreadId> = None;

        let cbus_fiber_id = run_cbus_endpoint("cbus_send_message_test");

        struct CondPtr(*const Cond);
        unsafe impl Send for CondPtr {}

        let cond = Cond::new();
        let cond_ptr = CondPtr(&cond as *const Cond);

        let thread = thread::spawn(move || {
            unsafe { SENDER_THREAD_ID = Some(thread::current().id()) };
            let mut pipe = cbus::LCPipe::new("cbus_send_message_test");
            let msg = Message::new(move || {
                unsafe { TX_THREAD_ID = Some(thread::current().id()) };
                let cond = unsafe { cond_ptr.0.as_ref().unwrap() };
                cond.broadcast();
            });
            pipe.push_message(msg);
        });

        cond.wait();

        unsafe {
            assert!(SENDER_THREAD_ID.is_some());
            assert!(TX_THREAD_ID.is_some());
            assert_ne!(SENDER_THREAD_ID, TX_THREAD_ID);
        }

        thread.join().unwrap();
        assert!(fiber::cancel(cbus_fiber_id));
    }
}
