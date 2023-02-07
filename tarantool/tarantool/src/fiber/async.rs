use std::{
    future::Future,
    pin::Pin,
    rc::Rc,
    task::Poll,
    time::{Duration, Instant},
};

use futures::pin_mut;

pub mod oneshot;
pub mod timeout;
pub mod watch;

/// Error that happens on the receiver side of the channel.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
#[error("sender dropped")]
pub struct RecvError;

mod waker {
    use crate::fiber;
    use std::rc::Rc;
    use std::task::RawWaker;
    use std::task::RawWakerVTable;
    use std::task::Waker;

    #[derive(Default)]
    pub struct FiberWaker {
        cond: fiber::Cond,
    }

    impl FiberWaker {
        pub fn cond(&self) -> &fiber::Cond {
            &self.cond
        }

        pub fn wake(&self) {
            self.cond.broadcast()
        }
    }

    unsafe impl Send for FiberWaker {}
    unsafe impl Sync for FiberWaker {}

    pub fn with_rcw(rcw: Rc<FiberWaker>) -> Waker {
        let raw_waker = raw_waker(rcw);
        unsafe { Waker::from_raw(raw_waker) }
    }

    fn raw_waker(rcw: Rc<FiberWaker>) -> RawWaker {
        const RC_WAKER_VT: RawWakerVTable = RawWakerVTable::new(
            rc_waker_clone,
            rc_waker_wake,
            rc_waker_wake_by_ref,
            rc_waker_drop,
        );
        let ptr: *const () = Rc::into_raw(rcw).cast();
        RawWaker::new(ptr, &RC_WAKER_VT)
    }

    unsafe fn rc_waker_clone(data: *const ()) -> RawWaker {
        let rcw: Rc<FiberWaker> = {
            // Clone it manually
            Rc::increment_strong_count(data);
            Rc::from_raw(data.cast())
        };
        raw_waker(rcw)
    }

    /// Represents `fn wake(self)`, must consume the data
    unsafe fn rc_waker_wake(data: *const ()) {
        let rcw: Rc<FiberWaker> = Rc::from_raw(data.cast());
        rcw.wake();
        drop(rcw);
    }

    /// Represents `fn wake_by_ref(&self)`, must NOT consume the data
    unsafe fn rc_waker_wake_by_ref(data: *const ()) {
        let rcw: Rc<FiberWaker> = Rc::from_raw(data.cast());
        rcw.wake();
        std::mem::forget(rcw);
    }

    unsafe fn rc_waker_drop(data: *const ()) {
        let rcw: Rc<FiberWaker> = Rc::from_raw(data.cast());
        drop(rcw)
    }
}

pub(crate) mod coio {
    use std::cell::Cell;
    use std::ffi::CString;
    use std::rc::Rc;

    /// Request for address resolution. After being set in [`context::ContextExt`] it will be executed by `block_on`.
    /// See [coio_getaddrinfo](tarantool::ffi::tarantool::coio_getaddrinfo) for low level details.
    #[derive(Clone, Debug)]
    pub struct GetAddrInfo {
        pub host: CString,
        pub hints: libc::addrinfo,
        pub res: Rc<Cell<*mut libc::addrinfo>>,
        pub err: Rc<Cell<bool>>,
    }
}

pub(crate) mod context {
    use super::coio::GetAddrInfo;
    use std::os::unix::io::RawFd;
    use std::task::Context;
    use std::task::Waker;
    use std::time::Instant;

    use crate::ffi::tarantool as ffi;

    /// The context is primarily used to pass wakup conditions from a
    /// pending future to the async executor (i.e `block_on`). There's
    /// no place for that in returned `Poll` enum, so we use a
    /// workaround.
    #[repr(C)]
    pub struct ContextExt<'a> {
        /// Important: the `Context` field must come at the first place.
        /// Otherwise, reinterpreting (and further dereferencing) a `Context`
        /// pointer would be an UB.
        cx: Context<'a>,

        /// A time limit to wake up the fiber. If `None`, the `block_on`
        /// async executor will use `Duration::MAX` value as a timeout.
        pub(super) deadline: Option<Instant>,

        /// Wait an event on a file descriptor rather than on a
        /// `fiber::Cond` (that is under the hood of a `Waker`).
        pub(super) coio_wait: Option<(RawFd, ffi::CoIOFlags)>,

        /// Wait for address resolution rather than on a
        /// `fiber::Cond` (that is under the hood of a `Waker`).
        pub(super) coio_getaddrinfo: Option<GetAddrInfo>,
    }

    impl<'a> ContextExt<'a> {
        #[must_use]
        pub fn from_waker(waker: &'a Waker) -> Self {
            Self {
                cx: Context::from_waker(waker),
                deadline: None,
                coio_wait: None,
                coio_getaddrinfo: None,
            }
        }

        pub fn cx(&mut self) -> &mut Context<'a> {
            &mut self.cx
        }

        /// SAFETY: The following conditions must be met:
        /// 1. The `Contex` must be the first field of `ContextExt`.
        /// 2. Provided `cx` must really be the `ContextExt`. It's up to
        ///    the caller, so the function is still marked unsafe.
        pub(crate) unsafe fn as_context_ext<'b>(cx: &'b mut Context<'_>) -> &'b mut Self {
            let cx: &mut ContextExt = &mut *(cx as *mut Context).cast();
            cx
        }

        /// SAFETY: `cx` must really be the `ContextExt`
        pub unsafe fn set_deadline(cx: &mut Context<'_>, new: Instant) {
            let cx = Self::as_context_ext(cx);
            if let Some(ref mut deadline) = cx.deadline {
                if new < *deadline {
                    *deadline = new
                }
            } else {
                cx.deadline = Some(new)
            }
        }

        /// SAFETY: `cx` must really be the `ContextExt`
        pub unsafe fn set_coio_wait(cx: &mut Context<'_>, fd: RawFd, event: ffi::CoIOFlags) {
            let cx = Self::as_context_ext(cx);
            cx.coio_wait = Some((fd, event));
        }

        /// SAFETY: `cx` must really be the `ContextExt`
        pub unsafe fn set_coio_getaddrinfo(cx: &mut Context<'_>, v: GetAddrInfo) {
            let cx = Self::as_context_ext(cx);
            cx.coio_getaddrinfo = Some(v);
        }
    }
}

/// A wrapper around a future which has on_drop behavior.
/// See [`on_drop`].
pub struct OnDrop<Fut, Fun: FnMut()> {
    future: Fut,
    on_drop: Fun,
}

impl<Fut: Future, Fun: FnMut()> OnDrop<Fut, Fun> {
    #[inline]
    fn pin_get_future(self: Pin<&mut Self>) -> Pin<&mut Fut> {
        // This is okay because `future` is pinned when `self` is.
        unsafe { self.map_unchecked_mut(|s| &mut s.future) }
    }
}

impl<Fut: Future, Fun: FnMut()> Future for OnDrop<Fut, Fun> {
    type Output = Fut::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.pin_get_future().poll(cx)
    }
}

impl<Fut, Fun: FnMut()> Drop for OnDrop<Fut, Fun> {
    fn drop(&mut self) {
        (self.on_drop)()
    }
}

/// Adds a closure to the future, which will be executed when the future is dropped.
/// This can be useful to cleanup external resources on future cancelation.
pub fn on_drop<Fut: Future, Fun: FnMut()>(future: Fut, on_drop: Fun) -> OnDrop<Fut, Fun> {
    OnDrop { future, on_drop }
}

/// Futures implementing this trait can attach a closure
/// which will be executed when the future is dropped.
/// See [`on_drop`].
pub trait IntoOnDrop: Future + Sized {
    /// Adds on_drop closure to a future. See [`on_drop`].
    #[inline]
    fn on_drop<Fun: FnMut()>(self, on_drop: Fun) -> OnDrop<Self, Fun> {
        self::on_drop(self, on_drop)
    }
}

impl<T> IntoOnDrop for T where T: Future + Sized {}

/// Runs a future to completion on the fiber-based runtime. This is the async runtime’s entry point.
///
/// This runs the given future on the current fiber, blocking until it is complete, and yielding its resolved result.
pub fn block_on<F: Future>(f: F) -> F::Output {
    let rcw: Rc<waker::FiberWaker> = Default::default();
    let waker = waker::with_rcw(rcw.clone());

    pin_mut!(f);
    loop {
        let mut cx = context::ContextExt::from_waker(&waker);

        if let Poll::Ready(t) = f.as_mut().poll(cx.cx()) {
            return t;
        }

        let timeout = match cx.deadline {
            Some(deadline) => deadline.saturating_duration_since(Instant::now()),
            None => Duration::MAX,
        };

        if let Some(getaddrinfo) = cx.coio_getaddrinfo {
            let mut res = std::ptr::null_mut();
            let out = unsafe {
                crate::ffi::tarantool::coio_getaddrinfo(
                    getaddrinfo.host.as_ptr(),
                    std::ptr::null(),
                    &getaddrinfo.hints as *const _,
                    &mut res as *mut _,
                    timeout.as_secs_f64(),
                )
            };
            getaddrinfo.err.set(out != 0);
            getaddrinfo.res.set(res);
        } else if let Some((fd, event)) = cx.coio_wait {
            unsafe {
                crate::ffi::tarantool::coio_wait(fd, event.bits(), timeout.as_secs_f64());
            }
        } else {
            rcw.cond().wait_timeout(timeout);
        }
    }
}

#[cfg(feature = "internal_test")]
mod tests {
    use std::cell::Cell;

    use super::timeout::IntoTimeout as _;
    use super::*;

    async fn always_pending() {
        loop {
            futures::pending!()
        }
    }

    #[crate::test(tarantool = "crate")]
    fn on_drop_is_executed() {
        block_on(async {
            let mut executed = false;
            always_pending()
                .on_drop(|| executed = true)
                .timeout(Duration::from_secs(0))
                .await
                .unwrap_err();
            assert!(executed);
        });
    }

    #[crate::test(tarantool = "crate")]
    fn nested_on_drop_is_executed() {
        let executed = Rc::new(Cell::new(false));
        let executed_clone = executed.clone();
        let foo = || async {
            always_pending().on_drop(|| executed_clone.set(true)).await;
        };
        block_on(async {
            foo().timeout(Duration::from_secs(0)).await.unwrap_err();
        });
        assert!(executed.get());
    }
}
