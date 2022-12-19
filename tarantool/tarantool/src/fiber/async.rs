use std::{future::Future, rc::Rc, task::Poll, time::Instant};

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

mod context {
    use std::task::Context;
    use std::task::Waker;
    use std::time::Instant;

    #[repr(C)]
    pub struct ContextExt<'a> {
        /// Important: the `Context` field must come at the first place.
        /// Otherwise, reinterpreting (and further dereferencing) a `Context`
        /// pointer would be an UB.
        cx: Context<'a>,
        // TODO descriptor: Option<CoIOFileDescriptor>
        // A descriptor to poll with coio
        deadline: Option<Instant>,
    }

    impl<'a> ContextExt<'a> {
        #[must_use]
        pub fn from_waker(waker: &'a Waker) -> Self {
            Self {
                cx: Context::from_waker(waker),
                deadline: None,
            }
        }

        pub fn cx(&mut self) -> &mut Context<'a> {
            &mut self.cx
        }

        pub fn deadline(&self) -> Option<Instant> {
            self.deadline
        }

        pub unsafe fn set_deadline(cx: &mut Context<'_>, new: Instant) {
            // SAFETY: The following conditions must be met:
            // 1. The `Contex` must be the first field of `ContextExt`.
            // 2. Provided `cx` must really be the `ContextExt`. It's up to
            //    the caller, so the function is still marked unsafe.
            let cx: &mut ContextExt = &mut *(cx as *mut Context).cast();

            if matches!(cx.deadline, Some(old) if new > old) {
                // Don't increase it.
                return;
            }

            cx.deadline = Some(new);
        }
    }
}

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

        match cx.deadline() {
            Some(deadline) => {
                let timeout = deadline.saturating_duration_since(Instant::now());
                rcw.cond().wait_timeout(timeout)
            }
            None => rcw.cond().wait(),
        };
    }
}
