use std::{future::Future, rc::Rc, task::Poll, time::Instant};

use futures::pin_mut;

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
#[error("Sender dropped")]
pub struct RecvError;

pub mod oneshot {
    use super::{timeout::Timeout, RecvError};
    use std::{
        cell::Cell,
        future::Future,
        pin::Pin,
        rc::{Rc, Weak},
        task::{Context, Poll, Waker},
        time::Duration,
    };

    enum State<T> {
        Pending(Option<Waker>),
        Ready(T),
    }

    impl<T> Default for State<T> {
        fn default() -> Self {
            Self::Pending(None)
        }
    }

    pub struct Receiver<T>(Rc<Cell<State<T>>>);
    pub struct Sender<T>(Weak<Cell<State<T>>>);

    impl<T> Receiver<T> {
        pub fn timeout(self, timeout: Duration) -> Timeout<Self> {
            super::timeout::timeout(timeout, self)
        }
    }

    impl<T> Future for Receiver<T> {
        type Output = Result<T, RecvError>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let cell = &self.0;
            match cell.take() {
                State::Pending(mut waker) if Rc::weak_count(cell) > 0 => {
                    waker.get_or_insert_with(|| cx.waker().clone());
                    cell.set(State::Pending(waker));
                    Poll::Pending
                }
                State::Pending(_) => Poll::Ready(Err(RecvError)),
                State::Ready(t) => Poll::Ready(Ok(t)),
            }
        }
    }

    impl<T> Sender<T> {
        /// Sends the `value` and notifies the receiver.
        pub fn send(self, value: T) {
            let cell = if let Some(cell) = self.0.upgrade() {
                cell
            } else {
                return;
            };

            if let State::Pending(Some(waker)) = cell.take() {
                waker.wake()
            }

            cell.set(State::Ready(value));
        }

        /// Returns true if there's no receiver awaiting,
        pub fn is_dropped(&self) -> bool {
            self.0.strong_count() == 0
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            let cell = if let Some(cell) = self.0.upgrade() {
                cell
            } else {
                return;
            };
            match cell.take() {
                ready @ State::Ready(_) => cell.set(ready),
                State::Pending(Some(waker)) => waker.wake(),
                State::Pending(None) => (),
            }
        }
    }

    pub fn channel<T>() -> (Receiver<T>, Sender<T>) {
        let cell = Cell::new(State::default());
        let strong = Rc::from(cell);
        let weak = Rc::downgrade(&strong);
        (Receiver(strong), Sender(weak))
    }
}

// SAFETY:
// In this module `RefCell::borrow` is used a lot.
// This method panics if there are alive mutable borrows at that moment.
// But in this case it is safe to do this as:
// 1. Mutable borrows are taken and released in an encapsulated Sender functions
// 2. There are no `await` or `fiber::sleep` calls inside sender functions
// 3. This module is meant for single threaded async runtime
pub mod watch {
    use super::RecvError;
    use std::{
        cell::{BorrowMutError, Cell, Ref, RefCell},
        future::Future,
        ops::Deref,
        pin::Pin,
        rc::Rc,
        task::{Context, Poll, Waker},
        time::Duration,
    };

    pub struct Value<T> {
        value: T,
        version: u64,
    }

    impl<T> Value<T> {
        fn set(&mut self, v: T) {
            self.value = v;
            // It is ok to overflow as we check only the difference in version
            // and having receivers stuck near 0 version when sender has exceeded u64 is extremely unlickely.
            self.version = self.version.wrapping_add(1);
        }
    }

    struct State<T> {
        value: RefCell<Value<T>>,
        // I would be better to use HashSet here,
        // but `Waker` doesn't implement it.
        wakers: RefCell<Vec<Waker>>,
        sender_exists: Cell<bool>,
    }

    impl<T> State<T> {
        fn add_waker(&self, waker: &Waker) {
            let mut wakers = self.wakers.borrow_mut();
            if !wakers.iter().any(|w| waker.will_wake(w)) {
                wakers.push(waker.clone());
            }
        }

        fn wake_all(&self) {
            for waker in self.wakers.borrow_mut().drain(..) {
                waker.wake()
            }
        }
    }

    #[derive(thiserror::Error, Debug)]
    #[error("Failed to send this value, as receivers are currently holding a reference to the previous value: {0}")]
    pub struct SendError(BorrowMutError);

    pub struct Sender<T> {
        state: Rc<State<T>>,
    }

    pub struct Receiver<T> {
        state: Rc<State<T>>,
        seen_version: u64,
    }

    impl<T> Sender<T> {
        pub fn subscribe(&self) -> Receiver<T> {
            Receiver {
                state: self.state.clone(),
                seen_version: self.state.value.borrow().version,
            }
        }

        pub fn send(&self, value: T) -> Result<(), SendError> {
            self.state
                .value
                .try_borrow_mut()
                .map_err(SendError)?
                .set(value);
            self.state.wake_all();
            Ok(())
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            self.state.sender_exists.set(false);
            self.state.wake_all()
        }
    }

    pub struct ValueRef<'a, T>(Ref<'a, Value<T>>);

    impl<'a, T> Deref for ValueRef<'a, T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            &self.0.value
        }
    }

    pub struct Notification<'a, T> {
        rx: &'a mut Receiver<T>,
    }

    impl<'a, T> Notification<'a, T> {
        pub fn timeout(self, timeout: Duration) -> super::timeout::Timeout<Self> {
            super::timeout::timeout(timeout, self)
        }
    }

    impl<T> Receiver<T> {
        pub fn has_changed(&self) -> bool {
            self.state.value.borrow().version != self.seen_version
        }

        pub fn changed(&mut self) -> Notification<T> {
            Notification { rx: self }
        }

        /// Care must be taken not to hold a ref, when the sender is setting a new value.
        /// This includes not holding a ref across await points and not explicitely yielding
        /// control to other fibers while holding a ref.
        ///
        /// Consider using [`Self::get`] or [`Self::get_clone`] instead.
        pub fn borrow(&self) -> ValueRef<T> {
            ValueRef(self.state.value.borrow())
        }

        pub fn get(&self) -> T
        where
            T: Copy,
        {
            *self.borrow().deref()
        }

        pub fn get_cloned(&self) -> T
        where
            T: Clone,
        {
            self.borrow().deref().clone()
        }
    }

    impl<T> Clone for Receiver<T> {
        fn clone(&self) -> Self {
            Self {
                state: self.state.clone(),
                seen_version: self.state.value.borrow().version,
            }
        }
    }

    impl<'a, T> Future for Notification<'a, T> {
        type Output = Result<(), RecvError>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if !self.rx.state.sender_exists.get() {
                return Poll::Ready(Err(RecvError));
            }
            let version = self.rx.state.value.borrow().version;
            if version != self.rx.seen_version {
                self.rx.seen_version = version;
                Poll::Ready(Ok(()))
            } else {
                self.rx.state.add_waker(cx.waker());
                Poll::Pending
            }
        }
    }

    pub fn channel<T>(initial: T) -> (Sender<T>, Receiver<T>) {
        let state = State {
            value: RefCell::new(Value {
                value: initial,
                version: 0,
            }),
            wakers: Default::default(),
            sender_exists: Cell::new(true),
        };
        let tx = Sender {
            state: Rc::new(state),
        };
        let rx = tx.subscribe();
        (tx, rx)
    }
}

pub mod timeout {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;
    use std::time::Duration;
    use std::time::Instant;

    use super::context::ContextExt;

    #[derive(thiserror::Error, Debug, PartialEq, Eq)]
    #[error("Deadline expired")]
    pub struct Expired;

    pub struct Timeout<F> {
        future: F,
        deadline: Instant,
    }

    pub fn timeout<F: Future>(timeout: Duration, f: F) -> Timeout<F> {
        Timeout {
            future: f,
            deadline: Instant::now() + timeout,
        }
    }

    impl<F: Future> Timeout<F> {
        #[inline]
        fn pin_get_future(self: Pin<&mut Self>) -> Pin<&mut F> {
            // This is okay because `field` is pinned when `self` is.
            unsafe { self.map_unchecked_mut(|s| &mut s.future) }
        }
    }

    impl<F: Future> Future for Timeout<F> {
        type Output = Result<F::Output, Expired>;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let deadline = self.deadline;

            // First, try polling the future
            if let Poll::Ready(v) = self.pin_get_future().poll(cx) {
                Poll::Ready(Ok(v))
            } else if Instant::now() > deadline {
                Poll::Ready(Err(Expired)) // expired
            } else {
                // SAFETY: This is safe as long as the `Context` really
                // is the `ContextExt`. It's always true within provided
                // `block_on` async runtime.
                unsafe { ContextExt::set_deadline(cx, deadline) };
                Poll::Pending
            }
        }
    }
}

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
