// SAFETY:
// In this module `RefCell::borrow` is used a lot.
// This method panics if there are alive mutable borrows at that moment.
// But in this case it is safe to do this as:
// 1. Mutable borrows are taken and released in an encapsulated Sender functions
// 2. There are no `await` or `fiber::sleep` calls inside sender functions
// 3. This module is meant for single threaded async runtime
//
//! A single-producer, multi-consumer channel that only retains the *last* sent
//! value.
//!
//! This channel is useful for watching for changes to a value from multiple
//! points in the code base, for example, changes to configuration values.
//!
//! # Usage
//!
//! [`channel`] returns a [`Sender`] / [`Receiver`] pair. These are the producer
//! and sender halves of the channel. The channel is created with an initial
//! value. The **latest** value stored in the channel is accessed with
//! [`Receiver::borrow()`]. Awaiting [`Receiver::changed()`] waits for a new
//! value to sent by the [`Sender`] half.
//!
//! # Example
//! ```no_run
//! use tarantool::fiber::r#async::watch;
//! use tarantool::fiber;
//!
//! let (tx, mut rx) = watch::channel::<i32>(10);
//! tx.send(20).unwrap();
//! let value = fiber::block_on(async move {
//!     rx.changed().await.unwrap();
//!     rx.get()
//! });
//! ```
//!
//! # Closing
//!
//! [`Sender::is_closed`] allows the producer to detect
//! when all [`Receiver`] handles have been dropped. This indicates that there
//! is no further interest in the values being produced and work can be stopped.

use super::RecvError;
use std::{
    cell::{Cell, Ref, RefCell},
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

/// Error produced when sending a value fails.
#[derive(thiserror::Error, Debug)]
#[error("Failed to send this value, as receivers are currently holding a reference to the previous value")]
pub struct SendError<T>(pub T);

/// Sends values to the associated [`Receiver`](struct@Receiver).
///
/// Instances are created by the [`channel`](fn@channel) function.
pub struct Sender<T> {
    state: Rc<State<T>>,
}

/// Receives values from the associated [`Sender`](struct@Sender).
///
/// Instances are created by the [`channel`](fn@channel) function.
pub struct Receiver<T> {
    state: Rc<State<T>>,
    seen_version: u64,
}

impl<T> Sender<T> {
    /// Creates a new [`Receiver`] connected to this `Sender`.
    ///
    /// All messages sent before this call to `subscribe` are initially marked
    /// as seen by the new `Receiver`.
    ///
    /// This method can be called even if there are no other receivers. In this
    /// case, the channel is reopened.
    pub fn subscribe(&self) -> Receiver<T> {
        Receiver {
            state: self.state.clone(),
            seen_version: self.state.value.borrow().version,
        }
    }

    /// Sends a new value via the channel, notifying all receivers.
    ///
    /// This method fails if any of receivers is currently holding a reference
    /// to the previous value.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        if let Ok(mut value_ref) = self.state.value.try_borrow_mut() {
            value_ref.set(value);
        } else {
            return Err(SendError(value));
        }
        self.state.wake_all();
        Ok(())
    }

    /// Checks if the channel has been closed. This happens when all receivers
    /// have dropped.
    pub fn is_closed(&self) -> bool {
        // Only the rc instance of this sender remains
        Rc::strong_count(&self.state) == 1
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.state.sender_exists.set(false);
        self.state.wake_all()
    }
}

/// Returns a reference to the inner value.
///
/// Outstanding borrows hold a read lock on the inner value. This means that
/// long lived borrows could cause the produce half to block. It is recommended
/// to keep the borrow as short lived as possible.
pub struct ValueRef<'a, T>(Ref<'a, Value<T>>);

impl<'a, T> Deref for ValueRef<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0.value
    }
}

/// Future that returns when a new value is published in [`Sender`].
pub struct Notification<'a, T> {
    rx: &'a mut Receiver<T>,
}

impl<'a, T> Notification<'a, T> {
    /// Adds timeout to a future. See [`super::timeout::Timeout`].
    pub fn timeout(self, timeout: Duration) -> super::timeout::Timeout<Self> {
        super::timeout::timeout(timeout, self)
    }
}

impl<T> Receiver<T> {
    /// Checks if this channel contains a message that this receiver has not yet
    /// seen. The new value is not marked as seen.
    ///
    /// Although this method is called `has_changed`, it does not check new
    /// messages for equality, so this call will return true even if the new
    /// message is equal to the old message.
    pub fn has_changed(&self) -> bool {
        self.state.value.borrow().version != self.seen_version
    }

    /// Waits for a change notification, then marks the newest value as seen.
    ///
    /// If the newest value in the channel has not yet been marked seen when
    /// this method is called, the method marks that value seen and returns
    /// immediately. If the newest value has already been marked seen, then the
    /// method sleeps until a new message is sent by the [`Sender`] connected to
    /// this `Receiver`, or until the [`Sender`] is dropped.
    ///
    /// This method returns an error if and only if the [`Sender`] is dropped.    
    pub fn changed(&mut self) -> Notification<T> {
        Notification { rx: self }
    }

    /// Returns a reference to the most recently sent value.
    ///
    /// This method does not mark the returned value as seen, so future calls to
    /// [`Self::changed`] may return immediately even if you have already seen the
    /// value with a call to `borrow`.
    ///
    /// Care must be taken not to hold a ref, when the sender is setting a new value.
    /// This includes not holding a ref across await points and not explicitely yielding
    /// control to other fibers while holding a ref.
    ///
    /// Consider using [`Self::get`] or [`Self::get_cloned`] instead.
    pub fn borrow(&self) -> ValueRef<T> {
        ValueRef(self.state.value.borrow())
    }

    /// Returns a copy of the most recently sent value.
    ///
    /// This method does not mark the returned value as seen, so future calls to
    /// [`Self::changed`] may return immediately even if you have already seen the
    /// value with a call to `borrow`.
    pub fn get(&self) -> T
    where
        T: Copy,
    {
        *self.borrow().deref()
    }

    /// Returns the most recently sent value cloned.
    ///
    /// This method does not mark the returned value as seen, so future calls to
    /// [`Self::changed`] may return immediately even if you have already seen the
    /// value with a call to `borrow`.
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

/// Creates a new watch channel, returning the "send" and "receive" handles.
///
/// All values sent by [`Sender`] will become visible to the [`Receiver`] handles.
/// Only the last value sent is made available to the [`Receiver`] half. All
/// intermediate values are dropped.
///
/// See [`super::watch`] for examples.
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

#[cfg(feature = "tarantool_test")]
mod tests {
    use super::*;
    use crate::fiber;
    use crate::fiber::r#async::timeout;
    use crate::test::{TestCase, TESTS};
    use crate::test_name;
    use futures::join;
    use linkme::distributed_slice;

    const _1_SEC: Duration = Duration::from_secs(1);

    #[distributed_slice(TESTS)]
    static RECEIVE_NOTIFICATION_SENT_BEFORE: TestCase = TestCase {
        name: test_name!("receive_notification_sent_before"),
        f: || {
            let (tx, mut rx_1) = channel::<i32>(10);
            let mut rx_2 = rx_1.clone();
            // Subscribe should work same as rx clone
            let mut rx_3 = tx.subscribe();
            tx.send(20).unwrap();
            assert_eq!(
                fiber::block_on(async move {
                    let _ = join!(rx_1.changed(), rx_2.changed(), rx_3.changed());
                    (*rx_1.borrow(), *rx_2.borrow(), *rx_3.borrow())
                }),
                (20, 20, 20)
            );
        },
    };

    #[distributed_slice(TESTS)]
    static RECEIVE_NOTIFICATION_SENT_AFTER: TestCase = TestCase {
        name: test_name!("receive_notification_sent_after"),
        f: || {
            let (tx, mut rx_1) = channel::<i32>(10);
            let mut rx_2 = rx_1.clone();
            // Subscribe should work same as rx clone
            let mut rx_3 = tx.subscribe();
            let jh = fiber::start(move || {
                fiber::block_on(async move {
                    let _ = join!(rx_1.changed(), rx_2.changed(), rx_3.changed());
                    (*rx_1.borrow(), *rx_2.borrow(), *rx_3.borrow())
                })
            });
            tx.send(20).unwrap();
            assert_eq!(jh.join(), (20, 20, 20))
        },
    };

    #[distributed_slice(TESTS)]
    static RECEIVE_MULTIPLE_NOTIFICATIONS: TestCase = TestCase {
        name: test_name!("receive_multiple_notifications"),
        f: || {
            let (tx, mut rx_1) = channel::<i32>(10);
            let jh = fiber::start(|| {
                fiber::block_on(async {
                    rx_1.changed().await.unwrap();
                    *rx_1.borrow()
                })
            });
            tx.send(1).unwrap();
            assert_eq!(jh.join(), 1);
            let jh = fiber::start(|| {
                fiber::block_on(async {
                    rx_1.changed().await.unwrap();
                    *rx_1.borrow()
                })
            });
            tx.send(2).unwrap();
            assert_eq!(jh.join(), 2);
        },
    };

    #[distributed_slice(TESTS)]
    static RETAINS_ONLY_LAST_NOTIFICATION: TestCase = TestCase {
        name: test_name!("retains_only_last_notification"),
        f: || {
            let (tx, mut rx_1) = channel::<i32>(10);
            tx.send(1).unwrap();
            tx.send(2).unwrap();
            tx.send(3).unwrap();
            let v = fiber::block_on(async {
                rx_1.changed().await.unwrap();
                *rx_1.borrow()
            });
            assert_eq!(v, 3);
            // No changes after
            assert_eq!(
                fiber::block_on(rx_1.changed().timeout(_1_SEC)),
                Err(timeout::Expired)
            );
        },
    };

    #[distributed_slice(TESTS)]
    static NOTIFICATION_RECEIVE_ERROR: TestCase = TestCase {
        name: test_name!("notification_receive_error"),
        f: || {
            let (tx, mut rx_1) = channel::<i32>(10);
            let jh = fiber::start(|| fiber::block_on(rx_1.changed()));
            drop(tx);
            assert_eq!(jh.join(), Err(RecvError));
        },
    };

    #[distributed_slice(TESTS)]
    static NOTIFICATION_RECEIVED_IN_CONCURRENT_FIBER: TestCase = TestCase {
        name: test_name!("notification_received_in_concurrent_fiber"),
        f: || {
            let (tx, mut rx_1) = channel::<i32>(10);
            let mut rx_2 = rx_1.clone();
            let jh_1 = fiber::start(|| fiber::block_on(rx_1.changed()));
            let jh_2 = fiber::start(|| fiber::block_on(rx_2.changed()));
            tx.send(1).unwrap();
            assert!(jh_1.join().is_ok());
            assert!(jh_2.join().is_ok());
        },
    };
}
