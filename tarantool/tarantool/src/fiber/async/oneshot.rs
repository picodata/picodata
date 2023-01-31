//! A one-shot channel is used for sending a single message between
//! asynchronous tasks. The [`channel`] function is used to create a
//! [`Sender`] and [`Receiver`] handle pair that form the channel.
//!
//! The `Sender` handle is used by the producer to send the value.
//! The `Receiver` handle is used by the consumer to receive the value.
//!
//! Each handle can be used on separate fiber.
//!
//! Since the `send` method is not async it can be used from non-async code.
//!
//! # Example
//! ```no_run
//! use tarantool::fiber::r#async::oneshot;
//! use tarantool::fiber;
//!
//! let (tx, rx) = oneshot::channel::<i32>();
//! tx.send(56);
//! let value = fiber::block_on(rx);
//! ```
//!
//! If the sender is dropped without sending, the receiver will fail with
//! [`super::RecvError`]:

use super::RecvError;
use std::{
    cell::Cell,
    fmt::Debug,
    future::Future,
    pin::Pin,
    rc::{Rc, Weak},
    task::{Context, Poll, Waker},
};

#[derive(Debug)]
enum State<T> {
    Pending(Option<Waker>),
    Ready(T),
}

impl<T> Default for State<T> {
    fn default() -> Self {
        Self::Pending(None)
    }
}

/// Receives a value from the associated [`Sender`].
///
/// A pair of both a [`Sender`] and a [`Receiver`]  are created by the
/// [`channel`](fn@channel) function.
///
/// This channel has no `recv` method because the receiver itself implements the
/// [`Future`] trait. To receive a value, `.await` the `Receiver` object directly.
///
/// If the sender is dropped without sending, the receiver will fail with
/// [`super::RecvError`]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Receiver<T>(Rc<Cell<State<T>>>);

/// Sends a value to the associated [`Receiver`].
///
/// A pair of both a [`Sender`] and a [`Receiver`]  are created by the
/// [`channel`](fn@channel) function.
///
/// If the sender is dropped without sending, the receiver will fail with
/// [`super::RecvError`]
pub struct Sender<T>(Weak<Cell<State<T>>>);

impl<T> Receiver<T> {
    /// Returns `true` if the associated [`Sender`] handle has been dropped.
    ///
    /// If `true` is returned, awaiting this future will always result in an error.
    #[inline]
    pub fn is_closed(&self) -> bool {
        Rc::weak_count(&self.0) == 0
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let cell = &self.0;
        match cell.take() {
            State::Pending(mut waker) if !self.is_closed() => {
                waker.get_or_insert_with(|| cx.waker().clone());
                cell.set(State::Pending(waker));
                Poll::Pending
            }
            State::Pending(_) => Poll::Ready(Err(RecvError)),
            State::Ready(t) => Poll::Ready(Ok(t)),
        }
    }
}

impl<T> Debug for Receiver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

impl<T> Sender<T> {
    /// Attempts to send a value on this channel, returning it back if it could
    /// not be sent.
    ///
    /// This method consumes `self` as only one value may ever be sent on a oneshot
    /// channel. It is not marked async because sending a message to an oneshot
    /// channel never requires any form of waiting.  Because of this, the `send`
    /// method can be used in both synchronous and asynchronous code without
    /// problems.
    ///
    /// A successful send occurs when it is determined that the other end of the
    /// channel has not hung up already. An unsuccessful send would be one where
    /// the corresponding receiver has already been deallocated. Note that a
    /// return value of `Err` means that the data will never be received, but
    /// a return value of `Ok` does *not* mean that the data will be received.
    /// It is possible for the corresponding receiver to hang up immediately
    /// after this function returns `Ok`.
    pub fn send(self, value: T) -> Result<(), T> {
        let cell = if let Some(cell) = self.0.upgrade() {
            cell
        } else {
            return Err(value);
        };

        if let State::Pending(Some(waker)) = cell.take() {
            waker.wake()
        }

        cell.set(State::Ready(value));
        Ok(())
    }

    /// Returns `true` if the associated [`Receiver`] handle has been dropped.
    ///
    /// A [`Receiver`] is closed when
    /// [`Receiver`] value is dropped.
    ///
    /// If `true` is returned, a call to `send` will always result in an error.    
    pub fn is_closed(&self) -> bool {
        self.0.strong_count() == 0
    }
}

impl<T> Debug for Sender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
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

/// Creates a new one-shot channel for sending single values across asynchronous
/// tasks.
///
/// The function returns separate "send" and "receive" handles. The `Sender`
/// handle is used by the producer to send the value. The `Receiver` handle is
/// used by the consumer to receive the value.
///
/// Each handle can be used on separate fibers.
///
/// See [`super::oneshot`] for examples.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let cell = Cell::new(State::default());
    let strong = Rc::from(cell);
    let weak = Rc::downgrade(&strong);
    (Sender(weak), Receiver(strong))
}

#[cfg(feature = "tarantool_test")]
mod tests {
    use super::*;
    use crate::fiber;
    use futures::join;
    use std::time::Duration;

    #[crate::test]
    fn drop_receiver() {
        let (tx, rx) = channel::<i32>();
        assert!(!tx.is_closed());
        drop(rx);
        assert!(tx.is_closed());
        assert_eq!(tx.send(0).unwrap_err(), 0);
    }

    #[crate::test]
    fn drop_sender() {
        let (tx, rx) = channel::<i32>();
        assert!(!rx.is_closed());
        drop(tx);
        assert!(rx.is_closed());
        assert_eq!(fiber::block_on(rx).unwrap_err(), RecvError);
    }

    #[crate::test]
    fn receive_non_blocking() {
        let (tx, rx) = channel::<i32>();
        tx.send(56).unwrap();
        assert_eq!(fiber::block_on(rx), Ok(56));
    }

    #[crate::test]
    fn receive_non_blocking_after_dropping_sender() {
        let (tx, rx) = channel::<i32>();
        drop(tx);
        assert_eq!(fiber::block_on(rx), Err(RecvError));
    }

    #[crate::test]
    fn receive_blocking_before_sending() {
        let (tx, rx) = channel::<i32>();
        let jh = fiber::start_async(rx);
        tx.send(39).unwrap();
        assert_eq!(jh.join(), Ok(39));
    }

    #[crate::test]
    fn receive_blocking_before_dropping_sender() {
        let (tx, rx) = channel::<i32>();
        let jh = fiber::start_async(rx);
        drop(tx);
        assert_eq!(jh.join(), Err(RecvError));
    }

    #[crate::test]
    fn join_two_after_sending() {
        let f = async {
            let (tx1, rx1) = channel::<i32>();
            let (tx2, rx2) = channel::<i32>();

            tx1.send(101).unwrap();
            tx2.send(102).unwrap();
            join!(rx1, rx2)
        };
        assert_eq!(fiber::block_on(f), (Ok(101), Ok(102)));
    }

    #[crate::test]
    fn join_two_before_sending() {
        let c = fiber::Cond::new();
        drop(c);

        let (tx1, rx1) = channel::<i32>();
        let (tx2, rx2) = channel::<i32>();

        let jh = fiber::start_async(async { join!(rx1, rx2) });

        tx1.send(201).unwrap();
        fiber::sleep(Duration::ZERO);
        tx2.send(202).unwrap();
        assert_eq!(jh.join(), (Ok(201), Ok(202)));
    }

    #[crate::test]
    fn join_two_drop_one() {
        let (tx1, rx1) = channel::<i32>();
        let (tx2, rx2) = channel::<i32>();

        let jh = fiber::start_async(async { join!(rx1, rx2) });
        tx1.send(301).unwrap();
        fiber::sleep(Duration::ZERO);
        drop(tx2);
        assert_eq!(jh.join(), (Ok(301), Err(RecvError)));
    }
}
