//! Allows a future to execute for a maximum amount of time.
//!
//! See [`Timeout`] documentation for more details.
//!
//! [`Timeout`]: struct@Timeout
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use super::context::ContextExt;

/// Error returned by [`Timeout`]
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
#[error("deadline expired")]
pub struct Expired;

/// Future returned by [`timeout`](timeout).
pub struct Timeout<F> {
    future: F,
    deadline: Instant,
}

/// Requires a `Future` to complete before the specified duration has elapsed.
///
/// If the future completes before the duration has elapsed, then the completed
/// value is returned. Otherwise, an error is returned and the future is
/// canceled.
/// ```no_run
/// use tarantool::fiber::r#async::*;
/// use tarantool::fiber;
/// use std::time::Duration;
///
/// let (tx, rx) = oneshot::channel::<i32>();
///
/// // Wrap the future with a `Timeout` set to expire in 10 milliseconds.
/// if let Err(_) = fiber::block_on(timeout::timeout(Duration::from_millis(10), rx)) {
///     println!("did not receive value within 10 ms");
/// }      
/// ```
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
