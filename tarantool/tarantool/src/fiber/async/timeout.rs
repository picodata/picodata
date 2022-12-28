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
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Timeout<F> {
    future: F,
    deadline: Option<Instant>,
}

/// Requires a `Future` to complete before the specified duration has elapsed.
///
/// If the future completes before the duration has elapsed, then the completed
/// value is returned. Otherwise, an error is returned and the future is
/// canceled.
///
/// A `timeout` equal to [`Duration::ZERO`] guarantees that awaiting this future
/// will **not** result in a fiber yield.
///
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
#[inline]
pub fn timeout<F: Future>(timeout: Duration, f: F) -> Timeout<F> {
    Timeout {
        future: f,
        deadline: Instant::now().checked_add(timeout),
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
            return Poll::Ready(Ok(v));
        }

        // Then check deadline and, if necessary, update wakup condition
        // in the context.
        match deadline {
            Some(deadline) if Instant::now() >= deadline => {
                Poll::Ready(Err(Expired)) // expired
            }
            Some(deadline) => {
                // SAFETY: This is safe as long as the `Context` really
                // is the `ContextExt`. It's always true within provided
                // `block_on` async runtime.
                unsafe { ContextExt::set_deadline(cx, deadline) };
                Poll::Pending
            }
            None => {
                // No deadline, wait forever
                Poll::Pending
            }
        }
    }
}

/// Futures implementing this trait can be constrained with a timeout (see
/// [`Timeout`]).
///
/// **NOTE**: this trait is implemented for all type implementing
/// [`std::future::Future`], but it must be used **only** with futures from
/// [`crate::fiber::async`] otherwise the behaviour is undefined.
pub trait IntoTimeout: Future + Sized {
    /// Adds timeout to a future. See [`Timeout`].
    #[inline]
    fn timeout(self, timeout: Duration) -> Timeout<Self> {
        super::timeout::timeout(timeout, self)
    }
}

impl<T> IntoTimeout for T where T: Future + Sized {}

#[cfg(feature = "tarantool_test")]
mod tests {
    use super::*;
    use crate::fiber;
    use crate::fiber::check_yield;
    use crate::fiber::r#async::{oneshot, RecvError};
    use crate::fiber::YieldResult::{DidntYield, Yielded};
    use crate::test::{TestCase, TESTS};
    use crate::test_name;
    use linkme::distributed_slice;
    use std::time::Duration;

    const _0_SEC: Duration = Duration::ZERO;
    const _1_SEC: Duration = Duration::from_secs(1);

    #[distributed_slice(TESTS)]
    static INSTANT_FUTURE: TestCase = TestCase {
        name: test_name!("instant_future"),
        f: || {
            let fut = async { 78 };
            assert_eq!(fiber::block_on(fut), 78);

            let fut = timeout(Duration::ZERO, async { 79 });
            assert_eq!(fiber::block_on(fut), Ok(79));
        },
    };

    #[distributed_slice(TESTS)]
    static ACTUAL_TIMEOUT_PROMISE: TestCase = TestCase {
        name: test_name!("actual_timeout_promise"),
        f: || {
            let (tx, rx) = oneshot::channel::<i32>();
            let fut = async move { rx.timeout(_0_SEC).await };

            let jh = fiber::start_async(fut);
            assert_eq!(jh.join(), Err(Expired));
            drop(tx);
        },
    };

    #[distributed_slice(TESTS)]
    static DROP_TX_BEFORE_TIMEOUT: TestCase = TestCase {
        name: test_name!("drop_tx_before_timeout"),
        f: || {
            let (tx, rx) = oneshot::channel::<i32>();
            let fut = async move { rx.timeout(_1_SEC).await };

            let jh = fiber::start(move || fiber::block_on(fut));
            drop(tx);
            assert_eq!(jh.join(), Ok(Err(RecvError)));
        },
    };

    #[distributed_slice(TESTS)]
    static SEND_TX_BEFORE_TIMEOUT: TestCase = TestCase {
        name: test_name!("send_tx_before_timeout"),
        f: || {
            let (tx, rx) = oneshot::channel::<i32>();
            let fut = async move { rx.timeout(_1_SEC).await };

            let jh = fiber::start(move || fiber::block_on(fut));
            tx.send(400).unwrap();
            assert_eq!(jh.join(), Ok(Ok(400)));
        },
    };

    #[distributed_slice(TESTS)]
    static TIMEOUT_DURATION_MAX: TestCase = TestCase {
        name: test_name!("timeout_duration_max"),
        f: || {
            // must not panic
            fiber::block_on(timeout(Duration::MAX, async { 1 })).unwrap();
        },
    };

    #[distributed_slice(TESTS)]
    static AWAIT_ACTUALLY_YIELDS: TestCase = TestCase {
        name: test_name!("await_actually_yields"),
        f: || {
            // ready future, no timeout -> no yield
            assert_eq!(
                check_yield(|| fiber::block_on(async { 101 })),
                DidntYield(101)
            );

            // ready future, 0 timeout -> no yield
            assert_eq!(
                check_yield(|| fiber::block_on(timeout(Duration::ZERO, async { 202 }))),
                DidntYield(Ok(202))
            );

            // ready future, positive timeout -> no yield
            assert_eq!(
                check_yield(|| fiber::block_on(timeout(Duration::from_secs(1), async { 303 }))),
                DidntYield(Ok(303))
            );

            // pending future, no timeout -> yield
            let (_tx, rx) = oneshot::channel::<i32>();
            let f = check_yield(|| fiber::start(|| fiber::block_on(rx)));
            // the yield happens as soon as fiber::start is called,
            // but if fiber::block_on didn't yield we wouldn't even get here,
            // so this check is totally legit
            assert!(matches!(f, Yielded(_)));
            // we leak some memory here, but avoid a panic.
            // Don't do this in your code
            std::mem::forget(f);

            // pending future, 0 timeout -> no yield
            let (_tx, rx) = oneshot::channel::<i32>();
            assert_eq!(
                check_yield(|| fiber::block_on(timeout(Duration::ZERO, rx))),
                DidntYield(Err(Expired))
            );

            // pending future, positive timeout -> yield
            let (_tx, rx) = oneshot::channel::<i32>();
            assert_eq!(
                check_yield(|| fiber::block_on(timeout(Duration::from_millis(10), rx))),
                Yielded(Err(Expired))
            );
        },
    };
}
