#![cfg(any(feature = "tokio_components", doc))]

use crate::cbus::{LCPipe, RecvError, SendError};
use crate::fiber::Cond;
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::thread;
use std::time::Duration;
use tokio::sync::Notify;

type CordWaker = crate::cbus::unbounded::Waker;

/// A synchronization component between producers (a tokio task) and a consumer (a cord).
/// The responsibility of this component is to wake up a producer when it's blocked because
/// channel internal buffer is full.
struct TaskWaker {
    notify: Notify,
}

impl TaskWaker {
    fn new() -> Self {
        Self {
            notify: Notify::default(),
        }
    }

    /// Lock until waker is woken up.
    /// In context of sync-channels, return from this function mean that there's some free
    /// space in message buffer, or receiver is disconnected.
    async fn wait(&self, disconnected: &AtomicBool) {
        if disconnected.load(Ordering::Acquire) {
            return;
        }

        //  If `Notify::notified` happens (called) after `Notify::notify_waiters` then it's not return
        // immediately (instead of situation when `notified` called after `notify_one`). For this case
        // a `timeout` is using, for prevent a deadlock.
        while (tokio::time::timeout(Duration::from_millis(10), self.notify.notified()).await)
            .is_err()
        {}
    }

    /// Send wakeup signal to a single [`TaskWaker::wait`] caller.
    fn wakeup_one(&self) {
        self.notify.notify_one();
    }

    /// Send wakeup signal to all [`TaskWaker::wait`] callers.
    fn wakeup_all(&self) {
        self.notify.notify_waiters();
    }
}

/// A synchronous mpsc channel based on tarantool cbus.
struct Channel<T> {
    list: crossbeam_queue::ArrayQueue<T>,
    disconnected: AtomicBool,
    cbus_endpoint: String,
}

impl<T> Channel<T> {
    /// Create a new channel.
    ///
    /// # Arguments
    ///
    /// * `cbus_endpoint`: cbus endpoint name.
    /// * `cap`: specifies the buffer size.
    fn new(cbus_endpoint: &str, cap: NonZeroUsize) -> Self {
        Self {
            list: crossbeam_queue::ArrayQueue::new(cap.into()),
            disconnected: AtomicBool::new(false),
            cbus_endpoint: cbus_endpoint.to_string(),
        }
    }
}

/// Creates a new synchronous channel, returning the sender/receiver halves.
/// Please note that the receiver should only be used inside the cord.
///
/// Like asynchronous [`channel`]s ([`crate::cbus::unbounded::channel`]),
/// the [`EndpointReceiver`] will block until a message becomes
/// available. Synchronous channel differs greatly in the semantics of the sender, however.
///
/// This channel has an internal buffer on which messages will be queued.
/// `cap` specifies the buffer size. When the internal buffer becomes full,
/// future sends will *block* waiting for the buffer to open up.
///
/// # Arguments
///
/// * `cbus_endpoint`: cbus endpoint name. Note that the tx thread (or any other cord)
/// must have a fiber occupied by the endpoint cbus_loop.
/// * `cap`: specifies the buffer size.
///
/// # Examples
///
/// ```no_run
/// #[cfg(feature = "picodata")] {
/// use tarantool::cbus::sync::tokio::channel;
/// use std::num::NonZeroUsize;
/// let (sender, receiver) = channel::<u8>("some_endpoint", NonZeroUsize::new(100).unwrap());
/// }
/// ```
pub fn channel<T>(cbus_endpoint: &str, cap: NonZeroUsize) -> (Sender<T>, EndpointReceiver<T>) {
    let chan = Arc::new(Channel::new(cbus_endpoint, cap));
    let waker = Arc::new(CordWaker::new(Cond::new()));
    let arc_guard = Arc::new(tokio::sync::Mutex::default());
    let task_waker = Arc::new(TaskWaker::new());
    let s = Sender {
        inner: Arc::new(SenderInner {
            chan: Arc::clone(&chan),
        }),
        cord_waker: Arc::downgrade(&waker),
        task_waker: Arc::clone(&task_waker),
        lcpipe: RefCell::new(LCPipe::new(&chan.cbus_endpoint)),
        arc_guard: Arc::clone(&arc_guard),
    };
    let r = EndpointReceiver {
        chan: Arc::clone(&chan),
        cord_waker: Some(Arc::clone(&waker)),
        task_waker: Arc::clone(&task_waker),
        arc_guard,
    };
    (s, r)
}

struct SenderInner<T> {
    chan: Arc<Channel<T>>,
}

unsafe impl<T> Send for SenderInner<T> {}

impl<T> Drop for SenderInner<T> {
    fn drop(&mut self) {
        self.chan.disconnected.store(true, Ordering::Release);
    }
}

/// A sending-half of a channel. Can be used in tokio task context.
/// Messages can be sent through this channel with [`Sender::send`].
/// Clone the sender if you need one more producer.
pub struct Sender<T> {
    /// a "singleton" part of sender, drop of this part means that all sender's are dropped and
    /// receiver must return [`RecvError::Disconnected`] on `recv`
    inner: Arc<SenderInner<T>>,
    /// synchronize receiver and producers (send wakeup messages from producer to receiver),
    /// using weak ref here cause drop `Waker` outside of cord thread lead to segfault
    cord_waker: Weak<CordWaker>,
    /// synchronize receiver and producers (send wakeup messages from receiver to producer)
    task_waker: Arc<TaskWaker>,
    /// an LCPipe instance, unique for each sender
    lcpipe: RefCell<LCPipe>,
    /// This mutex used for create a critical that guards an invariant - when sender upgrade
    /// `Weak<Waker>` reference there is two `Arc<Waker>` in the same moment of time (in this case
    /// `Waker` always dropped at receiver side) or `Weak<Waker>::upgrade` returns `None`. Compliance
    /// with this invariant guarantees that the `Cond` always dropped at receiver (TX thread) side.
    arc_guard: Arc<tokio::sync::Mutex<()>>,
}

unsafe impl<T> Send for Sender<T> {}

unsafe impl<T> Sync for Sender<T> {}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let crit = self.arc_guard.clone();
        let cord_waker = self.cord_waker.clone();
        let lcpipe: &mut LCPipe = &mut self.lcpipe.borrow_mut();

        // we use a separate thread because blocking_lock will panics if called from tokio runtime
        thread::scope(move |s| {
            s.spawn(move || {
                let _crit_section = crit.blocking_lock();
                if let Some(waker) = cord_waker.upgrade() {
                    waker.wakeup(lcpipe);
                }
            });
        });
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            cord_waker: self.cord_waker.clone(),
            task_waker: self.task_waker.clone(),
            lcpipe: RefCell::new(LCPipe::new(&self.inner.chan.cbus_endpoint)),
            arc_guard: self.arc_guard.clone(),
        }
    }
}

impl<T> Sender<T> {
    /// Attempts to send a value on this channel, returning it back if it could
    /// not be sent (in case when receiver half is closed). If channel buffer is full then
    /// current task sleep until it's freed.
    ///
    /// Note that a return value of [`Err`] means that the data will never be
    /// received, but a return value of [`Ok`] does *not* mean that the data
    /// will be received. It is possible for the corresponding receiver to
    /// hang up immediately after this function returns [`Ok`].
    ///
    /// # Arguments
    ///
    /// * `message`: message to send
    pub async fn send(&self, msg: T) -> Result<(), SendError<T>> {
        let mut msg = msg;
        // We assume that this lock has a minimal impact on performance, in most of situations
        // lock of mutex will take the fast path.

        loop {
            let crit_section = self.arc_guard.lock().await;
            if let Some(waker) = self.cord_waker.upgrade() {
                let push_result = self.inner.chan.list.push(msg);
                if let Err(not_accepted_msg) = push_result {
                    // if buffer is full - end critical section for cord waker and wait until buffer is freed
                    // go to the next iteration then
                    drop(waker);
                    drop(crit_section);
                    self.task_waker.wait(&self.inner.chan.disconnected).await;
                    if self.inner.chan.disconnected.load(Ordering::Acquire) {
                        return Err(SendError(not_accepted_msg));
                    }
                    msg = not_accepted_msg;
                } else {
                    waker.wakeup(&mut self.lcpipe.borrow_mut());
                    return Ok(());
                }
            } else {
                return Err(SendError(msg));
            }
        }
    }
}

/// Receiver part of synchronous channel. Must be used in cord context.
pub struct EndpointReceiver<T> {
    chan: Arc<Channel<T>>,
    cord_waker: Option<Arc<CordWaker>>,
    task_waker: Arc<TaskWaker>,
    arc_guard: Arc<tokio::sync::Mutex<()>>,
}

// The receiver part can be sent from place to place, so long as it
// is not used to receive non-sendable things.
unsafe impl<T> Send for EndpointReceiver<T> {}

impl<T> Drop for EndpointReceiver<T> {
    fn drop(&mut self) {
        self.chan.disconnected.store(true, Ordering::Release);
        self.task_waker.wakeup_all();
        let _crit_section = self.arc_guard.blocking_lock();
        drop(self.cord_waker.take());
    }
}

impl<T> EndpointReceiver<T> {
    /// Attempts to wait for a value on this receiver, returns a [`RecvError::Disconnected`]
    /// when all of producers are dropped.
    pub fn receive(&self) -> Result<T, RecvError> {
        loop {
            if let Some(msg) = self.chan.list.pop() {
                self.task_waker.wakeup_one();
                return Ok(msg);
            }

            if self.chan.disconnected.load(Ordering::Acquire) {
                return Err(RecvError::Disconnected);
            }

            self.cord_waker
                .as_ref()
                .expect("unreachable: waker must exists")
                .wait();
        }
    }

    /// Return message count in receiver buffer.
    pub fn len(&self) -> usize {
        self.chan.list.len()
    }

    /// Return true if receiver message buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(feature = "internal_test")]
mod tests {
    use crate::cbus::sync;
    use crate::cbus::tests::run_cbus_endpoint;
    use crate::cbus::RecvError;
    use crate::fiber;
    use crate::fiber::{check_yield, YieldResult};
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;
    use std::time::Duration;

    #[crate::test(tarantool = "crate")]
    pub fn single_producer() {
        let mut cbus_fiber = run_cbus_endpoint("tokio_single_producer");

        let cap = NonZeroUsize::new(10).unwrap();
        let (tx, rx) = sync::tokio::channel("tokio_single_producer", cap);

        let tokio_rt = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    for i in 0..1000 {
                        _ = tx.send(i).await;
                        if i % 100 == 0 {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                });
        });

        assert_eq!(
            check_yield(|| {
                let mut recv_results = vec![];
                for _ in 0..1000 {
                    recv_results.push(rx.receive().unwrap());
                }
                recv_results
            }),
            YieldResult::Yielded((0..1000).collect::<Vec<_>>())
        );

        tokio_rt.join().unwrap();
        cbus_fiber.cancel();
    }

    #[crate::test(tarantool = "crate")]
    pub fn single_producer_lock() {
        let mut cbus_fiber = run_cbus_endpoint("tokio_single_producer_lock");

        static SEND_COUNTER: AtomicU64 = AtomicU64::new(0);

        let cap = NonZeroUsize::new(10).unwrap();
        let (tx, rx) = sync::tokio::channel("tokio_single_producer_lock", cap);

        let tokio_rt = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    for i in 0..100 {
                        _ = tx.send(i).await;
                        SEND_COUNTER.fetch_add(1, Ordering::SeqCst);
                    }
                });
        });

        fiber::sleep(Duration::from_millis(100));

        let mut recv_results = vec![];
        for i in 0..10 {
            // assert that sender write 10 messages on each iteration and sleep
            assert_eq!(SEND_COUNTER.load(Ordering::SeqCst), (i + 1) * 10);
            for _ in 0..10 {
                recv_results.push(rx.receive().unwrap());
            }
            fiber::sleep(Duration::from_millis(10));
        }

        assert_eq!((0..100).collect::<Vec<_>>(), recv_results);

        tokio_rt.join().unwrap();
        cbus_fiber.cancel();
    }

    #[crate::test(tarantool = "crate")]
    pub fn drop_rx_before_tx() {
        // This test check that there is no memory corruption if sender part of channel drops after
        // receiver part. Previously, when the receiver was drop after sender, [`Fiber::Cond`] release outside the tx thread
        // and segfault is occurred.

        let mut cbus_fiber = run_cbus_endpoint("tokio_drop_rx_before_tx");
        let cap = NonZeroUsize::new(1000).unwrap();
        let (tx, rx) = sync::tokio::channel("tokio_drop_rx_before_tx", cap);

        let tokio_rt = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    for i in 1..300 {
                        _ = tx.send(i).await;
                        if i % 100 == 0 {
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                });
        });

        fiber::sleep(Duration::from_secs(1));
        drop(rx);
        tokio_rt.join().unwrap();

        cbus_fiber.cancel();
    }

    #[crate::test(tarantool = "crate")]
    pub fn tx_disconnect() {
        let mut cbus_fiber = run_cbus_endpoint("tokio_tx_disconnect");

        let cap = NonZeroUsize::new(1).unwrap();
        let (tx, rx) = sync::tokio::channel("tokio_tx_disconnect", cap);

        let tokio_rt = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    _ = tx.send(1).await;
                    _ = tx.send(2).await;
                });
        });

        assert!(matches!(rx.receive(), Ok(1)));
        assert!(matches!(rx.receive(), Ok(2)));
        assert!(matches!(rx.receive(), Err(RecvError::Disconnected)));

        tokio_rt.join().unwrap();
        cbus_fiber.cancel();
    }

    #[crate::test(tarantool = "crate")]
    pub fn rx_disconnect() {
        let mut cbus_fiber = run_cbus_endpoint("tokio_rx_disconnect");

        let cap = NonZeroUsize::new(1).unwrap();
        let (tx, rx) = sync::tokio::channel("tokio_rx_disconnect", cap);

        let tokio_rt = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    assert!(tx.send(1).await.is_ok());
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    // at this point receiver must be dropped and send return an error
                    assert!(tx.send(2).await.is_err());
                });
        });

        assert!(matches!(rx.receive(), Ok(1)));
        drop(rx);

        tokio_rt.join().unwrap();
        cbus_fiber.cancel();
    }

    #[crate::test(tarantool = "crate")]
    pub fn multiple_producer() {
        const MESSAGES_PER_PRODUCER: i32 = 10_000;
        let mut cbus_fiber = run_cbus_endpoint("tokio_multiple_producer");

        let cap = NonZeroUsize::new(10).unwrap();
        let (tx, rx) = sync::tokio::channel("tokio_multiple_producer", cap);

        let tokio_rt = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let mut handles = vec![];

                    for _ in 0..3 {
                        let sender = tx.clone();
                        let jh = tokio::spawn(async move {
                            for i in 0..MESSAGES_PER_PRODUCER {
                                _ = sender.send(i).await;
                            }
                        });
                        handles.push(jh);
                    }

                    for h in handles {
                        h.await.unwrap();
                    }
                });
        });

        for _ in 0..MESSAGES_PER_PRODUCER * 3 {
            assert!(matches!(rx.receive(), Ok(_)));
        }
        assert!(matches!(rx.receive(), Err(RecvError::Disconnected)));

        tokio_rt.join().unwrap();
        cbus_fiber.cancel();
    }

    #[crate::test(tarantool = "crate")]
    pub fn multiple_producer_lock() {
        const MESSAGES_PER_PRODUCER: i32 = 100;
        let mut cbus_fiber = run_cbus_endpoint("tokio_multiple_producer_lock");
        let cap = NonZeroUsize::new(10).unwrap();
        let (tx, rx) = sync::tokio::channel("tokio_multiple_producer_lock", cap);

        static SEND_COUNTER: AtomicU64 = AtomicU64::new(0);

        let tokio_rt = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let mut handles = vec![];

                    for _ in 0..3 {
                        let sender = tx.clone();
                        let jh = tokio::spawn(async move {
                            for i in 0..MESSAGES_PER_PRODUCER {
                                _ = sender.send(i).await;
                                SEND_COUNTER.fetch_add(1, Ordering::SeqCst);
                            }
                        });
                        handles.push(jh);
                    }

                    for h in handles {
                        h.await.unwrap();
                    }
                });
        });

        fiber::sleep(Duration::from_millis(100));
        for i in 0..10 * 3 {
            // assert that all threads produce 10 messages and sleep after
            assert_eq!(SEND_COUNTER.load(Ordering::SeqCst), (i + 1) * 10);
            for _ in 0..10 {
                assert!(matches!(rx.receive(), Ok(_)));
            }
            fiber::sleep(Duration::from_millis(100));
        }
        assert!(matches!(rx.receive(), Err(RecvError::Disconnected)));

        tokio_rt.join().unwrap();
        cbus_fiber.cancel();
    }
}
