use crate::cbus::{LCPipe, RecvError, SendError};
use crate::fiber::Cond;
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{self, Arc, Mutex, Weak};
use std::thread;

type CordWaker = crate::cbus::unbounded::Waker;

/// Current thread process handler.
#[derive(Clone)]
struct Thread {
    inner: thread::Thread,
    flag: Arc<AtomicBool>,
}

impl Thread {
    fn current() -> Self {
        Self {
            inner: thread::current(),
            flag: Arc::new(AtomicBool::new(false)),
        }
    }

    fn park(&self) {
        if !self.flag.load(Ordering::Acquire) {
            thread::park();
        }
    }

    fn unpark(&self) {
        self.flag.store(true, Ordering::Release);
        self.inner.unpark();
    }
}

/// A synchronization component between producers (an OS thread) and a consumer (a cord).
/// The responsibility of this component is to wake up a producer when it's blocked because
/// channel internal buffer is full.
struct ThreadWaker {
    /// A queue of threads that are waiting to send data.
    list: crossbeam_queue::SegQueue<Thread>,
}

impl ThreadWaker {
    fn new() -> Self {
        Self {
            list: crossbeam_queue::SegQueue::new(),
        }
    }

    /// Lock until waker is woken up.
    /// In context of sync-channels, return from this function mean that there's some free
    /// space in message buffer, or receiver is disconnected.
    fn wait(&self, disconnected: &AtomicBool) {
        if disconnected.load(Ordering::Acquire) {
            return;
        }
        let t = Thread::current();
        self.list.push(t.clone());
        t.park();
    }

    /// Send wakeup signal to a single [`ThreadWaker::wait`] caller.
    fn wakeup_one(&self) {
        if let Some(thread) = self.list.pop() {
            thread.unpark();
        }
    }

    /// Send wakeup signal to all [`ThreadWaker::wait`] callers.
    fn wakeup_all(&self) {
        while let Some(thread) = self.list.pop() {
            thread.unpark();
        }
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
/// Like asynchronous [`channel`]s, the [`EndpointReceiver`] will block until a message becomes
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
/// use tarantool::cbus::sync::std::channel;
/// use std::num::NonZeroUsize;
/// let (sender, receiver) = channel::<u8>("some_endpoint", NonZeroUsize::new(100).unwrap());
/// }
/// ```
pub fn channel<T>(cbus_endpoint: &str, cap: NonZeroUsize) -> (Sender<T>, EndpointReceiver<T>) {
    let chan = Arc::new(Channel::new(cbus_endpoint, cap));
    let waker = Arc::new(CordWaker::new(Cond::new()));
    let arc_guard = Arc::new(sync::Mutex::default());
    let thread_waker = Arc::new(ThreadWaker::new());
    let s = Sender {
        inner: Arc::new(SenderInner {
            chan: Arc::clone(&chan),
        }),
        cord_waker: Arc::downgrade(&waker),
        thread_waker: Arc::clone(&thread_waker),
        lcpipe: RefCell::new(LCPipe::new(&chan.cbus_endpoint)),
        arc_guard: Arc::clone(&arc_guard),
    };
    let r = EndpointReceiver {
        chan: Arc::clone(&chan),
        cord_waker: Some(Arc::clone(&waker)),
        thread_waker: Arc::clone(&thread_waker),
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

/// A sending-half of a channel. Can be used in OS thread context (because `send` may block tarantool
/// or tokio runtime).
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
    thread_waker: Arc<ThreadWaker>,
    /// an LCPipe instance, unique for each sender
    lcpipe: RefCell<LCPipe>,
    /// This mutex used for create a critical that guards an invariant - when sender upgrade
    /// `Weak<Waker>` reference there is two `Arc<Waker>` in the same moment of time (in this case
    /// `Waker` always dropped at receiver side) or `Weak<Waker>::upgrade` returns `None`. Compliance
    /// with this invariant guarantees that the `Cond` always dropped at receiver (TX thread) side.
    arc_guard: Arc<sync::Mutex<()>>,
}

unsafe impl<T> Send for Sender<T> {}

unsafe impl<T> Sync for Sender<T> {}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // We assume that this lock has a minimal impact on performance, in most of situations
        // lock of mutex will take the fast path.
        let _crit_section = self.arc_guard.lock().unwrap();

        if let Some(waker) = self.cord_waker.upgrade() {
            waker.wakeup(&mut self.lcpipe.borrow_mut());
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            cord_waker: self.cord_waker.clone(),
            thread_waker: self.thread_waker.clone(),
            lcpipe: RefCell::new(LCPipe::new(&self.inner.chan.cbus_endpoint)),
            arc_guard: self.arc_guard.clone(),
        }
    }
}

impl<T> Sender<T> {
    /// Attempts to send a value on this channel, returning it back if it could
    /// not be sent (in case when receiver half is closed). If channel buffer is full then
    /// current thread sleep until it's freed.
    ///
    /// Note that a return value of [`Err`] means that the data will never be
    /// received, but a return value of [`Ok`] does *not* mean that the data
    /// will be received. It is possible for the corresponding receiver to
    /// hang up immediately after this function returns [`Ok`].
    ///
    /// # Arguments
    ///
    /// * `message`: message to send
    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
        let mut msg = msg;
        loop {
            if self.inner.chan.disconnected.load(Ordering::Acquire) {
                return Err(SendError(msg));
            }
            let crit_section = self.arc_guard.lock().unwrap();
            let Some(waker) = self.cord_waker.upgrade() else {
                return Err(SendError(msg));
            };
            let Err(not_accepted_msg) = self.inner.chan.list.push(msg) else {
                waker.wakeup(&mut self.lcpipe.borrow_mut());
                return Ok(());
            };
            msg = not_accepted_msg;
            drop(crit_section);
            self.thread_waker.wait(&self.inner.chan.disconnected);
        }
    }
}

/// Receiver part of synchronous channel. Must be used in cord context.
pub struct EndpointReceiver<T> {
    chan: Arc<Channel<T>>,
    cord_waker: Option<Arc<CordWaker>>,
    thread_waker: Arc<ThreadWaker>,
    arc_guard: Arc<Mutex<()>>,
}

// The receiver part can be sent from place to place, so long as it
// is not used to receive non-sendable things.
unsafe impl<T> Send for EndpointReceiver<T> {}

impl<T> Drop for EndpointReceiver<T> {
    fn drop(&mut self) {
        self.chan.disconnected.store(true, Ordering::Release);
        self.thread_waker.wakeup_all();
        let _crit_section = self.arc_guard.lock().unwrap();
        drop(self.cord_waker.take());
    }
}

impl<T> EndpointReceiver<T> {
    /// Attempts to wait for a value on this receiver, returns a [`RecvError::Disconnected`]
    /// when all of producers are dropped.
    pub fn receive(&self) -> Result<T, RecvError> {
        loop {
            if let Some(msg) = self.chan.list.pop() {
                self.thread_waker.wakeup_one();
                return Ok(msg);
            }

            if self.chan.disconnected.load(Ordering::Acquire) {
                return Err(RecvError::Disconnected);
            }

            // Need to wake thread so it can push message
            // FIXME: why cord waker waits it's cond for 1ms ?
            self.thread_waker.wakeup_one();
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
#[allow(clippy::redundant_pattern_matching)]
mod tests {
    use crate::cbus::sync;
    use crate::cbus::tests::run_cbus_endpoint;
    use crate::cbus::RecvError;
    use crate::fiber;
    use crate::fiber::{check_yield, YieldResult};
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;
    use std::thread::JoinHandle;
    use std::time::Duration;

    #[crate::test(tarantool = "crate")]
    pub fn single_producer() {
        let cbus_fiber_id = run_cbus_endpoint("std_single_producer");

        let cap = NonZeroUsize::new(10).unwrap();
        let (tx, rx) = sync::std::channel("std_single_producer", cap);

        let thread = thread::spawn(move || {
            for i in 0..1000 {
                _ = tx.send(i);
                if i % 100 == 0 {
                    thread::sleep(Duration::from_millis(100));
                }
            }
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
        thread.join().unwrap();
        assert!(fiber::cancel(cbus_fiber_id));
    }

    #[crate::test(tarantool = "crate")]
    pub fn single_producer_lock() {
        let cbus_fiber_id = run_cbus_endpoint("std_single_producer_lock");

        static SEND_COUNTER: AtomicU64 = AtomicU64::new(0);

        let cap = NonZeroUsize::new(10).unwrap();
        let (tx, rx) = sync::std::channel("std_single_producer_lock", cap);
        let thread = thread::spawn(move || {
            for i in 0..100 {
                _ = tx.send(i);
                SEND_COUNTER.fetch_add(1, Ordering::SeqCst);
            }
        });

        fiber::sleep(Duration::from_millis(100));

        let mut recv_results = vec![];
        for i in 0..10 {
            // assert that sender write 10 messages on each iteration and sleep
            assert_eq!(SEND_COUNTER.load(Ordering::SeqCst), (i + 1) * 10);
            for _ in 0..10 {
                recv_results.push(rx.receive().unwrap());
            }
            fiber::sleep(Duration::from_millis(100));
        }

        assert_eq!((0..100).collect::<Vec<_>>(), recv_results);

        thread.join().unwrap();
        assert!(fiber::cancel(cbus_fiber_id));
    }

    #[crate::test(tarantool = "crate")]
    pub fn drop_rx_before_tx() {
        // This test check that there is no memory corruption if sender part of channel drops after
        // receiver part. Previously, when the receiver was drop after sender, [`Fiber::Cond`] release outside the tx thread
        // and segfault is occurred.

        let cbus_fiber_id = run_cbus_endpoint("std_drop_rx_before_tx");
        let cap = NonZeroUsize::new(1000).unwrap();
        let (tx, rx) = sync::std::channel("std_drop_rx_before_tx", cap);

        let thread = thread::spawn(move || {
            for i in 1..300 {
                _ = tx.send(i);
                if i % 100 == 0 {
                    thread::sleep(Duration::from_secs(1));
                }
            }
        });

        fiber::sleep(Duration::from_secs(1));
        drop(rx);
        thread.join().unwrap();

        assert!(fiber::cancel(cbus_fiber_id));
    }

    #[crate::test(tarantool = "crate")]
    pub fn tx_disconnect() {
        let cbus_fiber_id = run_cbus_endpoint("std_tx_disconnect");

        let cap = NonZeroUsize::new(1).unwrap();
        let (tx, rx) = sync::std::channel("std_tx_disconnect", cap);

        let thread = thread::spawn(move || {
            _ = tx.send(1);
            _ = tx.send(2);
        });

        assert!(matches!(rx.receive(), Ok(1)));
        assert!(matches!(rx.receive(), Ok(2)));
        assert!(matches!(rx.receive(), Err(RecvError::Disconnected)));

        thread.join().unwrap();
        assert!(fiber::cancel(cbus_fiber_id));
    }

    #[crate::test(tarantool = "crate")]
    pub fn rx_disconnect() {
        let cbus_fiber_id = run_cbus_endpoint("std_rx_disconnect");

        let cap = NonZeroUsize::new(1).unwrap();
        let (tx, rx) = sync::std::channel("std_rx_disconnect", cap);

        let thread = thread::spawn(move || {
            assert!(tx.send(1).is_ok());
            thread::sleep(Duration::from_millis(100));
            // at this point receiver must be dropped and send return an error
            assert!(tx.send(2).is_err());
        });

        assert!(matches!(rx.receive(), Ok(1)));
        drop(rx);

        thread.join().unwrap();
        assert!(fiber::cancel(cbus_fiber_id));
    }

    #[crate::test(tarantool = "crate")]
    pub fn multiple_producer() {
        const MESSAGES_PER_PRODUCER: i32 = 10_000;
        let cbus_fiber_id = run_cbus_endpoint("std_multiple_producer");

        let cap = NonZeroUsize::new(10).unwrap();
        let (tx, rx) = sync::std::channel("std_multiple_producer", cap);

        fn create_producer(sender: sync::std::Sender<i32>) -> JoinHandle<()> {
            thread::spawn(move || {
                for i in 0..MESSAGES_PER_PRODUCER {
                    _ = sender.send(i);
                }
            })
        }

        let jh1 = create_producer(tx.clone());
        let jh2 = create_producer(tx.clone());
        let jh3 = create_producer(tx);

        for _ in 0..MESSAGES_PER_PRODUCER * 3 {
            assert!(matches!(rx.receive(), Ok(_)));
        }
        assert!(matches!(rx.receive(), Err(RecvError::Disconnected)));

        jh1.join().unwrap();
        jh2.join().unwrap();
        jh3.join().unwrap();
        assert!(fiber::cancel(cbus_fiber_id));
    }

    #[crate::test(tarantool = "crate")]
    pub fn multiple_producer_lock() {
        const MESSAGES_PER_PRODUCER: i32 = 100;
        let cbus_fiber_id = run_cbus_endpoint("std_multiple_producer_lock");

        let cap = NonZeroUsize::new(10).unwrap();
        let (tx, rx) = sync::std::channel("std_multiple_producer_lock", cap);

        static SEND_COUNTER: AtomicU64 = AtomicU64::new(0);

        fn create_producer(sender: sync::std::Sender<i32>) -> JoinHandle<()> {
            thread::spawn(move || {
                for i in 0..MESSAGES_PER_PRODUCER {
                    _ = sender.send(i);
                    SEND_COUNTER.fetch_add(1, Ordering::SeqCst);
                }
            })
        }

        let jh1 = create_producer(tx.clone());
        let jh2 = create_producer(tx.clone());
        let jh3 = create_producer(tx);

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

        jh1.join().unwrap();
        jh2.join().unwrap();
        jh3.join().unwrap();
        assert!(fiber::cancel(cbus_fiber_id));
    }
}
