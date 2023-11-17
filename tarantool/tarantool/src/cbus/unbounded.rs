use super::{LCPipe, Message, SendError, UnsafeCond};
use crate::cbus::RecvError;
use crate::fiber::Cond;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

/// A synchronization component between producers and a consumer.
pub(super) struct Waker {
    /// synchronize a waker, signal when waker is up to date
    condition: Option<Arc<UnsafeCond>>,
    /// indicate that waker already up to date
    woken: AtomicBool,
}

impl Waker {
    pub(super) fn new(cond: Cond) -> Self {
        Self {
            condition: Some(Arc::new(UnsafeCond(cond))),
            woken: AtomicBool::new(false),
        }
    }

    /// Send wakeup signal to a [`Waker::wait`] caller.
    pub(super) fn force_wakeup(&self, cond: Arc<UnsafeCond>, pipe: &mut LCPipe) {
        let msg = Message::new(move || {
            // SAFETY: it is ok to call as_ref() here because this callback will be invoked
            // on the thread that created the channel with this cond
            unsafe { (*cond).as_ref().signal() };
        });
        pipe.push_message(msg);
    }

    /// Release waker if it lock in [`Waker::wait`].
    pub(super) fn wakeup(&self, pipe: &mut LCPipe) {
        let do_wake = self
            .woken
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if do_wake {
            let cond = Arc::clone(
                self.condition
                    .as_ref()
                    .expect("unreachable: condition never empty"),
            );
            self.force_wakeup(cond, pipe);
        }
    }

    /// Lock until waker is woken up, or return instantly if waker already woken.
    pub(super) fn wait(&self) {
        if self
            .woken
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            let cond = self
                .condition
                .as_ref()
                .expect("unreachable: condition never empty");

            // SAFETY: it is ok to call wait() here because we're on original thread that created the cond
            unsafe { (**cond).as_ref().wait_timeout(Duration::from_millis(1)) };
        }
    }
}

/// A unbounded mpsc channel based on tarantool cbus.
/// This a channel between any arbitrary threads (producers) and a cord (consumer).
/// Cord - a thread with `libev` event loop inside (typically tx thread).
struct Channel<T> {
    /// [`crossbeam_queue::SegQueue`] is used as lock free buffer, internally this is a linked list with buckets
    list: crossbeam_queue::SegQueue<T>,
    /// indicate that all producers are disconnected from channel
    disconnected: AtomicBool,
    /// name of a cbus endpoint, using for create an LCPipe instances
    cbus_endpoint: String,
}

impl<T> Channel<T> {
    /// Create a new channel.
    ///
    /// # Arguments
    ///
    /// * `cbus_endpoint`: cbus endpoint name.
    fn new(cbus_endpoint: &str) -> Self {
        Self {
            list: crossbeam_queue::SegQueue::new(),
            disconnected: AtomicBool::new(false),
            cbus_endpoint: cbus_endpoint.to_string(),
        }
    }
}

/// Creates a new unbounded channel, returning the sender/receiver halves. Please note that the receiver should only be used inside the cord.
///
/// # Arguments
///
/// * `cbus_endpoint`: cbus endpoint name. Note that the tx thread (or any other cord)
/// must have a fiber occupied by the endpoint cbus_loop.
///
/// # Examples
///
/// ```no_run
/// #[cfg(feature = "picodata")] {
/// use tarantool::cbus::unbounded;
/// let (sender, receiver) = unbounded::channel::<u8>("some_endpoint");
/// }
/// ```
pub fn channel<T>(cbus_endpoint: &str) -> (Sender<T>, EndpointReceiver<T>) {
    let chan = Arc::new(Channel::new(cbus_endpoint));
    let waker = Arc::new(Waker::new(Cond::new()));
    let arc_guard = Arc::new(Mutex::default());
    let s = Sender {
        inner: Arc::new(SenderInner {
            chan: Arc::clone(&chan),
        }),
        waker: Arc::downgrade(&waker),
        lcpipe: RefCell::new(LCPipe::new(&chan.cbus_endpoint)),
        arc_guard: Arc::clone(&arc_guard),
    };
    let r = EndpointReceiver {
        chan: Arc::clone(&chan),
        waker: Some(Arc::clone(&waker)),
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

/// A sending-half of unbounded channel. Can be used in any context (tarantool cord or arbitrary thread).
/// Messages can be sent through this channel with [`Sender::send`].
/// Clone the sender if you need one more producer.
pub struct Sender<T> {
    /// a "singleton" part of sender, drop of this part means that all sender's are dropped and
    /// receiver must return [`RecvError::Disconnected`] on `recv`
    inner: Arc<SenderInner<T>>,
    /// synchronize receiver and producers, using weak ref here cause drop `Waker` outside of
    /// cord thread lead to segfault
    waker: Weak<Waker>,
    /// an LCPipe instance, unique for each sender
    lcpipe: RefCell<LCPipe>,
    /// This mutex used for create a critical that guards an invariant - when sender upgrade
    /// `Weak<Waker>` reference there is two `Arc<Waker>` in the same moment of time (in this case
    /// `Waker` always dropped at receiver side) or `Weak<Waker>::upgrade` returns `None`. Compliance
    /// with this invariant guarantees that the `Cond` always dropped at receiver (TX thread) side.
    arc_guard: Arc<Mutex<()>>,
}

unsafe impl<T> Send for Sender<T> {}

unsafe impl<T> Sync for Sender<T> {}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // We assume that this lock has a minimal impact on performance, in most of situations
        // lock of mutex will take the fast path.
        let _crit_section = self.arc_guard.lock().unwrap();

        if let Some(waker) = self.waker.upgrade() {
            waker.wakeup(&mut self.lcpipe.borrow_mut());
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            waker: self.waker.clone(),
            lcpipe: RefCell::new(LCPipe::new(&self.inner.chan.cbus_endpoint)),
            arc_guard: self.arc_guard.clone(),
        }
    }
}

impl<T> Sender<T> {
    /// Attempts to send a value on this channel, returning it back if it could
    /// not be sent.
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
        // We assume that this lock has a minimal impact on performance, in most of situations
        // lock of mutex will take the fast path.
        let _crit_section = self.arc_guard.lock().unwrap();

        // wake up a sleeping receiver
        if let Some(waker) = self.waker.upgrade() {
            self.inner.chan.list.push(msg);
            waker.wakeup(&mut self.lcpipe.borrow_mut());
            Ok(())
        } else {
            Err(SendError(msg))
        }
    }
}

/// Receiver part of unbounded channel. Must be used in cord context.
pub struct EndpointReceiver<T> {
    chan: Arc<Channel<T>>,
    waker: Option<Arc<Waker>>,
    arc_guard: Arc<Mutex<()>>,
}

unsafe impl<T> Send for EndpointReceiver<T> {}

impl<T> Drop for EndpointReceiver<T> {
    fn drop(&mut self) {
        let _crit_section = self.arc_guard.lock().unwrap();
        drop(self.waker.take());
    }
}

impl<T> EndpointReceiver<T> {
    /// Attempts to wait for a value on this receiver, returns a [`RecvError::Disconnected`]
    /// when all of producers are dropped.
    pub fn receive(&self) -> Result<T, RecvError> {
        loop {
            if let Some(msg) = self.chan.list.pop() {
                return Ok(msg);
            }

            if self.chan.disconnected.load(Ordering::Acquire) {
                return Err(RecvError::Disconnected);
            }

            self.waker
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
    use super::super::tests::run_cbus_endpoint;
    use crate::cbus::{unbounded, RecvError};
    use crate::fiber;
    use crate::fiber::{check_yield, YieldResult};
    use std::thread;
    use std::thread::JoinHandle;
    use std::time::Duration;

    #[crate::test(tarantool = "crate")]
    pub fn unbounded_test() {
        let mut cbus_fiber = run_cbus_endpoint("unbounded_test");

        let (tx, rx) = unbounded::channel("unbounded_test");

        let thread = thread::spawn(move || {
            for i in 0..1000 {
                _ = tx.send(i);
                if i % 100 == 0 {
                    thread::sleep(Duration::from_millis(1000));
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
        cbus_fiber.cancel();
    }

    #[crate::test(tarantool = "crate")]
    pub fn unbounded_test_drop_rx_before_tx() {
        // This test check that there is no memory corruption if sender part of channel drops after
        // receiver part. Previously, when the receiver was drop after sender, [`Fiber::Cond`] release outside the tx thread
        // and segfault is occurred.

        let mut cbus_fiber = run_cbus_endpoint("unbounded_test_drop_rx_before_tx");
        let (tx, rx) = unbounded::channel("unbounded_test_drop_rx_before_tx");

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

        cbus_fiber.cancel();
    }

    #[crate::test(tarantool = "crate")]
    pub fn unbounded_disconnect_test() {
        let mut cbus_fiber = run_cbus_endpoint("unbounded_disconnect_test");

        let (tx, rx) = unbounded::channel("unbounded_disconnect_test");

        let thread = thread::spawn(move || {
            _ = tx.send(1);
            _ = tx.send(2);
        });

        assert!(matches!(rx.receive(), Ok(1)));
        assert!(matches!(rx.receive(), Ok(2)));
        assert!(matches!(rx.receive(), Err(RecvError::Disconnected)));

        thread.join().unwrap();
        cbus_fiber.cancel();
    }

    #[crate::test(tarantool = "crate")]
    pub fn unbounded_mpsc_test() {
        const MESSAGES_PER_PRODUCER: i32 = 10_000;
        let mut cbus_fiber = run_cbus_endpoint("unbounded_mpsc_test");

        let (tx, rx) = unbounded::channel("unbounded_mpsc_test");

        fn create_producer(sender: unbounded::Sender<i32>) -> JoinHandle<()> {
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
        cbus_fiber.cancel();
    }
}
