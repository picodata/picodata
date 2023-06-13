use super::{LCPipe, Message, MessageHop};
use crate::cbus::RecvError;
use crate::fiber::Cond;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// A synchronization component between producers and a consumer.
struct Waker {
    /// synchronize a waker, signal when waker is up to date
    condition: Arc<Cond>,
    /// indicate that waker already up to date
    woken: AtomicBool,
}

impl Waker {
    fn new(cond: Cond) -> Self {
        Self {
            condition: Arc::new(cond),
            woken: AtomicBool::new(false),
        }
    }

    /// Release waker if it lock in [`Waker::wait`].
    fn wakeup(&self, pipe: &LCPipe) {
        let do_wake = self
            .woken
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if do_wake {
            let hop = MessageHop::new(|b: Box<Message<Arc<Cond>>>| {
                b.user_data().signal();
            });

            let msg = Message::new(hop, Arc::clone(&self.condition));
            pipe.push_message(msg);
        }
    }

    /// Lock until waker is woken up, or return instantly if waker already woken.
    fn wait(&self) {
        while self
            .woken
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            self.condition.wait();
        }
    }
}

/// A unbounded mpsc channel based on tarantool cbus.
/// This a channel between any arbitrary threads (producers) and a cord (consumer).
/// Cord - a thread with `libev` event loop inside (typically tx thread).
struct Channel<T> {
    /// [`crossbeam_queue::SegQueue`] is used as lock free buffer, internally this is a linked list with buckets
    list: crossbeam_queue::SegQueue<T>,
    /// synchronize receiver and producers
    waker: Waker,
    /// indicate that all producers are disconnected from channel
    disconnected: AtomicBool,
}

impl<T> Channel<T> {
    /// Create a new channel.
    fn new() -> Self {
        let cond = Cond::new();
        Self {
            list: crossbeam_queue::SegQueue::new(),
            waker: Waker::new(cond),
            disconnected: AtomicBool::new(false),
        }
    }
}

impl<T> Default for Channel<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Creates a new unbounded channel, returning the sender/receiver halves. Please note that the receiver should only be used inside the cord.
///
/// # Arguments
///
/// * `cbus_endpoint`: cbus endpoint name. Note that the tx thread (or any other cord)
/// must have a fiber occupied by the endpoint cbus_loop.
///
/// returns: (Sender<T>, Receiver<T>)
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
    let chan = Arc::new(Channel::new());
    let s = SenderInner {
        chan: Arc::clone(&chan),
        pipe: LCPipe::new(cbus_endpoint),
    };
    let r = EndpointReceiver {
        chan: Arc::clone(&chan),
    };
    (Sender { inner: Arc::new(s) }, r)
}

struct SenderInner<T> {
    chan: Arc<Channel<T>>,
    pipe: LCPipe,
}

unsafe impl<T> Send for SenderInner<T> {}

impl<T> Drop for SenderInner<T> {
    fn drop(&mut self) {
        self.chan.disconnected.store(true, Ordering::Release);
        self.chan.waker.wakeup(&self.pipe);
    }
}

/// A sending-half of unbounded channel. Can be used in any context (tarantool cord or arbitrary thread).
/// Messages can be sent through this channel with [`Sender::send`].
/// Clone the sender if you need one more producer.
#[derive(Clone)]
pub struct Sender<T> {
    inner: Arc<SenderInner<T>>,
}

unsafe impl<T> Send for Sender<T> {}

impl<T> Sender<T> {
    /// Attempts to send a value on this channel.
    ///
    /// # Arguments
    ///
    /// * `message`: message to send
    pub fn send(&self, msg: T) {
        self.inner.chan.list.push(msg);
        // wake up a sleeping receiver
        self.inner.chan.waker.wakeup(&self.inner.pipe);
    }
}

/// Receiver part of unbounded channel. Must be used in cord context.
pub struct EndpointReceiver<T> {
    chan: Arc<Channel<T>>,
}

unsafe impl<T> Send for EndpointReceiver<T> {}

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

            self.chan.waker.wait();
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
                tx.send(i);
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
    pub fn unbounded_disconnect_test() {
        let mut cbus_fiber = run_cbus_endpoint("unbounded_disconnect_test");

        let (tx, rx) = unbounded::channel("unbounded_disconnect_test");

        let thread = thread::spawn(move || {
            tx.send(1);
            tx.send(2);
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
                    sender.send(i);
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
