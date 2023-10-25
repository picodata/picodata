use super::{LCPipe, Message, UnsafeCond};
use crate::cbus::RecvError;
use crate::fiber::Cond;
use std::cell::{RefCell, UnsafeCell};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

/// A oneshot channel based on tarantool cbus. This a channel between any arbitrary thread and a cord.
/// Cord - a thread with `libev` event loop inside (typically tx thread).
struct Channel<T> {
    message: UnsafeCell<Option<T>>,
    /// Condition variable for synchronize consumer (cord) and producer,
    /// using an [`Arc`] instead of raw pointer cause there is a situation
    /// when channel dropped before cbus endpoint receive a cond
    cond: Arc<UnsafeCond>,
    /// Atomic flag, signaled that sender already have a data for receiver
    ready: AtomicBool,
}

unsafe impl<T> Sync for Channel<T> where T: Send {}

unsafe impl<T> Send for Channel<T> where T: Send {}

impl<T> Channel<T> {
    /// Create a new channel.
    fn new() -> Self {
        Self {
            message: UnsafeCell::new(None),
            ready: AtomicBool::new(false),
            cond: Arc::new(UnsafeCond(Cond::new())),
        }
    }
}

/// A sending-half of oneshot channel. Can be used in any context (tarantool cord or arbitrary thread).
/// Messages can be sent through this channel with [`Sender::send`].
///
/// If sender dropped before [`Sender::send`] is calling then [`EndpointReceiver::receive`] will return with [`RecvError::Disconnected`].
/// It is safe to drop sender when [`EndpointReceiver::receive`] is not calling.
pub struct Sender<T> {
    channel: Weak<Channel<T>>,
    pipe: RefCell<LCPipe>,
    /// This mutex used for create a critical that guards an invariant - when sender upgrade
    /// `Weak<Channel<T>>` reference there is two `Arc<Channel<T>>` in the same moment of time (in
    /// this case `Cond` in `Channel<T>` always dropped at receiver side) or
    /// `Weak<Channel<T>>::upgrade` returns `None`. Compliance with this invariant guarantees that
    /// the `Cond` always dropped at receiver (TX thread) side.
    arc_guard: Arc<Mutex<()>>,
}

unsafe impl<T> Send for Sender<T> {}

unsafe impl<T> Sync for Sender<T> {}

/// Receiver part of oneshot channel. Must be used in cord context.
pub struct EndpointReceiver<T> {
    channel: Option<Arc<Channel<T>>>,
    arc_guard: Arc<Mutex<()>>,
}

/// Creates a new oneshot channel, returning the sender/receiver halves. Please note that the receiver should only be used inside the cord.
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
/// use tarantool::cbus::oneshot;
/// let (sender, receiver) = oneshot::channel::<u8>("some_endpoint");
/// }
/// ```
pub fn channel<T>(cbus_endpoint: &str) -> (Sender<T>, EndpointReceiver<T>) {
    let channel = Arc::new(Channel::new());
    let arc_guard = Arc::new(Mutex::default());

    (
        Sender {
            channel: Arc::downgrade(&channel),
            pipe: RefCell::new(LCPipe::new(cbus_endpoint)),
            arc_guard: Arc::clone(&arc_guard),
        },
        EndpointReceiver {
            channel: Some(channel),
            arc_guard,
        },
    )
}

impl<T> Sender<T> {
    /// Attempts to send a value on this channel.
    ///
    /// # Arguments
    ///
    /// * `message`: message to send
    pub fn send(self, message: T) {
        // We assume that this lock has a minimal impact on performance, in most of situations
        // lock of mutex will take the fast path.
        let _crit_sect = self.arc_guard.lock().unwrap();

        if let Some(chan) = self.channel.upgrade() {
            unsafe { *chan.message.get() = Some(message) };
            chan.ready.store(true, Ordering::Release);
            // [`Sender`] dropped at this point and [`Cond::signal()`] happens on drop.
            // Another words, [`Cond::signal()`] happens anyway, regardless of the existence of message in the channel.
            // After that, the receiver interprets the lack of a message as a disconnect.
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // We assume that this lock has a minimal impact on performance, in most of situations
        // lock of mutex will take the fast path.
        let _crit_sect = self.arc_guard.lock().unwrap();

        let mb_chan = self.channel.upgrade();
        let mb_cond = mb_chan.map(|chan| chan.cond.clone());
        // at this point we are sure that there is at most one reference to a [`Channel`] - in receiver side,
        // possible reference `mb_chan` will be dropped on previous line (in `map` call)

        if let Some(cond) = mb_cond {
            // ref-counter of `cond` will decrement at endpoint side (typically in tx thread) and not on
            // sender drop, because `cond` moved in callback argument of [`cbus::Message`] and decrement
            // when message is handling
            let msg = Message::new(move || {
                // SAFETY: it is ok to call as_ref() here because this callback will be invoked
                // on the thread that created the channel with this cond
                unsafe { (*cond).as_ref().signal() };
            });
            self.pipe.borrow_mut().push_message(msg);
        }
    }
}

impl<T> EndpointReceiver<T> {
    /// Attempts to wait for a value on this receiver, returns a [`RecvError`]
    /// if the corresponding channel has hung up (sender was dropped).
    pub fn receive(self) -> Result<T, RecvError> {
        let channel = self
            .channel
            .as_ref()
            .expect("unreachable: channel must exists");

        if !channel.ready.swap(false, Ordering::Acquire) {
            // assume that situation when [`crate::fiber::Cond::signal()`] called before
            // [`crate::fiber::Cond::wait()`] and after swap `ready` to false  is never been happen,
            // cause signal and wait both calling in tx thread (or any other cord) and there is now yields between it

            // SAFETY: it is ok to call wait() here because we're on original thread that created the cond
            unsafe {
                (*channel.cond).as_ref().wait();
            }
        }
        unsafe {
            channel
                .message
                .get()
                .as_mut()
                .expect("unexpected null pointer")
                .take()
        }
        .ok_or(RecvError::Disconnected)
    }
}

impl<T> Drop for EndpointReceiver<T> {
    fn drop(&mut self) {
        let _crit_sect = self.arc_guard.lock().unwrap();
        drop(self.channel.take());
    }
}

impl<T> Default for Channel<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "internal_test")]
mod tests {
    use super::super::tests::run_cbus_endpoint;
    use crate::cbus::{oneshot, RecvError};
    use crate::fiber::{check_yield, YieldResult};
    use std::time::Duration;
    use std::{mem, thread};

    #[crate::test(tarantool = "crate")]
    pub fn oneshot_test() {
        let mut cbus_fiber = run_cbus_endpoint("oneshot_test");

        let (sender, receiver) = oneshot::channel("oneshot_test");
        let thread = thread::spawn(move || {
            thread::sleep(Duration::from_secs(1));
            sender.send(1);
        });

        assert_eq!(
            check_yield(|| { receiver.receive().unwrap() }),
            YieldResult::Yielded(1)
        );
        thread.join().unwrap();

        let (sender, receiver) = oneshot::channel("oneshot_test");
        let thread = thread::spawn(move || {
            sender.send(2);
        });
        thread.join().unwrap();

        assert_eq!(
            check_yield(|| { receiver.receive().unwrap() }),
            YieldResult::DidntYield(2)
        );

        cbus_fiber.cancel();
    }

    #[crate::test(tarantool = "crate")]
    pub fn oneshot_multiple_channels_test() {
        let mut cbus_fiber = run_cbus_endpoint("oneshot_multiple_channels_test");

        let (sender1, receiver1) = oneshot::channel("oneshot_multiple_channels_test");
        let (sender2, receiver2) = oneshot::channel("oneshot_multiple_channels_test");

        let thread1 = thread::spawn(move || {
            thread::sleep(Duration::from_secs(1));
            sender1.send("1");
        });

        let thread2 = thread::spawn(move || {
            thread::sleep(Duration::from_secs(2));
            sender2.send("2");
        });

        let result2 = receiver2.receive();
        let result1 = receiver1.receive();

        assert!(matches!(result1, Ok("1")));
        assert!(matches!(result2, Ok("2")));

        thread1.join().unwrap();
        thread2.join().unwrap();
        cbus_fiber.cancel();
    }

    #[crate::test(tarantool = "crate")]
    pub fn oneshot_sender_drop_test() {
        let mut cbus_fiber = run_cbus_endpoint("oneshot_sender_drop_test");

        let (sender, receiver) = oneshot::channel::<()>("oneshot_sender_drop_test");

        let thread = thread::spawn(move || {
            thread::sleep(Duration::from_secs(1));
            mem::drop(sender)
        });

        let result = receiver.receive();
        assert!(matches!(result, Err(RecvError::Disconnected)));

        thread.join().unwrap();
        cbus_fiber.cancel();
    }
}
