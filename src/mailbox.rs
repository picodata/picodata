//! Another inter-fiber communication primitive.
//!
//! The type [`Mailbox<T>`] is similar to the `tarantool::fiber::Channel<T>`.
//! But unlike channels which are fixed-size, the mailbox holds messages in a
//! growable `Vec<T>`.
//!

use ::tarantool::fiber;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

/// An inter-fiber communication channel based on `Vec<T>`.
///
/// One can send messages one by one, transferring the ownership.
///
/// ```no_run
/// use picodata::mailbox::Mailbox;
///
/// let mailbox: Mailbox<String> = Mailbox::new();
/// mailbox.send("Hello".into());
/// mailbox.send("World".into());
/// ```
///
/// Messages are delivered in the same order as they're sent.
///
/// Receiving them is possible in two ways:
///
/// 1. Blocking
///
/// ```no_run
/// # use picodata::mailbox::Mailbox;
/// # use std::time::Duration;
///
/// let mailbox: Mailbox<String> = Mailbox::new();
/// let timeout = Duration::from_secs(1);
///
/// mailbox.send("Hello".into());
/// mailbox.send("World".into());
///
/// // Won't yield as mailbox isn't empty.
/// assert_eq!(mailbox.receive_all(timeout), vec!["Hello", "World"]);
///
/// // Will yield and subsequently timeout.
/// assert!(mailbox.receive_all(timeout).is_empty());
/// ```
///
/// 2. Non-blocking
///
/// ```no_run
/// # use picodata::mailbox::Mailbox;
///
/// let mailbox: Mailbox<()> = Mailbox::new();
///
/// // Won't ever yield.
/// assert!(mailbox.try_receive_all().is_empty());
/// ```

#[derive(Default)]
pub struct Mailbox<T>(Rc<Inner<T>>);

#[derive(Default)]
struct Inner<T> {
    cond: Rc<fiber::Cond>,
    content: RefCell<Vec<T>>,
}

impl<T> Clone for Mailbox<T> {
    /// Shares access to the existing mailbox, thereby enabling
    /// inter-fiber communication. The conent itself isn't cloned.
    ///
    /// # Example
    /// ```no_run
    /// use picodata::mailbox::Mailbox;
    ///
    /// let original: Mailbox<i32> = Mailbox::new();
    /// let cloned: Mailbox<i32> = original.clone();
    /// cloned.send(55);
    /// assert_eq!(original.try_receive_all(), vec![55]);
    /// ```
    ///
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> ::std::fmt::Debug for Mailbox<T> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        let typ = format!("Mailbox<{}>", std::any::type_name::<T>());
        f.debug_struct(&typ).finish_non_exhaustive()
    }
}

impl<T> Mailbox<T> {
    /// Creates a new, empty mailbox.
    pub fn new() -> Self {
        Self(Rc::new(Inner {
            cond: Rc::default(),
            content: RefCell::new(Vec::new()),
        }))
    }

    /// Creates an empty mailbox using the provided `fiber::Cond`. This
    /// allows sharing the same `cond` in several places.
    pub fn with_cond(cond: Rc<fiber::Cond>) -> Self {
        Self(Rc::new(Inner {
            cond,
            content: RefCell::default(),
        }))
    }

    /// Puts `value` into the mailbox and wakes up a pending recipient.
    ///
    /// The mailbox size is unlimited, so sending a message never blocks
    /// the calling fiber.
    ///
    pub fn send(&self, v: T) {
        self.0.content.borrow_mut().push(v);
        self.0.cond.signal();
    }

    /// Checks the mailbox for incoming messages without blocking the
    /// caller.
    ///
    /// Always returns a `Vec<T>`, either empty or not.
    ///
    /// # Examples
    /// ```no_run
    /// use picodata::mailbox::Mailbox;
    ///
    /// let mailbox: Mailbox<i32> = Mailbox::new();
    /// mailbox.send(9);
    ///
    /// assert_eq!(mailbox.try_receive_all(), vec![9]); // doesn't yield
    /// assert!(mailbox.try_receive_all().is_empty()); // doesn't yield either
    /// ```
    pub fn try_receive_all(&self) -> Vec<T> {
        self.0.content.take()
    }

    /// Checks the mailbox for incoming messages. If there're none, puts
    /// the calling fiber into sleep and yields. The fiber is resumed in
    /// any of these events:
    ///
    /// - Some other fiber sends a message using [`Self::send`].
    /// - Some other fiber signals a `cond` provided in
    ///   [`Self::with_cond`].
    /// - Upon expiration of the `timeout`.
    ///
    /// # Yields
    ///
    /// This function will yield if the mailbox is empty. Even if
    /// `timeout` is zero. It won't yield if there're some messages.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use picodata::mailbox::Mailbox;
    /// use std::time::Duration;
    ///
    /// let mailbox: Mailbox<i32> = Mailbox::new();
    /// let timeout = Duration::from_secs(1);
    ///
    /// mailbox.send(7);
    /// assert_eq!(mailbox.receive_all(timeout), vec![7]); // doesn't yield
    /// assert!(mailbox.receive_all(timeout).is_empty()); // yields until timeout expires
    /// ```
    ///
    pub fn receive_all(&self, timeout: Duration) -> Vec<T> {
        if self.0.content.borrow().is_empty() {
            self.0.cond.wait_timeout(timeout);
        }

        self.0.content.take()
    }
}

inventory::submit!(crate::InnerTest {
    name: "test_mailbox",
    body: || {
        let mailbox = Mailbox::<u8>::new();
        assert_eq!(mailbox.receive_all(Duration::ZERO), Vec::<u8>::new());
        assert_eq!(mailbox.try_receive_all(), Vec::<u8>::new());

        mailbox.send(1);
        assert_eq!(mailbox.receive_all(Duration::ZERO), vec![1]);

        mailbox.send(2);
        mailbox.send(3);
        assert_eq!(mailbox.receive_all(Duration::ZERO), vec![2, 3]);

        assert_eq!(mailbox.try_receive_all(), Vec::<u8>::new());
        mailbox.send(4);
        mailbox.send(5);
        assert_eq!(mailbox.try_receive_all(), vec![4, 5]);

        assert_eq!(format!("{mailbox:?}"), "Mailbox<u8> { .. }")
    }
});
