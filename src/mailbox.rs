use ::tarantool::fiber;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

struct Inner<T> {
    cond: Rc<fiber::Cond>,
    content: RefCell<Vec<T>>,
}

pub struct Mailbox<T>(Rc<Inner<T>>);

impl<T> Clone for Mailbox<T> {
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
    pub fn new() -> Self {
        Self(Rc::new(Inner {
            cond: Rc::default(),
            content: RefCell::new(Vec::new()),
        }))
    }

    pub fn with_cond(cond: Rc<fiber::Cond>) -> Self {
        Self(Rc::new(Inner {
            cond,
            content: RefCell::default(),
        }))
    }

    pub fn send(&self, v: T) {
        self.0.content.borrow_mut().push(v);
        self.0.cond.signal();
    }

    pub fn try_receive_all(&self) -> Vec<T> {
        self.0.content.take()
    }

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
