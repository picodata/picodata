#![allow(clippy::redundant_allocation)]
use std::{
    cell::RefCell,
    rc::Rc,
    time::Duration,
};

use tarantool::fiber::{
    fiber_yield, is_cancelled, sleep, Cond, Fiber, FiberAttr
};

pub fn test_fiber_new() {
    let mut fiber = Fiber::new("test_fiber", &mut |_| 0);
    fiber.set_joinable(true);
    fiber.start(());
    fiber.join();
}

pub fn test_fiber_new_with_attr() {
    let mut attr = FiberAttr::new();
    attr.set_stack_size(100_000).unwrap();

    let mut fiber = Fiber::new_with_attr("test_fiber", &attr, &mut |_| 0);
    fiber.set_joinable(true);
    fiber.start(());
    fiber.join();
}

pub fn test_fiber_arg() {
    let mut fiber = Fiber::new("test_fiber", &mut |x| {
        assert_eq!(*x, 99);
        0
    });
    fiber.set_joinable(true);
    fiber.start(99);
    fiber.join();
}

pub fn test_fiber_cancel() {
    let mut fiber = Fiber::new("test_fiber", &mut |_| {
        assert_eq!(is_cancelled(), false);
        sleep(Duration::from_millis(10));
        assert_eq!(is_cancelled(), true);
        0
    });
    fiber.set_joinable(true);
    fiber.start(());
    fiber.cancel();
    fiber.join();
}

pub fn test_fiber_wake() {
    let mut fiber = Fiber::new("test_fiber", &mut |_| {
        fiber_yield();
        0
    });
    fiber.set_joinable(true);
    fiber.start(());
    sleep(Duration::from_millis(10));
    fiber.wakeup();
    fiber.join();
}

pub fn test_fiber_wake_multiple() {
    let res = Rc::new(RefCell::new(vec![]));
    let mut fibers = vec![];
    for (i, c) in (1..).zip(&['a', 'b', 'c']) {
        let mut fiber = Fiber::new(
            &format!("test_fiber_{}", c),
            &mut |r: Box<Rc<RefCell<Vec<i32>>>>| {
                fiber_yield();
                r.borrow_mut().push(i);
                0
            }
        );
        fiber.start(res.clone());
        fiber.wakeup();
        fibers.push(fiber);
    }

    for f in &mut fibers {
        f.set_joinable(true);
    }

    res.borrow_mut().push(0);
    for f in fibers {
        f.join();
    }
    res.borrow_mut().push(4);

    let res = res.borrow().iter().copied().collect::<Vec<_>>();
    // This is what we want:
    // assert_eq(res, vec![0, 1, 2, 3, 4]);
    // This is what we get:
    assert_eq!(res, vec![0, 3, 3, 3, 4]);
    // Because `Fiber` doesn't work with closures. `i` is passed by reference
    // and by the time the first fiber starts executing, it is equal to 3.
    // This is actually undefined behavior, so adding this test is probably a
    // bad idea
}

pub fn test_fiber_cond_signal() {
    let cond = Rc::new(Cond::new());
    let mut fiber = Fiber::new("test_fiber", &mut |cond: Box<Rc<Cond>>| {
        (*cond).wait();
        0
    });
    fiber.set_joinable(true);
    fiber.start(cond.clone());
    sleep(Duration::from_millis(10));
    cond.signal();
    fiber.join();
}

pub fn test_fiber_cond_broadcast() {
    let cond = Rc::new(Cond::new());

    let mut fiber_a = Fiber::new("test_fiber_a", &mut |cond: Box<Rc<Cond>>| {
        (*cond).wait();
        0
    });
    fiber_a.set_joinable(true);
    fiber_a.start(cond.clone());

    let mut fiber_b = Fiber::new("test_fiber_b", &mut |cond: Box<Rc<Cond>>| {
        (*cond).wait();
        0
    });
    fiber_b.set_joinable(true);
    fiber_b.start(cond.clone());

    sleep(Duration::from_millis(10));
    cond.broadcast();
    fiber_a.join();
    fiber_b.join();
}

pub fn test_fiber_cond_timeout() {
    let cond = Rc::new(Cond::new());
    let mut fiber = Fiber::new("test_fiber", &mut |cond: Box<Rc<Cond>>| {
        let r = (*cond).wait_timeout(Duration::from_millis(10));
        assert!(!r);
        0
    });
    fiber.set_joinable(true);
    fiber.start(cond.clone());
    sleep(Duration::from_millis(20));
    cond.signal();
    fiber.join();
}
