use std::rc::Rc;

use tarantool_module::fiber::{fiber_yield, is_cancelled, sleep};
use tarantool_module::{Fiber, FiberAttr, FiberCond};

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
        sleep(0.01);
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
    sleep(0.01);
    fiber.wakeup();
    fiber.join();
}

pub fn test_fiber_cond_signal() {
    let cond = Rc::new(FiberCond::new());
    let mut fiber = Fiber::new("test_fiber", &mut |cond: Box<Rc<FiberCond>>| {
        (*cond).wait();
        0
    });
    fiber.set_joinable(true);
    fiber.start(cond.clone());
    sleep(0.01);
    cond.signal();
    fiber.join();
}

pub fn test_fiber_cond_broadcast() {
    let cond = Rc::new(FiberCond::new());

    let mut fiber_a = Fiber::new("test_fiber_a", &mut |cond: Box<Rc<FiberCond>>| {
        (*cond).wait();
        0
    });
    fiber_a.set_joinable(true);
    fiber_a.start(cond.clone());

    let mut fiber_b = Fiber::new("test_fiber_b", &mut |cond: Box<Rc<FiberCond>>| {
        (*cond).wait();
        0
    });
    fiber_b.set_joinable(true);
    fiber_b.start(cond.clone());

    sleep(0.01);
    cond.broadcast();
    fiber_a.join();
    fiber_b.join();
}

pub fn test_fiber_cond_timeout() {
    let cond = Rc::new(FiberCond::new());
    let mut fiber = Fiber::new("test_fiber", &mut |cond: Box<Rc<FiberCond>>| {
        let r = (*cond).wait_timeout(0.01);
        assert!(!r);
        0
    });
    fiber.set_joinable(true);
    fiber.start(cond.clone());
    sleep(0.02);
    cond.signal();
    fiber.join();
}
