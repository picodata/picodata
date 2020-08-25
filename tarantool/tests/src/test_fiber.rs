use tarantool_module::Fiber;
use tarantool_module::fiber::{fiber_yield, is_cancelled, sleep};

pub fn test_fiber() {
    let mut fiber = Fiber::new("test_fiber", &mut |_| { 0 });
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
