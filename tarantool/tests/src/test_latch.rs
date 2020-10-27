use std::rc::Rc;

use tarantool_module::fiber::{sleep, Fiber};
use tarantool_module::latch::Latch;

pub fn test_latch_lock() {
    let latch = Rc::new(Latch::new());

    let mut fiber = Fiber::new("test_fiber", &mut |_| {
        let _lock = latch.lock();
        sleep(0.01);
        0
    });

    fiber.set_joinable(true);
    fiber.start(());
    latch.lock();
    fiber.join();
}

pub fn test_latch_try_lock() {
    let latch = Rc::new(Latch::new());

    let mut fiber = Fiber::new("test_fiber", &mut |_| {
        let _lock = latch.lock();
        sleep(0.01);
        0
    });

    fiber.set_joinable(true);
    fiber.start(());
    assert!(latch.try_lock().is_none());

    fiber.join();
    assert!(latch.try_lock().is_some());
}
