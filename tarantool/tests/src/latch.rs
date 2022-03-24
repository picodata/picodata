use std::rc::Rc;
use std::time::Duration;

use tarantool::fiber::{sleep, Fiber, Latch};

pub fn latch_lock() {
    let latch = Rc::new(Latch::new());

    let mut fiber = Fiber::new("test_fiber", &mut |_| {
        let _lock = latch.lock();
        sleep(Duration::from_millis(10));
        0
    });

    fiber.set_joinable(true);
    fiber.start(());
    latch.lock();
    fiber.join();
}

pub fn latch_try_lock() {
    let latch = Rc::new(Latch::new());

    let mut fiber = Fiber::new("test_fiber", &mut |_| {
        let _lock = latch.lock();
        sleep(Duration::from_millis(10));
        0
    });

    fiber.set_joinable(true);
    fiber.start(());
    assert!(latch.try_lock().is_none());

    fiber.join();
    assert!(latch.try_lock().is_some());
}
