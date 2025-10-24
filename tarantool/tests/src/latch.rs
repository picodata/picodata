use std::rc::Rc;
use std::time::Duration;

use tarantool::fiber;

pub fn latch_lock() {
    let latch = Rc::new(fiber::Latch::new());

    let fiber = fiber::start(|| {
        let _lock = latch.lock();
        fiber::sleep(Duration::from_millis(10));
    });

    latch.lock();
    fiber.join();
}

pub fn latch_try_lock() {
    let latch = Rc::new(fiber::Latch::new());

    let fiber = fiber::start(|| {
        let _lock = latch.lock();
        fiber::sleep(Duration::from_millis(10));
    });

    assert!(latch.try_lock().is_none());

    fiber.join();
    assert!(latch.try_lock().is_some());
}
