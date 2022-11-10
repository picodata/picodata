use std::time::Duration;

use futures::join;
use tarantool::fiber::{self, r#async::*};

const _0_SEC: Duration = Duration::ZERO;
const _1_SEC: Duration = Duration::from_secs(1);

pub fn drop_the_result() {
    let (rx, tx) = oneshot::channel::<i32>();
    assert!(!tx.is_dropped());
    drop(rx);
    assert!(tx.is_dropped());
    tx.send(0);
}

pub fn receive_non_blocking() {
    let (rx, tx) = oneshot::channel::<i32>();
    tx.send(56);
    assert_eq!(block_on(rx), Some(56));
}

pub fn receive_non_blocking_after_dropping_sender() {
    let (rx, tx) = oneshot::channel::<i32>();
    drop(tx);
    assert_eq!(block_on(rx), None);
}

pub fn receive_blocking_before_sending() {
    let (rx, tx) = oneshot::channel::<i32>();
    let jh = fiber::start(move || block_on(rx));
    tx.send(39);
    assert_eq!(jh.join(), Some(39));
}

pub fn receive_blocking_before_dropping_sender() {
    let (rx, tx) = oneshot::channel::<i32>();
    let jh = fiber::start(move || block_on(rx));
    drop(tx);
    assert_eq!(jh.join(), None);
}

pub fn join_two_after_sending() {
    let f = async {
        let (rx1, tx1) = oneshot::channel::<i32>();
        let (rx2, tx2) = oneshot::channel::<i32>();

        tx1.send(101);
        tx2.send(102);
        join!(rx1, rx2)
    };
    assert_eq!(block_on(f), (Some(101), Some(102)));
}

pub fn join_two_before_sending() {
    let c = fiber::Cond::new();
    drop(c);

    let (rx1, tx1) = oneshot::channel::<i32>();
    let (rx2, tx2) = oneshot::channel::<i32>();

    let jh = fiber::start(move || block_on(async { join!(rx1, rx2) }));

    tx1.send(201);
    fiber::sleep(Duration::ZERO);
    tx2.send(202);
    assert_eq!(jh.join(), (Some(201), Some(202)));
}

pub fn join_two_drop_one() {
    let (rx1, tx1) = oneshot::channel::<i32>();
    let (rx2, tx2) = oneshot::channel::<i32>();

    let jh = fiber::start(move || block_on(async { join!(rx1, rx2) }));
    tx1.send(301);
    fiber::sleep(Duration::ZERO);
    drop(tx2);
    assert_eq!(jh.join(), (Some(301), None));
}

pub fn instant_future() {
    let fut = async { 78 };
    assert_eq!(block_on(fut), 78);

    let fut = timeout::timeout(Duration::ZERO, async { 79 });
    assert_eq!(block_on(fut), Some(79));
}

pub fn actual_timeout_promise() {
    let (rx, tx) = oneshot::channel::<i32>();
    let fut = async move { rx.timeout(_0_SEC).await };

    let jh = fiber::start(|| block_on(fut));
    assert_eq!(jh.join(), None);
    drop(tx);
}

pub fn drop_tx_before_timeout() {
    let (rx, tx) = oneshot::channel::<i32>();
    let fut = async move { rx.timeout(_1_SEC).await };

    let jh = fiber::start(move || block_on(fut));
    drop(tx);
    assert_eq!(jh.join(), Some(None));
}

pub fn send_tx_before_timeout() {
    let (rx, tx) = oneshot::channel::<i32>();
    let fut = async move { rx.timeout(_1_SEC).await };

    let jh = fiber::start(move || block_on(fut));
    tx.send(400);
    assert_eq!(jh.join(), Some(Some(400)));
}
