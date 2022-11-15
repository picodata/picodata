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
    assert_eq!(block_on(rx), Ok(56));
}

pub fn receive_non_blocking_after_dropping_sender() {
    let (rx, tx) = oneshot::channel::<i32>();
    drop(tx);
    assert_eq!(block_on(rx), Err(RecvError));
}

pub fn receive_blocking_before_sending() {
    let (rx, tx) = oneshot::channel::<i32>();
    let jh = fiber::start(move || block_on(rx));
    tx.send(39);
    assert_eq!(jh.join(), Ok(39));
}

pub fn receive_blocking_before_dropping_sender() {
    let (rx, tx) = oneshot::channel::<i32>();
    let jh = fiber::start(move || block_on(rx));
    drop(tx);
    assert_eq!(jh.join(), Err(RecvError));
}

pub fn join_two_after_sending() {
    let f = async {
        let (rx1, tx1) = oneshot::channel::<i32>();
        let (rx2, tx2) = oneshot::channel::<i32>();

        tx1.send(101);
        tx2.send(102);
        join!(rx1, rx2)
    };
    assert_eq!(block_on(f), (Ok(101), Ok(102)));
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
    assert_eq!(jh.join(), (Ok(201), Ok(202)));
}

pub fn join_two_drop_one() {
    let (rx1, tx1) = oneshot::channel::<i32>();
    let (rx2, tx2) = oneshot::channel::<i32>();

    let jh = fiber::start(move || block_on(async { join!(rx1, rx2) }));
    tx1.send(301);
    fiber::sleep(Duration::ZERO);
    drop(tx2);
    assert_eq!(jh.join(), (Ok(301), Err(RecvError)));
}

pub fn instant_future() {
    let fut = async { 78 };
    assert_eq!(block_on(fut), 78);

    let fut = timeout::timeout(Duration::ZERO, async { 79 });
    assert_eq!(block_on(fut), Ok(79));
}

pub fn actual_timeout_promise() {
    let (rx, tx) = oneshot::channel::<i32>();
    let fut = async move { rx.timeout(_0_SEC).await };

    let jh = fiber::start(|| block_on(fut));
    assert_eq!(jh.join(), Err(timeout::Expired));
    drop(tx);
}

pub fn drop_tx_before_timeout() {
    let (rx, tx) = oneshot::channel::<i32>();
    let fut = async move { rx.timeout(_1_SEC).await };

    let jh = fiber::start(move || block_on(fut));
    drop(tx);
    assert_eq!(jh.join(), Ok(Err(RecvError)));
}

pub fn send_tx_before_timeout() {
    let (rx, tx) = oneshot::channel::<i32>();
    let fut = async move { rx.timeout(_1_SEC).await };

    let jh = fiber::start(move || block_on(fut));
    tx.send(400);
    assert_eq!(jh.join(), Ok(Ok(400)));
}

pub fn receive_notification_sent_before() {
    let (tx, mut rx_1) = watch::channel::<i32>(10);
    let mut rx_2 = rx_1.clone();
    tx.send(20).unwrap();
    assert_eq!(
        block_on(async move {
            let _ = join!(rx_1.changed(), rx_2.changed());
            (*rx_1.borrow(), *rx_2.borrow())
        }),
        (20, 20)
    );
}

pub fn receive_notification_sent_after() {
    let (tx, mut rx_1) = watch::channel::<i32>(10);
    let mut rx_2 = rx_1.clone();
    let jh = fiber::start(move || {
        block_on(async move {
            let _ = join!(rx_1.changed(), rx_2.changed());
            (*rx_1.borrow(), *rx_2.borrow())
        })
    });
    tx.send(20).unwrap();
    assert_eq!(jh.join(), (20, 20))
}

pub fn receive_multiple_notifications() {
    let (tx, mut rx_1) = watch::channel::<i32>(10);
    let jh = fiber::start(|| {
        block_on(async {
            rx_1.changed().await.unwrap();
            *rx_1.borrow()
        })
    });
    tx.send(1).unwrap();
    assert_eq!(jh.join(), 1);
    let jh = fiber::start(|| {
        block_on(async {
            rx_1.changed().await.unwrap();
            *rx_1.borrow()
        })
    });
    tx.send(2).unwrap();
    assert_eq!(jh.join(), 2);
}

pub fn retains_only_last_notification() {
    let (tx, mut rx_1) = watch::channel::<i32>(10);
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    tx.send(3).unwrap();
    let v = block_on(async {
        rx_1.changed().await.unwrap();
        *rx_1.borrow()
    });
    assert_eq!(v, 3);
    // No changes after
    assert_eq!(
        block_on(rx_1.changed().timeout(_1_SEC)),
        Err(timeout::Expired)
    );
}

pub fn notification_receive_error() {
    let (tx, mut rx_1) = watch::channel::<i32>(10);
    let jh = fiber::start(|| block_on(rx_1.changed()));
    drop(tx);
    assert_eq!(jh.join(), Err(RecvError));
}

pub fn notification_received_in_concurrent_fibers() {
    let (tx, mut rx_1) = watch::channel::<i32>(10);
    let mut rx_2 = rx_1.clone();
    let jh_1 = fiber::start(|| block_on(rx_1.changed()));
    let jh_2 = fiber::start(|| block_on(rx_2.changed()));
    tx.send(1).unwrap();
    assert!(jh_1.join().is_ok());
    assert!(jh_2.join().is_ok());
}
