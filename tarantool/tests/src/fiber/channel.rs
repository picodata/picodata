use std::{
    cell::Cell,
    rc::Rc,
    time::Duration,
};

use crate::common::{DropCounter, check_yield, YieldResult::{Yields, DoesntYield}};
use tarantool::fiber;
use tarantool::util::IntoClones;

pub fn send_self() {
    let ch = fiber::Channel::new(1);

    ch.send("hello").unwrap();

    assert_eq!(ch.recv().unwrap(), "hello");
}

pub fn send_full() {
    let ch = fiber::Channel::new(0);

    assert_eq!(
        check_yield(||
            ch.send_timeout("echo1", Duration::from_micros(1)).unwrap_err()),
        Yields(fiber::SendError::Timeout("echo1"))
    );

    assert_eq!(
        check_yield(|| ch.try_send("echo2").unwrap_err()),
        DoesntYield(fiber::TrySendError::Full("echo2"))
    );
}

pub fn recv_empty() {
    let ch = fiber::Channel::<()>::new(0);

    assert_eq!(
        check_yield(|| ch.recv_timeout(Duration::from_micros(1)).unwrap_err()),
        Yields(fiber::RecvError::Timeout)
    );

    assert_eq!(
        check_yield(|| ch.try_recv().unwrap_err()),
        DoesntYield(fiber::TryRecvError::Empty)
    );
}

pub fn unbuffered() {
    let (tx, rx) = fiber::Channel::new(0).into_clones();

    let f = fiber::defer(move || rx.recv().unwrap());

    assert_eq!(
        check_yield(|| tx.send("hello").unwrap()),
        Yields(())
    );

    assert_eq!(f.join(), "hello")
}

pub fn drop_sender() {
    let (tx, rx) = fiber::Channel::<()>::new(0).into_clones();

    let f = fiber::defer(move || rx.recv());

    tx.close();

    assert_eq!(f.join(), None);
}

pub fn dont_drop_msg() {
    let (tx, rx) = fiber::Channel::new(1).into_clones();
    tx.send("don't drop this").unwrap();
    tx.close();
    // fiber_channel_close destroys all the messages, isn't that nice?
    assert_eq!(rx.recv(), None);
}

pub fn one_v_two() {
    let (tx, rx1, rx2) = fiber::Channel::new(0).into_clones();
    let f1 = fiber::defer(move || rx1.recv().unwrap());
    let f2 = fiber::defer(move || rx2.recv().unwrap());
    tx.send("hello").unwrap();
    assert_eq!(f1.join(), "hello");
    tx.send("what's up").unwrap();
    assert_eq!(f2.join(), "what's up");
}

pub fn two_v_one() {
    let (tx1, tx2, rx) = fiber::Channel::new(0).into_clones();
    let f1 = fiber::defer(move || tx1.send("how ya doin?").unwrap());
    let f2 = fiber::defer(move || tx2.send("what's good").unwrap());
    assert_eq!(rx.recv(), Some("how ya doin?"));
    assert_eq!(rx.recv(), Some("what's good"));
    f1.join();
    f2.join();
}

pub fn drop_msgs() {
    let (drop_count_tx, drop_count_rx) = Rc::new(Cell::new(0)).into_clones();
    let (s1, s2, s3) = DropCounter(drop_count_tx).into_clones();

    let (tx, rx) = fiber::Channel::new(3).into_clones();
    tx.send(s1).unwrap();
    tx.send(s2).unwrap();
    tx.send(s3).unwrap();

    assert_eq!(drop_count_rx.get(), 0);
    drop((tx, rx));
    assert_eq!(drop_count_rx.get(), 3);
}

pub fn circle() {
    let (tx1, tx2, tx3, rx1, rx2, rx3) = fiber::Channel::new(0).into_clones();

    let f1 = fiber::defer_proc(move || {
        let mut msg = rx1.recv().unwrap();
        Vec::push(&mut msg, 3);
        tx1.send(msg).unwrap()
    });

    let f2 = fiber::defer(move || {
        tx2.send(vec![1]).unwrap();
        rx2.recv().unwrap()
    });

    let mut msg = rx3.recv().unwrap();
    msg.push(2);
    tx3.send(msg).unwrap();

    assert_eq!(f2.join(), vec![1, 2, 3]);
    let () = f1.join();
}

pub fn iter() {
    let (tx1, tx2, tx3, rx) = fiber::Channel::new(0).into_clones();
    let f1 = fiber::defer_proc(move || tx1.send(1).unwrap());
    let f2 = fiber::defer_proc(move || tx2.send(2).unwrap());
    let f3 = fiber::defer_proc(move || tx3.send(3).unwrap());
    let mut i = 0;
    for msg in &rx {
        i += 1;
        assert_eq!(msg, i);
        if i == 3 { break }
    }
    f1.join();
    f2.join();
    f3.join();
}

pub fn into_iter() {
    let (tx1, tx2, tx3, rx) = fiber::Channel::new(0).into_clones();
    let f1 = fiber::defer_proc(move || tx1.send(1).unwrap());
    let f2 = fiber::defer_proc(move || tx2.send(2).unwrap());
    let f3 = fiber::defer_proc(move || tx3.send(3).unwrap());
    let mut i = 0;
    for msg in rx {
        i += 1;
        assert_eq!(msg, i);
        if i == 3 { break }
    }
    f1.join();
    f2.join();
    f3.join();
}

pub fn try_iter() {
    let ch = fiber::Channel::new(3);
    ch.send(1).unwrap();
    ch.send(2).unwrap();
    ch.send(3).unwrap();
    assert_eq!(ch.try_iter().collect::<Vec<_>>(), vec![1, 2, 3]);
}

#[rustfmt::skip]
pub fn as_mutex() {
    let (lock1, lock2, lock3) = fiber::Channel::new(1).into_clones();
    let (log0, log1, log2, log3, log_out) = fiber::Channel::new(14).into_clones();
    let shared_resource = std::cell::UnsafeCell::new(vec![]);
    let sr = shared_resource.get();

    let f1 = fiber::defer_proc(move || {
        log1.send("f1:lock").unwrap();
        lock1.send(()).unwrap();            // Capture the lock
        log1.send("f1:critical").unwrap();
        fiber::sleep(Duration::ZERO);       // Tease the other fibers
        unsafe { (&mut *sr).push(1); }      // Access the critical section
        let () = lock1.recv().unwrap();     // Release the lock
        log1.send("f1:release").unwrap();
    });

    let f2 = fiber::defer_proc(move || {
        log2.send("f2:lock").unwrap();
        lock2.send(()).unwrap();            // Capture the lock
        log2.send("f2:critical").unwrap();
        fiber::sleep(Duration::ZERO);       // Tease the other fibers
        unsafe { (&mut *sr).push(2); }      // Access the critical section
        let () = lock2.recv().unwrap();     // Release the lock
        log2.send("f2:release").unwrap();
    });

    let f3 = fiber::defer_proc(move || {
        log3.send("f3:lock").unwrap();
        lock3.send(()).unwrap();            // Capture the lock
        log3.send("f3:critical").unwrap();
        fiber::sleep(Duration::ZERO);       // Tease the other fibers
        unsafe { (&mut *sr).push(3); }      // Access the critical section
        let () = lock3.recv().unwrap();     // Release the lock
        log3.send("f3:release").unwrap();
    });

    log0.send("main:sleep").unwrap();
    fiber::sleep(Duration::ZERO);

    log0.send("main:join(f2)").unwrap();
    f2.join();
    log0.send("main:join(f1)").unwrap();
    f1.join();
    log0.send("main:join(f3)").unwrap();
    f3.join();
    log0.send("main:done").unwrap();

    assert_eq!(unsafe { &*sr }, &[1, 2, 3]);

    assert_eq!(
        log_out.try_iter().collect::<Vec<_>>(),
        vec![
            "main:sleep",
                        "f1:lock",
                        "f1:critical",
                                    "f2:lock",
                                                "f3:lock",
            "main:join(f2)",
                        "f1:release",
                                    "f2:critical",
                                    "f2:release",
                                                "f3:critical",
            "main:join(f1)",
            "main:join(f3)",
                                                "f3:release",
            "main:done",
        ]
    );
}

pub fn demo() {
    let (log_tx, log_rx) = fiber::Channel::new(0).into_clones();
    let (tx, rx) = fiber::Channel::new(0).into_clones();

    let flog = fiber::defer(move || log_rx.into_iter().collect::<Vec<_>>());

    let f = fiber::defer_proc(move || {
        log_tx.send("job started".to_string()).unwrap();
        for msg in rx {
            log_tx.send(format!("job got data: {}", msg)).unwrap();
        }
        log_tx.send("job done".to_string()).unwrap();
        log_tx.close();
    });
    fiber::sleep(Duration::from_millis(10));
    tx.send(1).unwrap();
    fiber::sleep(Duration::from_millis(10));
    tx.close();

    f.join();

    assert_eq!(
        flog.join(), &[
            "job started".to_string(),
            "job got data: 1".to_string(),
            "job done".to_string(),
        ]
    );
}

pub fn drop_rx() {
    let (tx, rx) = fiber::Channel::new(0).into_clones();
    let f = fiber::defer_proc(move || rx.close());
    assert_eq!(tx.send("no block"), Err("no block"));
    f.join();
}

pub fn into_clones() {
    // fiber::Channel allows cloning
    // even if the inner type doesn't.
    struct NonClonable();

    #[derive(Clone)]
    struct MyChannel(fiber::Channel<NonClonable>);

    let (_, _) = MyChannel(fiber::Channel::new(1)).into_clones();
}

pub fn cannot_send_ref() {
    let (tx, rx) = fiber::Channel::new(0).into_clones();
    let f = fiber::defer(move || rx.recv());
    {
        let v = vec![1, 2, 3];
        // tx.send(&v).unwrap(); <- must not compile anymore
        tx.send(v).unwrap();
    }
    assert_eq!(f.join().unwrap(), &[1, 2, 3])
}
