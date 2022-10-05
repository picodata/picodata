use std::{
    rc::Rc,
    time::Duration,
};

use tarantool::{
    fiber::{defer_proc, sleep, start, start_proc, Channel, Mutex},
    util::IntoClones,
};

pub fn simple() {
    let (sr1, sr2) = Rc::new(Mutex::new(0)).into_clones();
    let f = start_proc(move || *sr2.lock() = 69);
    assert_eq!(*sr1.lock(), 69);
    f.join();
}

pub fn try_lock() {
    let (sr1, sr2) = Rc::new(Mutex::new(0)).into_clones();
    let f = start_proc(move || {
        let mut guard = sr2.lock();
        sleep(Duration::ZERO);
        *guard = 420;
    });
    assert!(sr1.try_lock().is_none());
    f.join();
    assert_eq!(sr1.try_lock().map(|g| *g), Some(420))
}

pub fn debug() {
    let m = Mutex::new(0);
    let mut guard = m.lock();
    let s = start(|| format!("{:?}", m)).join();
    assert_eq!(&s[..21], "Mutex { data: <locked");
    assert_eq!(&s[s.len()-7..], ">, .. }");
    *guard = 13;
    Mutex::unlock(guard);
    let s = start(|| format!("{:?}", m)).join();
    assert_eq!(s, "Mutex { data: 13, .. }");
}

pub fn advanced() {
    let (log0, log1, log2, log3, log_out) = Channel::new(14).into_clones();
    let shared_resource = Rc::new(Mutex::new(vec![]));
    let (sr0, sr1, sr2, sr3) = shared_resource.into_clones();

    let f1 = defer_proc(move || {
        log1.send("f1:lock").unwrap();
        let mut guard = sr1.lock();         // Acquire the lock
        log1.send("f1:critical").unwrap();
        sleep(Duration::ZERO);              // Tease the other fibers
        guard.push(1);                      // Critical section
        log1.send("f1:release").unwrap();
    });

    let f2 = defer_proc(move || {
        log2.send("f2:lock").unwrap();
        let mut guard = sr2.lock();         // Acquire the lock
        log2.send("f2:critical").unwrap();
        sleep(Duration::ZERO);              // Tease the other fibers
        guard.push(2);                      // Critical section
        log2.send("f2:release").unwrap();
    });

    let f3 = defer_proc(move || {
        log3.send("f3:lock").unwrap();
        let mut guard = sr3.lock();         // Acquire the lock
        log3.send("f3:critical").unwrap();
        sleep(Duration::ZERO);              // Tease the other fibers
        guard.push(3);                      // Critical section
        log3.send("f3:release").unwrap();
    });

    log0.send("main:sleep").unwrap();
    sleep(Duration::ZERO);

    log0.send("main:join(f2)").unwrap();
    f2.join();
    log0.send("main:join(f1)").unwrap();
    f1.join();
    log0.send("main:join(f3)").unwrap();
    f3.join();
    log0.send("main:done").unwrap();

    assert_eq!(Rc::try_unwrap(sr0).unwrap().into_inner(), &[1, 2, 3]);

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
