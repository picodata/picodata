#![allow(clippy::let_unit_value)]
#![allow(deprecated)]
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    time::Duration,
};

use crate::common::capture_value;
use crate::common::DropCounter;
use crate::common::LuaContextSpoiler;
use crate::common::LuaStackIntegrityGuard;
use tarantool::fiber;
use tarantool::fiber::Fiber;
use tarantool::tlua::AsLua;
use tarantool::util::IntoClones;

pub mod channel;
pub mod mutex;
pub mod old;

pub fn immediate() {
    let jh = fiber::Builder::new().func(|| 69).start().unwrap();
    let res = jh.join();
    assert_eq!(res, 69);

    let jh = fiber::start(|| 420);
    let res = jh.join();
    assert_eq!(res, 420);
}

pub fn immediate_with_attrs() {
    let jh = fiber::Builder::new()
        .name("boo")
        .stack_size(100_000)
        .unwrap()
        .func(|| 42)
        .start()
        .unwrap();
    let res = jh.join();
    assert_eq!(res, 42);
}

#[allow(clippy::needless_collect)]
pub fn multiple_immediate() {
    let mut res = vec![];
    let fibers = vec![vec![1, 2], vec![3, 4], vec![5, 6]]
        .into_iter()
        .map(|v| fiber::start(move || v.into_iter().map(|e| e + 1).collect::<Vec<_>>()))
        .collect::<Vec<_>>();
    res.push(1);
    res.extend(fibers.into_iter().flat_map(fiber::JoinHandle::join));
    res.push(8);
    assert_eq!(res, vec![1, 2, 3, 4, 5, 6, 7, 8]);
}

pub fn unit_immediate() {
    let jh = fiber::Builder::new().func(|| ()).start().unwrap();
    let () = jh.join();

    let () = fiber::start_proc(|| ()).join();
}

pub fn unit_immediate_with_attrs() {
    let jh = fiber::Builder::new()
        .name("boo")
        .stack_size(100_000)
        .unwrap()
        .proc(|| ())
        .start()
        .unwrap();
    let () = jh.join();
}

pub fn multiple_unit_immediate() {
    let res = Rc::new(RefCell::new(vec![]));
    let fibers = vec![vec![1, 2], vec![3, 4], vec![5, 6]]
        .into_iter()
        .map(|v| {
            let res_ref = res.clone();
            fiber::start_proc(move || {
                res_ref
                    .borrow_mut()
                    .extend(v.into_iter().map(|e| e + 1).collect::<Vec<_>>())
            })
        })
        .collect::<Vec<_>>();
    res.borrow_mut().push(8);
    for f in fibers {
        f.join()
    }
    res.borrow_mut().push(9);
    let res = res.borrow().iter().copied().collect::<Vec<_>>();
    assert_eq!(res, vec![2, 3, 4, 5, 6, 7, 8, 9]);
}

pub fn deferred() {
    let jh = fiber::Builder::new().func(|| 13).defer().unwrap();
    assert_eq!(jh.join(), 13);

    let jh = fiber::defer(|| 42);
    assert_eq!(jh.join(), 42);
}

pub fn deferred_with_attrs() {
    let res = fiber::Builder::new()
        .name("boo")
        .stack_size(100_000)
        .unwrap()
        .func(|| 15)
        .defer()
        .unwrap()
        .join();
    assert_eq!(res, 15);
}

#[allow(clippy::needless_collect)]
pub fn multiple_deferred() {
    let mut res = vec![];
    let fibers = vec![vec![1, 2], vec![3, 4], vec![5, 6]]
        .into_iter()
        .map(|v| fiber::defer(move || v.into_iter().map(|e| e + 1).collect::<Vec<_>>()))
        .collect::<Vec<_>>();
    res.push(1);
    res.extend(fibers.into_iter().flat_map(fiber::JoinHandle::join));
    res.push(8);
    assert_eq!(res, vec![1, 2, 3, 4, 5, 6, 7, 8]);
}

pub fn unit_deferred() {
    let jh = fiber::Builder::new().proc(|| ()).defer().unwrap();
    let () = jh.join();

    let res = Rc::new(Cell::new(0));
    let jh = {
        let res = res.clone();
        fiber::defer_proc(move || res.set(42))
    };
    assert_eq!(res.get(), 0);
    jh.join();
    assert_eq!(res.get(), 42);
}

pub fn unit_deferred_with_attrs() {
    let () = fiber::Builder::new()
        .name("boo")
        .stack_size(100_000)
        .unwrap()
        .proc(|| ())
        .defer()
        .unwrap()
        .join();
}

pub fn multiple_unit_deferred() {
    let res = Rc::new(RefCell::new(vec![]));
    let fibers = vec![vec![1, 2], vec![3, 4], vec![5, 6]]
        .into_iter()
        .map(|v| {
            let res_ref = res.clone();
            fiber::defer_proc(move || {
                res_ref
                    .borrow_mut()
                    .extend(v.into_iter().map(|e| e + 1).collect::<Vec<_>>())
            })
        })
        .collect::<Vec<_>>();
    res.borrow_mut().push(1);
    for f in fibers {
        f.join()
    }
    res.borrow_mut().push(8);
    let res = res.borrow().iter().copied().collect::<Vec<_>>();
    assert_eq!(res, vec![1, 2, 3, 4, 5, 6, 7, 8]);
}

pub fn immediate_yields() {
    let _guard = LuaStackIntegrityGuard::global("immediate_fiber_guard");

    let (tx, rx) = Rc::new(Cell::new(0)).into_clones();
    let csw1 = fiber::csw();
    let f = fiber::start(move || tx.set(69));
    let csw2 = fiber::csw();

    assert_eq!(rx.get(), 69);
    assert_eq!(csw2, csw1 + 1);

    f.join();
}

pub fn deferred_doesnt_yield() {
    let _guard = LuaStackIntegrityGuard::global("deferred_fiber_guard");

    let (tx, rx) = Rc::new(Cell::new(0)).into_clones();
    let csw1 = fiber::csw();
    let f = fiber::defer(move || tx.set(96));
    let csw2 = fiber::csw();

    assert_eq!(rx.get(), 0);
    assert_eq!(csw2, csw1);

    fiber::sleep(Duration::ZERO);
    assert_eq!(rx.get(), 96);

    f.join();
}

pub fn start_error() {
    let _guard = LuaStackIntegrityGuard::global("fiber_error_guard");

    let _spoiler = LuaContextSpoiler::new(
        r#"
            _fiber_new_backup = package.loaded.fiber.new
            package.loaded.fiber.new = function()
                error("Artificial error", 0)
            end
        "#,
        r#"
            package.loaded.fiber.new = _fiber_new_backup
            _fiber_new_backup = nil
        "#,
    );

    match fiber::Builder::new().func(|| ()).defer_lua() {
        Err(e) => assert_eq!(format!("{}", e), "lua error: Artificial error"),
        _ => panic!(),
    }
}

pub fn require_error() {
    let _guard = LuaStackIntegrityGuard::global("fiber_error_guard");

    let _spoiler = LuaContextSpoiler::new(
        r#"
            _fiber_backup = package.loaded.fiber
            package.loaded.fiber = nil
            package.preload.fiber = function()
                error("Artificial require error", 0)
            end
        "#,
        r#"
            package.preload.fiber = nil
            package.loaded.fiber = _fiber_backup
            _fiber_backup = nil
        "#,
    );

    match fiber::Builder::new().func(|| ()).defer_lua() {
        Err(e) => assert_eq!(format!("{}", e), "lua error: Artificial require error"),
        _ => panic!(),
    }
}

pub fn start_dont_join() {
    let (tx, rx) = Rc::new(Cell::new(0)).into_clones();
    let f = fiber::start(move || DropCounter(tx));

    assert_eq!(rx.get(), 0);
    drop(f);
    assert_eq!(rx.get(), 1);
}

pub fn start_proc_dont_join() {
    let (tx, rx) = Rc::new(Cell::new(0)).into_clones();
    let d = DropCounter(tx);

    assert_eq!(rx.get(), 0);
    let f = fiber::start_proc(move || capture_value(&d));
    assert_eq!(rx.get(), 1);
    drop(f);
    assert_eq!(rx.get(), 1);
}

pub fn defer_dont_join() {
    let _guard = LuaStackIntegrityGuard::global("defer_dont_join");

    let tx = Rc::new(Cell::new(0));
    let rx = Rc::downgrade(&tx);
    let f = fiber::defer(move || DropCounter(tx));

    assert_eq!(rx.strong_count(), 1);
    assert_eq!(rx.upgrade().unwrap().get(), 0);
    drop(f);
    // There's a memory leak that we can't do anything about if we drop the
    // JoinHandle without joining it first
    assert_eq!(rx.strong_count(), 1);
    assert_eq!(rx.upgrade().unwrap().get(), 0);
}

pub fn defer_proc_dont_join() {
    let _guard = LuaStackIntegrityGuard::global("defer_proc_dont_join");

    let tx = Rc::new(Cell::new(0));
    let rx = Rc::downgrade(&tx);
    let d = DropCounter(tx);
    let f = fiber::defer_proc(move || capture_value(&d));

    assert_eq!(rx.strong_count(), 1);
    assert_eq!(rx.upgrade().unwrap().get(), 0);
    drop(f);
    // There's a memory leak that we can't do anything about if we drop the
    // JoinHandle without joining it first
    assert_eq!(rx.strong_count(), 1);
    assert_eq!(rx.upgrade().unwrap().get(), 0);
}

pub fn immediate_with_cond() {
    let msgs = Rc::new(RefCell::new(vec![]));
    let cond = Rc::new(fiber::Cond::new());

    let fibers = (1..=3)
        .map(|i| {
            let msgs = msgs.clone();
            let cond = cond.clone();
            fiber::start_proc(move || {
                msgs.borrow_mut().push(i);
                cond.wait();
                msgs.borrow_mut().push(i + 3);
            })
        })
        .collect::<Vec<_>>();

    assert_eq!(*msgs.borrow(), vec![1, 2, 3]);

    cond.broadcast();
    fiber::sleep(Duration::ZERO);

    assert_eq!(*msgs.borrow(), vec![1, 2, 3, 4, 5, 6]);

    for f in fibers {
        f.join()
    }
}

pub fn deferred_with_cond() {
    let msgs = Rc::new(RefCell::new(vec![]));
    let cond = Rc::new(fiber::Cond::new());

    let fibers = (1..=3)
        .map(|i| {
            let msgs = msgs.clone();
            let cond = cond.clone();
            fiber::defer_proc(move || {
                msgs.borrow_mut().push(i);
                cond.wait();
                msgs.borrow_mut().push(i + 3);
            })
        })
        .collect::<Vec<_>>();

    assert!(msgs.borrow().is_empty());

    fiber::sleep(Duration::ZERO);

    assert_eq!(*msgs.borrow(), vec![1, 2, 3]);

    cond.broadcast();
    fiber::sleep(Duration::ZERO);

    assert_eq!(*msgs.borrow(), vec![1, 2, 3, 4, 5, 6]);

    for f in fibers {
        f.join()
    }
}

pub fn lua_thread() {
    let (log1, log2, log_out) = fiber::Channel::new(4).into_clones();
    let (l1, l1_keep) = Rc::new(tarantool::lua_state()).into_clones();
    let v1 = fiber::start(move || {
        log1.send("t1:push").unwrap();
        let l = (&*l1).push(42_i32);
        fiber::sleep(Duration::ZERO);
        log1.send("t1:read").unwrap();
        l.read::<i32>().unwrap()
    });

    let (l2, l2_keep) = Rc::new(tarantool::lua_state()).into_clones();
    let v2 = fiber::start(move || {
        log2.send("t2:push").unwrap();
        let l = (&*l2).push("hello");
        fiber::sleep(Duration::ZERO);
        log2.send("t2:read").unwrap();
        l.read::<String>().unwrap()
    });

    assert_eq!(v1.join(), 42);
    assert_eq!(v2.join(), "hello");
    assert_eq!(
        log_out.try_iter().collect::<Vec<_>>(),
        vec!["t1:push", "t2:push", "t1:read", "t2:read"]
    );

    drop(l1_keep);
    drop(l2_keep);
}

pub fn lifetime() {
    {
        let mut v = vec![1, 2, 3];
        let jh = fiber::start_proc(|| v[0] = 2);
        jh.join();
        assert_eq!(v, vec![2, 2, 3]);
    }

    // Doesn't compile
    // {
    //     let mut v = vec![1, 2, 3];
    //     fiber::start_proc(|| v[0] = 2)
    // }.join();

    {
        let v = vec![1, 2, 3];
        let jh = fiber::start(|| v[0]);
        assert_eq!(jh.join(), 1);
    }

    // Doesn't compile
    // {
    //     let v = vec![1, 2, 3];
    //     fiber::start(|| v[0])
    // }.join();

    {
        let mut v = vec![1, 2, 3];
        let jh = fiber::defer_proc(|| v[0] = 2);
        jh.join();
        assert_eq!(v, vec![2, 2, 3]);
    }

    // Doesn't compile
    // {
    //     let mut v = vec![1, 2, 3];
    //     fiber::defer_proc(|| v[0] = 2)
    // }.join();

    {
        let v = vec![1, 2, 3];
        let jh = fiber::defer(|| v[0]);
        assert_eq!(jh.join(), 1);
    }

    // Doesn't compile
    // {
    //     let v = vec![1, 2, 3];
    //     fiber::defer(|| v[0])
    // }.join();
}

pub fn r#yield() {
    //if fiber doesnt yield than test will be running forever
    let mut fiber = Fiber::new("test_fiber", &mut |_| {
        loop {
            // ignore fiber is canceled error
            fiber::r#yield().unwrap_or(());
        }
    });
    fiber.set_joinable(true);
    fiber.start(());
    fiber.cancel();
}

pub fn yield_canceled() {
    let mut fiber = Fiber::new("test_fiber", &mut |_| {
        fiber::sleep(Duration::from_millis(10));
        assert!(fiber::r#yield().is_err());
        0
    });
    fiber.set_joinable(true);
    fiber.start(());
    fiber.cancel();
    fiber::sleep(Duration::from_millis(20));
}
