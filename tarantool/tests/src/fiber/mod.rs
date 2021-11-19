use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    time::Duration,
};

use crate::common::{DropCounter, capture_value, fiber_csw};
use tarantool::fiber;
use tarantool::hlua::{
    AsLua,
    Lua,
};
use tarantool::ffi::lua;
use tarantool::util::IntoClones;

pub mod old;
pub mod channel;

pub fn immediate() {
    let jh = fiber::Builder::new()
        .func(|| 69)
        .start()
        .unwrap();
    let res = jh.join();
    assert_eq!(res, 69);

    let jh = fiber::start(|| 420);
    let res = jh.join();
    assert_eq!(res, 420);
}

pub fn immediate_with_attrs() {
    let jh = fiber::Builder::new()
        .name("boo")
        .stack_size(100_000).unwrap()
        .func(|| 42)
        .start()
        .unwrap();
    let res = jh.join();
    assert_eq!(res, 42);
}

pub fn multiple_immediate() {
    let mut res = vec![];
    let fibers = vec![vec![1, 2], vec![3, 4], vec![5, 6]]
        .into_iter()
        .map(|v|
            fiber::start(move || {
                v.into_iter().map(|e| e + 1).collect::<Vec::<_>>()
            })
        )
        .collect::<Vec<_>>();
    res.push(1);
    res.extend(
        fibers.into_iter()
            .map(fiber::JoinHandle::join)
            .flatten()
    );
    res.push(8);
    assert_eq!(res, vec![1, 2, 3, 4, 5, 6, 7, 8]);
}

pub fn unit_immediate() {
    let jh = fiber::Builder::new()
        .func(|| ())
        .start()
        .unwrap();
    let () = jh.join();

    let () = fiber::start_proc(|| ()).join();
}

pub fn unit_immediate_with_attrs() {
    let jh = fiber::Builder::new()
        .name("boo")
        .stack_size(100_000).unwrap()
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
                res_ref.borrow_mut().extend(
                    v.into_iter().map(|e| e + 1).collect::<Vec::<_>>()
                )
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
    let jh = fiber::Builder::new()
        .func(|| 13)
        .defer()
        .unwrap();
    assert_eq!(jh.join(), 13);

    let jh = fiber::defer(|| 42);
    assert_eq!(jh.join(), 42);
}

pub fn deferred_with_attrs() {
    let res = fiber::Builder::new()
        .name("boo")
        .stack_size(100_000).unwrap()
        .func(|| 15)
        .defer()
        .unwrap()
        .join();
    assert_eq!(res, 15);
}

pub fn multiple_deferred() {
    let mut res = vec![];
    let fibers = vec![vec![1, 2], vec![3, 4], vec![5, 6]]
        .into_iter()
        .map(|v|
            fiber::defer(move || {
                v.into_iter().map(|e| e + 1).collect::<Vec::<_>>()
            })
        )
        .collect::<Vec<_>>();
    res.push(1);
    res.extend(
        fibers.into_iter()
            .map(fiber::LuaJoinHandle::join)
            .flatten()
    );
    res.push(8);
    assert_eq!(res, vec![1, 2, 3, 4, 5, 6, 7, 8]);
}

pub fn unit_deferred() {
    let jh = fiber::Builder::new()
        .proc(|| ())
        .defer()
        .unwrap();
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
        .stack_size(100_000).unwrap()
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
            fiber::defer_proc(move ||
                res_ref.borrow_mut().extend(
                    v.into_iter().map(|e| e + 1).collect::<Vec::<_>>()
                )
            )
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

struct LuaStackIntegrityGuard {
    name: &'static str,
}

impl LuaStackIntegrityGuard {
    fn new(name: &'static str) -> Self {
        let lua: Lua = crate::hlua::global();
        let l = lua.as_lua();
        unsafe { lua::lua_pushlstring(l, name.as_bytes().as_ptr() as *mut i8, name.len()) };
        Self{name}
    }
}

impl Drop for LuaStackIntegrityGuard {
    fn drop(&mut self) {
        let lua: Lua = crate::hlua::global();
        let l = lua.as_lua();

        let msg = unsafe {
            let cstr = lua::lua_tostring(l, -1);
            if cstr.is_null() {
                panic!("Lua stack integrity violation");
            }
            let msg = std::ffi::CStr::from_ptr(cstr).to_str().unwrap();
            lua::lua_pop(l, 1);
            msg
        };

        assert_eq!(msg, self.name);
    }
}

pub fn immediate_yields() {
    let _guard = LuaStackIntegrityGuard::new("immediate_fiber_guard");

    let (tx, rx) = Rc::new(Cell::new(0)).into_clones();
    let csw1 = fiber_csw();
    let f = fiber::start(move || tx.set(69));
    let csw2 = fiber_csw();

    assert_eq!(rx.get(), 69);
    assert_eq!(csw2, csw1+1);

    f.join();
}

pub fn deferred_doesnt_yield() {
    let _guard = LuaStackIntegrityGuard::new("deferred_fiber_guard");

    let (tx, rx) = Rc::new(Cell::new(0)).into_clones();
    let csw1 = fiber_csw();
    let f = fiber::defer(move || tx.set(96));
    let csw2 = fiber_csw();

    assert_eq!(rx.get(), 0);
    assert_eq!(csw2, csw1);

    fiber::sleep(Duration::ZERO);
    assert_eq!(rx.get(), 96);

    f.join();
}

pub fn start_error() {
    let _guard = LuaStackIntegrityGuard::new("fiber_error_guard");

    let _spoiler = LuaContextSpoiler::new();

    match fiber::LuaFiber::new(fiber::LuaFiberFunc(|| ())).spawn() {
        Err(e) => assert_eq!(
            format!("{}", e),
            "Lua error: Execution error: Artificial error"
        ),
        _ => panic!(),
    }

    struct LuaContextSpoiler;

    impl LuaContextSpoiler {
        fn new() -> Self {
            crate::hlua::global().execute::<()>(r#"
            _fiber_new_backup = package.loaded.fiber.new
            package.loaded.fiber.new = function() error("Artificial error", 0) end
            "#).unwrap();
            Self
        }
    }

    impl Drop for LuaContextSpoiler {
        fn drop(&mut self) {
            crate::hlua::global().execute::<()>(r#"
            package.loaded.fiber.new = _fiber_new_backup
            _fiber_new_backup = nil
            "#).unwrap();
        }
    }
}

pub fn require_error() {
    let _guard = LuaStackIntegrityGuard::new("fiber_error_guard");

    let _spoiler = LuaContextSpoiler::new();

    match fiber::LuaFiber::new(fiber::LuaFiberFunc(|| ())).spawn() {
        Err(e) => assert_eq!(
            format!("{}", e),
            "Lua error: Execution error: Artificial require error"
        ),
        _ => panic!(),
    }

    struct LuaContextSpoiler;

    impl LuaContextSpoiler {
        fn new() -> Self {
            let lua: Lua = crate::hlua::global();
            lua.execute::<()>(r#"
            _fiber_backup = package.loaded.fiber
            package.loaded.fiber = nil
            package.preload.fiber = function() error("Artificial require error", 0) end
            "#).unwrap();
            Self
        }
    }

    impl Drop for LuaContextSpoiler {
        fn drop(&mut self) {
            let lua: Lua = crate::hlua::global();
            lua.execute::<()>(r#"
            package.preload.fiber = nil
            package.loaded.fiber = _fiber_backup
            _fiber_backup = nil
            "#).unwrap();
        }
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
    let _guard = LuaStackIntegrityGuard::new("defer_dont_join");

    let tx = Rc::new(Cell::new(0));
    let rx = Rc::downgrade(&tx);
    let f = fiber::defer(move || DropCounter(tx));

    assert_eq!(rx.strong_count(), 1);
    assert_eq!(rx.upgrade().unwrap().get(), 0);
    drop(f);
    // There's a memory leak that we can't do anything about if we drop the
    // LuaJoinHandle without joining it first
    assert_eq!(rx.strong_count(), 1);
    assert_eq!(rx.upgrade().unwrap().get(), 0);
}

pub fn defer_proc_dont_join() {
    let _guard = LuaStackIntegrityGuard::new("defer_proc_dont_join");

    let tx = Rc::new(Cell::new(0));
    let rx = Rc::downgrade(&tx);
    let d = DropCounter(tx);
    let f = fiber::defer_proc(move || capture_value(&d));

    assert_eq!(rx.strong_count(), 1);
    assert_eq!(rx.upgrade().unwrap().get(), 0);
    drop(f);
    // There's a memory leak that we can't do anything about if we drop the
    // LuaJoinHandle without joining it first
    assert_eq!(rx.strong_count(), 1);
    assert_eq!(rx.upgrade().unwrap().get(), 0);
}

pub fn immediate_with_cond() {
    let msgs = Rc::new(RefCell::new(vec![]));
    let cond = Rc::new(fiber::Cond::new());

    let fibers = (1..=3).map(|i| {
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

    let fibers = (1..=3).map(|i| {
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

