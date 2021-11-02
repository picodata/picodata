use std::{
    cell::RefCell,
    rc::Rc,
    time::Duration,
};

use tarantool::fiber::{
    self, fiber_yield, is_cancelled, sleep, Cond, Fiber, FiberAttr
};
use tarantool::hlua::{
    AsMutLua,
    Lua,
    LuaFunction
};

use tarantool::ffi::lua;

pub fn test_fiber_new() {
    let mut fiber = Fiber::new("test_fiber", &mut |_| 0);
    fiber.set_joinable(true);
    fiber.start(());
    fiber.join();
}

pub fn test_fiber_new_with_attr() {
    let mut attr = FiberAttr::new();
    attr.set_stack_size(100_000).unwrap();

    let mut fiber = Fiber::new_with_attr("test_fiber", &attr, &mut |_| 0);
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

pub fn test_fiber_wake_multiple() {
    let res = Rc::new(RefCell::new(vec![]));
    let mut fibers = vec![];
    for (i, c) in (1..).zip(&['a', 'b', 'c']) {
        let mut fiber = Fiber::new(
            &format!("test_fiber_{}", c),
            &mut |r: Box<Rc<RefCell<Vec<i32>>>>| {
                fiber_yield();
                r.borrow_mut().push(i);
                0
            }
        );
        fiber.start(res.clone());
        fiber.wakeup();
        fibers.push(fiber);
    }

    for f in &mut fibers {
        f.set_joinable(true);
    }

    res.borrow_mut().push(0);
    for f in fibers {
        f.join();
    }
    res.borrow_mut().push(4);

    let res = res.borrow().iter().copied().collect::<Vec<_>>();
    // This is what we want:
    // assert_eq(res, vec![0, 1, 2, 3, 4]);
    // This is what we get:
    assert_eq!(res, vec![0, 3, 3, 3, 4]);
    // Because `Fiber` doesn't work with closures. `i` is passed by reference
    // and by the time the first fiber starts executing, it is equal to 3.
    // This is actually undefined behavior, so adding this test is probably a
    // bad idea
}

pub fn test_fiber_cond_signal() {
    let cond = Rc::new(Cond::new());
    let mut fiber = Fiber::new("test_fiber", &mut |cond: Box<Rc<Cond>>| {
        (*cond).wait();
        0
    });
    fiber.set_joinable(true);
    fiber.start(cond.clone());
    sleep(0.01);
    cond.signal();
    fiber.join();
}

pub fn test_fiber_cond_broadcast() {
    let cond = Rc::new(Cond::new());

    let mut fiber_a = Fiber::new("test_fiber_a", &mut |cond: Box<Rc<Cond>>| {
        (*cond).wait();
        0
    });
    fiber_a.set_joinable(true);
    fiber_a.start(cond.clone());

    let mut fiber_b = Fiber::new("test_fiber_b", &mut |cond: Box<Rc<Cond>>| {
        (*cond).wait();
        0
    });
    fiber_b.set_joinable(true);
    fiber_b.start(cond.clone());

    sleep(0.01);
    cond.broadcast();
    fiber_a.join();
    fiber_b.join();
}

pub fn test_fiber_cond_timeout() {
    let cond = Rc::new(Cond::new());
    let mut fiber = Fiber::new("test_fiber", &mut |cond: Box<Rc<Cond>>| {
        let r = (*cond).wait_timeout(Duration::from_secs_f32(0.01));
        assert!(!r);
        0
    });
    fiber.set_joinable(true);
    fiber.start(cond.clone());
    sleep(0.02);
    cond.signal();
    fiber.join();
}

////////////////////////////////////////////////////////////////////////////////
// New
////////////////////////////////////////////////////////////////////////////////

pub fn test_immediate() {
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

pub fn test_immediate_with_attrs() {
    let jh = fiber::Builder::new()
        .name("boo")
        .stack_size(100_000).unwrap()
        .func(|| 42)
        .start()
        .unwrap();
    let res = jh.join();
    assert_eq!(res, 42);
}

pub fn test_multiple_immediate() {
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

pub fn test_unit_immediate() {
    let jh = fiber::Builder::new()
        .func(|| ())
        .start()
        .unwrap();
    let () = jh.join();

    let () = fiber::start_proc(|| ()).join();
}

pub fn test_unit_immediate_with_attrs() {
    let jh = fiber::Builder::new()
        .name("boo")
        .stack_size(100_000).unwrap()
        .proc(|| ())
        .start()
        .unwrap();
    let () = jh.join();
}

pub fn test_multiple_unit_immediate() {
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

pub fn test_deferred() {
    let jh = fiber::Builder::new()
        .func(|| 13)
        .defer()
        .unwrap();
    assert_eq!(jh.join(), 13);

    let jh = fiber::defer(|| 42);
    assert_eq!(jh.join(), 42);
}

pub fn test_deferred_with_attrs() {
    let res = fiber::Builder::new()
        .name("boo")
        .stack_size(100_000).unwrap()
        .func(|| 15)
        .defer()
        .unwrap()
        .join();
    assert_eq!(res, 15);
}

pub fn test_multiple_deferred() {
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

pub fn test_unit_deferred() {
    let jh = fiber::Builder::new()
        .proc(|| ())
        .defer()
        .unwrap();
    let () = jh.join();

    let res = std::cell::Cell::new(0);
    let jh = fiber::defer_proc(|| res.set(42));
    assert_eq!(res.get(), 0);
    jh.join();
    assert_eq!(res.get(), 42);
}

pub fn test_unit_deferred_with_attrs() {
    let () = fiber::Builder::new()
        .name("boo")
        .stack_size(100_000).unwrap()
        .proc(|| ())
        .defer()
        .unwrap()
        .join();
}

pub fn test_multiple_unit_deferred() {
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

fn fiber_csw() -> i32 {
    static mut FUNCTION_DEFINED: bool = false;
    let mut lua: Lua = crate::hlua::global();

    if unsafe { !FUNCTION_DEFINED } {
        lua.execute::<()>(r#"
        function fiber_csw()
        local fiber = require('fiber')
        return fiber.info()[fiber.id()].csw
        end
        "#).unwrap();
        unsafe { FUNCTION_DEFINED = true; }
    }

    return lua.get::<LuaFunction<_>, _>("fiber_csw").unwrap().call().unwrap();
}

struct LuaStackIntegrityGuard {
    name: &'static str,
}

impl LuaStackIntegrityGuard {
    fn new(name: &'static str) -> Self {
        let mut lua: Lua = crate::hlua::global();
        let l = lua.as_mut_lua().state_ptr();
        unsafe { lua::lua_pushlstring(l, name.as_bytes().as_ptr() as *mut i8, name.len()) };
        Self{name}
    }
}

impl Drop for LuaStackIntegrityGuard {
    fn drop(&mut self) {
        let mut lua: Lua = crate::hlua::global();
        let l = lua.as_mut_lua().state_ptr();

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

    let mut upvalue = 0;
    let csw1 = fiber_csw();
    fiber::start(|| upvalue = 69);
    let csw2 = fiber_csw();

    assert_eq!(upvalue, 69);
    assert_eq!(csw2, csw1+1);
}

pub fn deferred_doesnt_yield() {
    let _guard = LuaStackIntegrityGuard::new("deferred_fiber_guard");

    let mut upvalue = 0;
    let csw1 = fiber_csw();
    fiber::defer(|| upvalue = 96);
    let csw2 = fiber_csw();

    assert_eq!(upvalue, 0);
    assert_eq!(csw2, csw1);

    fiber::sleep(0.);
    assert_eq!(upvalue, 96);
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
            let mut lua: Lua = crate::hlua::global();
            lua.execute::<()>(r#"
            _fiber_new_backup = package.loaded.fiber.new
            package.loaded.fiber.new = function() error("Artificial error", 0) end
            "#).unwrap();
            Self
        }
    }

    impl Drop for LuaContextSpoiler {
        fn drop(&mut self) {
            let mut lua: Lua = crate::hlua::global();
            lua.execute::<()>(r#"
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
            let mut lua: Lua = crate::hlua::global();
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
            let mut lua: Lua = crate::hlua::global();
            lua.execute::<()>(r#"
            package.preload.fiber = nil
            package.loaded.fiber = _fiber_backup
            _fiber_backup = nil
            "#).unwrap();
        }
    }
}
