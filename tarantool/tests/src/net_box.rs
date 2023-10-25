#![allow(clippy::redundant_allocation)]
use std::io;
use std::rc::Rc;
use std::time::Duration;

use tarantool::error::Error;
use tarantool::fiber::{reschedule, sleep, start, Cond, Fiber};
use tarantool::index::IteratorType;
use tarantool::net_box::{promise::State, Conn, ConnOptions, ConnTriggers, Options};
use tarantool::space::Space;
use tarantool::tuple::Tuple;

use crate::{
    common::{QueryOperation, S1Record, S2Record},
    LISTEN,
};
use std::cell::{Cell, RefCell};

fn default_conn() -> Conn {
    Conn::new(
        ("localhost", unsafe { LISTEN }),
        ConnOptions::default(),
        None,
    )
    .unwrap()
}

fn test_user_conn() -> Conn {
    Conn::new(
        ("localhost", unsafe { LISTEN }),
        ConnOptions {
            user: "test_user".into(),
            password: "password".into(),
            ..ConnOptions::default()
        },
        None,
    )
    .unwrap()
}

pub fn immediate_close() {
    let port = unsafe { LISTEN };
    let _ = Conn::new(("localhost", port), ConnOptions::default(), None).unwrap();
}

pub fn ping() {
    let conn = default_conn();
    conn.ping(&Options::default()).unwrap();
}

pub fn execute() {
    Space::find("test_s1")
        .unwrap()
        .insert(&(6001, "6001"))
        .unwrap();
    Space::find("test_s1")
        .unwrap()
        .insert(&(6002, "6002"))
        .unwrap();

    let lua = tarantool::lua_state();
    // Error is silently ignored on older versions, before 'compat' was introduced.
    _ = lua.exec("require'compat'.sql_seq_scan_default = 'old'");

    let conn: Conn = test_user_conn();

    let result = conn
        .execute(r#"SELECT * FROM "test_s1""#, &(), &Options::default())
        .expect("IPROTO execute sql request fail");
    assert!(result.len() >= 2);

    let result = conn
        .execute(
            r#"SELECT * FROM "test_s1" WHERE "id" = ?"#,
            &(6002,),
            &Options::default(),
        )
        .expect("IPROTO execute sql request fail");

    assert_eq!(result.len(), 1);
    assert_eq!(
        result.get(0).unwrap().decode::<(u64, String)>().unwrap(),
        (6002, "6002".to_string())
    );
}

pub fn ping_timeout() {
    let conn = default_conn();

    conn.ping(&Options {
        timeout: Some(Duration::from_secs(1)),
        ..Options::default()
    })
    .unwrap();

    conn.ping(&Options {
        timeout: None,
        ..Options::default()
    })
    .unwrap();
}

pub fn ping_concurrent() {
    let conn = Rc::new(default_conn());

    let mut fiber_a = Fiber::new("test_fiber_a", &mut |conn: Box<Rc<Conn>>| {
        conn.ping(&Options::default()).unwrap();
        0
    });
    fiber_a.set_joinable(true);

    let mut fiber_b = Fiber::new("test_fiber_b", &mut |conn: Box<Rc<Conn>>| {
        conn.ping(&Options::default()).unwrap();
        0
    });
    fiber_b.set_joinable(true);

    fiber_a.start(conn.clone());
    fiber_b.start(conn);

    fiber_a.join();
    fiber_b.join();
}

pub fn call() {
    let conn = test_user_conn();
    let result = conn
        .call("test_stored_proc", &(1, 2), &Options::default())
        .unwrap();
    assert_eq!(result.unwrap().decode::<(i32,)>().unwrap(), (3,));
}

pub fn call_async() {
    let conn = test_user_conn();
    let p1 = conn
        .call_async::<_, Tuple>("test_stored_proc", (69, 420))
        .unwrap();
    let p2 = conn
        .call_async::<_, (i32,)>("test_stored_proc", (13, 37))
        .unwrap();
    assert_eq!(p1.state(), State::Pending);
    assert_eq!(p2.state(), State::Pending);
    assert_eq!(p2.wait().unwrap(), (50,));
    assert_eq!(p1.state(), State::Kept);
    let tuple: Tuple = p1.wait().unwrap();
    assert_eq!(tuple.decode::<(i32,)>().unwrap(), (489,));
}

pub fn call_async_error() {
    let conn = test_user_conn();
    let p = conn
        .call_async::<_, ()>("Procedure is not defined", ())
        .unwrap();
    assert_eq!(
        p.wait().unwrap_err().to_string(),
        "server responded with error: Procedure 'Procedure is not defined' is not defined"
    );

    let mut p = conn
        .call_async::<_, ()>("Procedure is not defined", ())
        .unwrap();
    let cond = Rc::new(Cond::new());
    p.replace_cond(cond.clone());
    cond.wait();
    assert_eq!(p.state(), State::ReceivedError);
}

pub fn call_async_disconnected() {
    let conn = test_user_conn();
    let p = conn
        .call_async::<_, (i32,)>("test_stored_proc", (1, 1))
        .unwrap();
    assert_eq!(p.state(), State::Pending);
    let p = p.try_get().pending().unwrap();
    drop(conn);
    assert_eq!(p.state(), State::Disconnected);
    assert_eq!(p.wait().unwrap_err().to_string(), "io error: not connected");

    let conn = test_user_conn();
    let p = conn
        .call_async::<_, (i32,)>("test_stored_proc", (1, 1))
        .unwrap();
    sleep(Duration::from_millis(100));
    drop(conn);
    assert_eq!(p.state(), State::Kept);
    assert_eq!(p.try_get().ok(), Some((2,)));
}

pub fn call_timeout() {
    let conn = test_user_conn();
    let result = conn.call(
        "test_timeout",
        Vec::<()>::new().as_slice(),
        &Options {
            timeout: Some(Duration::from_millis(1)),
            ..Options::default()
        },
    );
    assert!(matches!(result, Err(Error::IO(ref e)) if e.kind() == io::ErrorKind::TimedOut));
}

pub fn call_async_timeout() {
    let conn = test_user_conn();
    let p = conn.call_async::<_, ()>("test_timeout", ()).unwrap();
    assert_eq!(p.state(), State::Pending);
    let _ = p
        .wait_timeout(Duration::from_millis(100))
        .pending()
        .unwrap();
}

pub fn call_async_wait_disconnected() {
    let conn = test_user_conn();
    let p = conn.call_async::<_, ()>("test_timeout", ()).unwrap();
    let jh = start(|| {
        reschedule();
        drop(conn);
    });
    assert_eq!(p.wait().unwrap_err().to_string(), "io error: not connected");
    jh.join();
}

pub fn eval() {
    let conn = test_user_conn();
    let result = conn
        .eval("return ...", &(1, 2), &Options::default())
        .unwrap();
    assert_eq!(result.unwrap().decode::<(i32, i32)>().unwrap(), (1, 2));
}

pub fn eval_async() {
    let conn = test_user_conn();
    let expr = "return require 'math'.modf(...)";
    let p1 = conn.eval_async(expr, (13.37,)).unwrap();
    let p2 = conn.eval_async(expr, (420.69,)).unwrap();
    assert_eq!(p2.wait().ok(), Some((420, 0.69f32)));
    assert_eq!(p1.wait().ok(), Some((13, 0.37f32)));
}

pub fn async_common_cond() {
    tarantool::lua_state()
        .exec(
            "
        box.schema.func.create('async_common_cond_proc', {body = [[
            function(timeout, value)
                require'fiber'.sleep(timeout)
                return value
            end
        ]]})
    ",
        )
        .unwrap();
    let conn = test_user_conn();
    let mut p1 = conn
        .call_async("async_common_cond_proc", (0.300, "one"))
        .unwrap();
    let mut p2 = conn
        .call_async("async_common_cond_proc", (0.100, "two"))
        .unwrap();
    let mut p3 = conn
        .call_async("async_common_cond_proc", (0.200, "three"))
        .unwrap();
    let cond = Rc::new(Cond::new());
    p1.replace_cond(cond.clone());
    p2.replace_cond(cond.clone());
    p3.replace_cond(cond.clone());
    assert_eq!(p1.state(), State::Pending);
    assert_eq!(p2.state(), State::Pending);
    assert_eq!(p3.state(), State::Pending);
    cond.wait();
    assert_eq!(p1.state(), State::Pending);
    assert_eq!(p2.try_get().ok(), Some(("two".to_string(),)));
    assert_eq!(p3.state(), State::Pending);
    cond.wait();
    assert_eq!(p1.state(), State::Pending);
    assert_eq!(p3.try_get().ok(), Some(("three".to_string(),)));
    cond.wait();
    assert_eq!(p1.try_get().ok(), Some(("one".to_string(),)));
}

pub fn connection_error() {
    let conn = Conn::new(
        "localhost:255",
        ConnOptions {
            reconnect_after: Duration::from_secs(0),
            ..ConnOptions::default()
        },
        None,
    )
    .unwrap();
    assert!(matches!(conn.ping(&Options::default()), Err(_)));
}

pub fn is_connected() {
    let port = unsafe { LISTEN };
    let conn = Conn::new(
        ("localhost", port),
        ConnOptions {
            reconnect_after: Duration::from_secs(0),
            ..ConnOptions::default()
        },
        None,
    )
    .unwrap();
    assert_eq!(conn.is_connected(), false);
    conn.ping(&Options::default()).unwrap();
    assert_eq!(conn.is_connected(), true);
}

pub fn schema_sync() {
    let conn = test_user_conn();

    assert!(conn.space("test_s2").unwrap().is_some());
    assert!(conn.space("test_s_tmp").unwrap().is_none());

    conn.call(
        "test_schema_update",
        Vec::<()>::new().as_slice(),
        &Options::default(),
    )
    .unwrap();
    assert!(conn.space("test_s_tmp").unwrap().is_some());

    conn.call(
        "test_schema_cleanup",
        Vec::<()>::new().as_slice(),
        &Options::default(),
    )
    .unwrap();
}

pub fn get() {
    let conn = test_user_conn();
    let space = conn.space("test_s2").unwrap().unwrap();

    let idx = space.index("idx_1").unwrap().unwrap();
    let output = idx
        .get(&("key_16".to_string(),), &Options::default())
        .unwrap();
    assert!(output.is_some());
    assert_eq!(
        output.unwrap().decode::<S2Record>().unwrap(),
        S2Record {
            id: 16,
            key: "key_16".to_string(),
            value: "value_16".to_string(),
            a: 1,
            b: 3
        }
    );
}

pub fn select() {
    let conn = test_user_conn();
    let space = conn.space("test_s2").unwrap().unwrap();

    let result: Vec<S1Record> = space
        .select(IteratorType::LE, &(2,), &Options::default())
        .unwrap()
        .map(|x| x.decode().unwrap())
        .collect();

    assert_eq!(
        result,
        vec![
            S1Record {
                id: 2,
                text: "key_2".to_string()
            },
            S1Record {
                id: 1,
                text: "key_1".to_string()
            }
        ]
    );
}

pub fn insert() {
    let local_space = Space::find("test_s1").unwrap();
    local_space.truncate().unwrap();

    let conn = test_user_conn();
    let remote_space = conn.space("test_s1").unwrap().unwrap();

    let input = S1Record {
        id: 1,
        text: "Test".to_string(),
    };
    let insert_result = remote_space.insert(&input, &Options::default()).unwrap();
    assert!(insert_result.is_some());
    assert_eq!(insert_result.unwrap().decode::<S1Record>().unwrap(), input);

    let output = local_space.get(&(input.id,)).unwrap();
    assert!(output.is_some());
    assert_eq!(output.unwrap().decode::<S1Record>().unwrap(), input);
}

pub fn replace() {
    let local_space = Space::find("test_s1").unwrap();
    local_space.truncate().unwrap();

    let original_input = S1Record {
        id: 1,
        text: "Original".to_string(),
    };
    local_space.insert(&original_input).unwrap();

    let conn = test_user_conn();
    let remote_space = conn.space("test_s1").unwrap().unwrap();

    let new_input = S1Record {
        id: original_input.id,
        text: "New".to_string(),
    };
    let replace_result = remote_space
        .replace(&new_input, &Options::default())
        .unwrap();
    assert!(replace_result.is_some());
    assert_eq!(
        replace_result.unwrap().decode::<S1Record>().unwrap(),
        new_input
    );

    let output = local_space.get(&(new_input.id,)).unwrap();
    assert!(output.is_some());
    assert_eq!(output.unwrap().decode::<S1Record>().unwrap(), new_input);
}

pub fn update() {
    let local_space = Space::find("test_s1").unwrap();
    local_space.truncate().unwrap();

    let input = S1Record {
        id: 1,
        text: "Original".to_string(),
    };
    local_space.insert(&input).unwrap();

    let conn = test_user_conn();
    let remote_space = conn.space("test_s1").unwrap().unwrap();

    let update_result = remote_space
        .update(
            &(input.id,),
            &[QueryOperation {
                op: "=".to_string(),
                field_id: 1,
                value: "New".into(),
            }],
            &Options::default(),
        )
        .unwrap();
    assert!(update_result.is_some());
    assert_eq!(
        update_result.unwrap().decode::<S1Record>().unwrap().text,
        "New"
    );

    let output = local_space.get(&(input.id,)).unwrap();
    assert_eq!(output.unwrap().decode::<S1Record>().unwrap().text, "New");
}

pub fn upsert() {
    let local_space = Space::find("test_s1").unwrap();
    local_space.truncate().unwrap();

    let original_input = S1Record {
        id: 1,
        text: "Original".to_string(),
    };
    local_space.insert(&original_input).unwrap();

    let conn = test_user_conn();
    let remote_space = conn.space("test_s1").unwrap().unwrap();

    remote_space
        .upsert(
            &S1Record {
                id: 1,
                text: "New".to_string(),
            },
            &[QueryOperation {
                op: "=".to_string(),
                field_id: 1,
                value: "Test 1".into(),
            }],
            &Options::default(),
        )
        .unwrap();

    remote_space
        .upsert(
            &S1Record {
                id: 2,
                text: "New".to_string(),
            },
            &[QueryOperation {
                op: "=".to_string(),
                field_id: 1,
                value: "Test 2".into(),
            }],
            &Options::default(),
        )
        .unwrap();

    let output = local_space.get(&(1,)).unwrap();
    assert_eq!(output.unwrap().decode::<S1Record>().unwrap().text, "Test 1");

    let output = local_space.get(&(2,)).unwrap();
    assert_eq!(output.unwrap().decode::<S1Record>().unwrap().text, "New");
}

pub fn delete() {
    let local_space = Space::find("test_s1").unwrap();
    local_space.truncate().unwrap();

    let input = S1Record {
        id: 1,
        text: "Test".to_string(),
    };
    local_space.insert(&input).unwrap();

    let conn = test_user_conn();
    let remote_space = conn.space("test_s1").unwrap().unwrap();

    let delete_result = remote_space
        .delete(&(input.id,), &Options::default())
        .unwrap();
    assert!(delete_result.is_some());
    assert_eq!(delete_result.unwrap().decode::<S1Record>().unwrap(), input);

    let output = local_space.get(&(input.id,)).unwrap();
    assert!(output.is_none());
}

pub fn cancel_recv() {
    let conn = Rc::new(default_conn());

    let mut fiber = Fiber::new("test_fiber_a", &mut |conn: Box<Rc<Conn>>| {
        for _ in 0..10 {
            match conn.ping(&Options::default()) {
                Ok(_) => {}
                Err(Error::IO(e)) if e.kind() == io::ErrorKind::ConnectionAborted => {}
                Err(Error::IO(e)) if e.kind() == io::ErrorKind::NotConnected => {}
                e => e.unwrap(),
            }
        }
        0
    });
    fiber.set_joinable(true);
    fiber.start(conn.clone());
    conn.close();
    fiber.join();
}

pub fn triggers_connect() {
    let port = unsafe { LISTEN };
    struct Checklist {
        connected: bool,
        disconnected: bool,
    }

    struct TriggersMock {
        checklist: Rc<RefCell<Checklist>>,
    }

    impl ConnTriggers for TriggersMock {
        fn on_connect(&self, _: &Conn) -> Result<(), Error> {
            self.checklist.borrow_mut().connected = true;
            Ok(())
        }
        fn on_disconnect(&self) {
            self.checklist.borrow_mut().disconnected = true;
        }
        fn on_schema_reload(&self, _: &Conn) {}
    }

    let checklist = Rc::new(RefCell::new(Checklist {
        connected: false,
        disconnected: false,
    }));

    let conn = Conn::new(
        ("localhost", port),
        ConnOptions::default(),
        Some(Rc::new(TriggersMock {
            checklist: checklist.clone(),
        })),
    )
    .unwrap();
    conn.ping(&Options::default()).unwrap();
    conn.close();

    assert_eq!(checklist.borrow().connected, true);
    assert_eq!(checklist.borrow().disconnected, true);
}

pub fn triggers_reject() {
    let port = unsafe { LISTEN };
    struct TriggersMock {}

    impl ConnTriggers for TriggersMock {
        fn on_connect(&self, _: &Conn) -> Result<(), Error> {
            Err(Error::IO(io::ErrorKind::Interrupted.into()))
        }
        fn on_disconnect(&self) {}
        fn on_schema_reload(&self, _: &Conn) {}
    }

    let conn = Conn::new(
        ("localhost", port),
        ConnOptions::default(),
        Some(Rc::new(TriggersMock {})),
    )
    .unwrap();

    let res = conn.ping(&Options::default());
    assert!(matches!(res, Err(Error::IO(err)) if err.kind() == io::ErrorKind::Interrupted));
}

pub fn triggers_schema_sync() {
    let port = unsafe { LISTEN };
    struct TriggersMock {
        is_trigger_called: Rc<Cell<bool>>,
    }

    impl ConnTriggers for TriggersMock {
        fn on_connect(&self, _: &Conn) -> Result<(), Error> {
            Ok(())
        }
        fn on_disconnect(&self) {}

        fn on_schema_reload(&self, _: &Conn) {
            self.is_trigger_called.set(true);
        }
    }

    let is_trigger_called = Rc::new(Cell::new(false));

    let conn = Conn::new(
        ("localhost", port),
        ConnOptions {
            user: "test_user".to_string(),
            password: "password".to_string(),
            ..ConnOptions::default()
        },
        Some(Rc::new(TriggersMock {
            is_trigger_called: is_trigger_called.clone(),
        })),
    )
    .unwrap();

    conn.call("test_schema_update", &[()][..0], &Options::default())
        .unwrap();
    conn.call(
        "test_schema_cleanup",
        Vec::<()>::new().as_slice(),
        &Options::default(),
    )
    .unwrap();
    conn.space("test_s2").unwrap().unwrap();
    conn.close();

    assert_eq!(is_trigger_called.get(), true);
}
