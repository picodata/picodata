#![allow(clippy::redundant_allocation)]
use std::io;
use std::rc::Rc;
use std::time::Duration;

use tarantool::error::Error;
use tarantool::fiber::Fiber;
use tarantool::index::IteratorType;
use tarantool::net_box::{Conn, ConnOptions, ConnTriggers, Options};
use tarantool::space::Space;

use crate::{
    common::{QueryOperation, S1Record, S2Record},
    LISTEN,
};
use std::cell::{Cell, RefCell};

pub fn test_immediate_close() {
    let port = unsafe { LISTEN };
    let _ = Conn::new(("localhost", port), ConnOptions::default(), None).unwrap();
}

pub fn test_ping() {
    let port = unsafe { LISTEN };
    let conn = Conn::new(("localhost", port), ConnOptions::default(), None).unwrap();
    conn.ping(&Options::default()).unwrap();
}

pub fn test_ping_timeout() {
    let port = unsafe { LISTEN };
    let conn = Conn::new(("localhost", port), ConnOptions::default(), None).unwrap();

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

pub fn test_ping_concurrent() {
    let port = unsafe { LISTEN };
    let conn = Rc::new(Conn::new(("localhost", port), ConnOptions::default(), None).unwrap());

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

pub fn test_call() {
    let port = unsafe { LISTEN };
    let conn_options = ConnOptions {
        user: "test_user".to_string(),
        password: "password".to_string(),
        ..ConnOptions::default()
    };
    let conn = Conn::new(("localhost", port), conn_options, None).unwrap();
    let result = conn
        .call("test_stored_proc", &(1, 2), &Options::default())
        .unwrap();
    assert_eq!(result.unwrap().into_struct::<(i32,)>().unwrap(), (3,));
}

pub fn test_call_timeout() {
    let port = unsafe { LISTEN };
    let conn_options = ConnOptions {
        user: "test_user".to_string(),
        password: "password".to_string(),
        ..ConnOptions::default()
    };
    let conn = Conn::new(("localhost", port), conn_options, None).unwrap();
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

pub fn test_eval() {
    let port = unsafe { LISTEN };
    let conn_options = ConnOptions {
        user: "test_user".to_string(),
        password: "password".to_string(),
        ..ConnOptions::default()
    };
    let conn = Conn::new(("localhost", port), conn_options, None).unwrap();
    let result = conn
        .eval("return ...", &(1, 2), &Options::default())
        .unwrap();
    assert_eq!(result.unwrap().into_struct::<(i32, i32)>().unwrap(), (1, 2));
}

pub fn test_connection_error() {
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

pub fn test_is_connected() {
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

pub fn test_schema_sync() {
    let port = unsafe { LISTEN };
    let conn = Conn::new(
        ("localhost", port),
        ConnOptions {
            user: "test_user".to_string(),
            password: "password".to_string(),
            ..ConnOptions::default()
        },
        None,
    )
    .unwrap();

    assert!(conn.space("test_s2").unwrap().is_some());
    assert!(conn.space("test_s_tmp").unwrap().is_none());

    conn.call("test_schema_update", Vec::<()>::new().as_slice(), &Options::default())
        .unwrap();
    assert!(conn.space("test_s_tmp").unwrap().is_some());

    conn.call(
        "test_schema_cleanup",
        Vec::<()>::new().as_slice(),
        &Options::default(),
    )
    .unwrap();
}

pub fn test_get() {
    let port = unsafe { LISTEN };
    let conn = Conn::new(
        ("localhost", port),
        ConnOptions {
            user: "test_user".to_string(),
            password: "password".to_string(),
            ..ConnOptions::default()
        },
        None,
    )
    .unwrap();
    let space = conn.space("test_s2").unwrap().unwrap();

    let idx = space.index("idx_1").unwrap().unwrap();
    let output = idx
        .get(&("key_16".to_string(),), &Options::default())
        .unwrap();
    assert!(output.is_some());
    assert_eq!(
        output.unwrap().into_struct::<S2Record>().unwrap(),
        S2Record {
            id: 16,
            key: "key_16".to_string(),
            value: "value_16".to_string(),
            a: 1,
            b: 3
        }
    );
}

pub fn test_select() {
    let port = unsafe { LISTEN };
    let conn = Conn::new(
        ("localhost", port),
        ConnOptions {
            user: "test_user".to_string(),
            password: "password".to_string(),
            ..ConnOptions::default()
        },
        None,
    )
    .unwrap();
    let space = conn.space("test_s2").unwrap().unwrap();

    let result: Vec<S1Record> = space
        .select(IteratorType::LE, &(2,), &Options::default())
        .unwrap()
        .map(|x| x.into_struct().unwrap())
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

pub fn test_insert() {
    let port = unsafe { LISTEN };
    let mut local_space = Space::find("test_s1").unwrap();
    local_space.truncate().unwrap();

    let conn = Conn::new(
        ("localhost", port),
        ConnOptions {
            user: "test_user".to_string(),
            password: "password".to_string(),
            ..ConnOptions::default()
        },
        None,
    )
    .unwrap();
    let mut remote_space = conn.space("test_s1").unwrap().unwrap();

    let input = S1Record {
        id: 1,
        text: "Test".to_string(),
    };
    let insert_result = remote_space.insert(&input, &Options::default()).unwrap();
    assert!(insert_result.is_some());
    assert_eq!(
        insert_result.unwrap().into_struct::<S1Record>().unwrap(),
        input
    );

    let output = local_space.get(&(input.id,)).unwrap();
    assert!(output.is_some());
    assert_eq!(output.unwrap().into_struct::<S1Record>().unwrap(), input);
}

pub fn test_replace() {
    let port = unsafe { LISTEN };
    let mut local_space = Space::find("test_s1").unwrap();
    local_space.truncate().unwrap();

    let original_input = S1Record {
        id: 1,
        text: "Original".to_string(),
    };
    local_space.insert(&original_input).unwrap();

    let conn = Conn::new(
        ("localhost", port),
        ConnOptions {
            user: "test_user".to_string(),
            password: "password".to_string(),
            ..ConnOptions::default()
        },
        None,
    )
    .unwrap();
    let mut remote_space = conn.space("test_s1").unwrap().unwrap();

    let new_input = S1Record {
        id: original_input.id,
        text: "New".to_string(),
    };
    let replace_result = remote_space
        .replace(&new_input, &Options::default())
        .unwrap();
    assert!(replace_result.is_some());
    assert_eq!(
        replace_result.unwrap().into_struct::<S1Record>().unwrap(),
        new_input
    );

    let output = local_space.get(&(new_input.id,)).unwrap();
    assert!(output.is_some());
    assert_eq!(
        output.unwrap().into_struct::<S1Record>().unwrap(),
        new_input
    );
}

pub fn test_update() {
    let port = unsafe { LISTEN };
    let mut local_space = Space::find("test_s1").unwrap();
    local_space.truncate().unwrap();

    let input = S1Record {
        id: 1,
        text: "Original".to_string(),
    };
    local_space.insert(&input).unwrap();

    let conn = Conn::new(
        ("localhost", port),
        ConnOptions {
            user: "test_user".to_string(),
            password: "password".to_string(),
            ..ConnOptions::default()
        },
        None,
    )
    .unwrap();
    let mut remote_space = conn.space("test_s1").unwrap().unwrap();

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
        update_result
            .unwrap()
            .into_struct::<S1Record>()
            .unwrap()
            .text,
        "New"
    );

    let output = local_space.get(&(input.id,)).unwrap();
    assert_eq!(
        output.unwrap().into_struct::<S1Record>().unwrap().text,
        "New"
    );
}

pub fn test_upsert() {
    let port = unsafe { LISTEN };
    let mut local_space = Space::find("test_s1").unwrap();
    local_space.truncate().unwrap();

    let original_input = S1Record {
        id: 1,
        text: "Original".to_string(),
    };
    local_space.insert(&original_input).unwrap();

    let conn = Conn::new(
        ("localhost", port),
        ConnOptions {
            user: "test_user".to_string(),
            password: "password".to_string(),
            ..ConnOptions::default()
        },
        None,
    )
    .unwrap();
    let mut remote_space = conn.space("test_s1").unwrap().unwrap();

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
    assert_eq!(
        output.unwrap().into_struct::<S1Record>().unwrap().text,
        "Test 1"
    );

    let output = local_space.get(&(2,)).unwrap();
    assert_eq!(
        output.unwrap().into_struct::<S1Record>().unwrap().text,
        "New"
    );
}

pub fn test_delete() {
    let port = unsafe { LISTEN };
    let mut local_space = Space::find("test_s1").unwrap();
    local_space.truncate().unwrap();

    let input = S1Record {
        id: 1,
        text: "Test".to_string(),
    };
    local_space.insert(&input).unwrap();

    let conn = Conn::new(
        ("localhost", port),
        ConnOptions {
            user: "test_user".to_string(),
            password: "password".to_string(),
            ..ConnOptions::default()
        },
        None,
    )
    .unwrap();
    let mut remote_space = conn.space("test_s1").unwrap().unwrap();

    let delete_result = remote_space
        .delete(&(input.id,), &Options::default())
        .unwrap();
    assert!(delete_result.is_some());
    assert_eq!(
        delete_result.unwrap().into_struct::<S1Record>().unwrap(),
        input
    );

    let output = local_space.get(&(input.id,)).unwrap();
    assert!(output.is_none());
}

pub fn test_cancel_recv() {
    let port = unsafe { LISTEN };
    let conn = Rc::new(Conn::new(("localhost", port), ConnOptions::default(), None).unwrap());

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

pub fn test_triggers_connect() {
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

pub fn test_triggers_reject() {
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

pub fn test_triggers_schema_sync() {
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
