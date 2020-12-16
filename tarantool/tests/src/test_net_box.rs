use std::io;
use std::rc::Rc;
use std::time::Duration;

use tarantool_module::error::Error;
use tarantool_module::fiber::Fiber;
use tarantool_module::index::IteratorType;
use tarantool_module::net_box::{Conn, ConnOptions, Options};
use tarantool_module::space::Space;

use crate::common::S1Record;

pub fn test_immediate_close() {
    let _ = Conn::new("localhost:3301", ConnOptions::default()).unwrap();
}

pub fn test_ping() {
    let conn = Conn::new("localhost:3301", ConnOptions::default()).unwrap();
    conn.ping(&Options::default()).unwrap();
}

pub fn test_ping_timeout() {
    let conn = Conn::new("localhost:3301", ConnOptions::default()).unwrap();

    conn.ping(&Options {
        timeout: Some(Duration::from_millis(1)),
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
    let conn = Rc::new(Conn::new("localhost:3301", ConnOptions::default()).unwrap());

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
    fiber_b.start(conn.clone());

    fiber_a.join();
    fiber_b.join();
}

pub fn test_call() {
    let conn_options = ConnOptions {
        user: "test_user".to_string(),
        password: "password".to_string(),
        ..ConnOptions::default()
    };
    let conn = Conn::new("localhost:3301", conn_options).unwrap();
    let result = conn
        .call("test_stored_proc", &(1, 2), &Options::default())
        .unwrap();
    assert_eq!(result.unwrap().into_struct::<(i32,)>().unwrap(), (3,));
}

pub fn test_call_timeout() {
    let conn_options = ConnOptions {
        user: "test_user".to_string(),
        password: "password".to_string(),
        ..ConnOptions::default()
    };
    let conn = Conn::new("localhost:3301", conn_options).unwrap();
    let result = conn.call(
        "test_timeout",
        &Vec::<()>::new(),
        &Options {
            timeout: Some(Duration::from_millis(1)),
            ..Options::default()
        },
    );
    assert!(matches!(result, Err(Error::IO(ref e)) if e.kind() == io::ErrorKind::TimedOut));
}

pub fn test_connection_error() {
    let conn = Conn::new(
        "localhost:255",
        ConnOptions {
            reconnect_after: Duration::from_secs(0),
            ..ConnOptions::default()
        },
    )
    .unwrap();
    assert!(matches!(conn.ping(&Options::default()), Err(_)));
}

pub fn test_is_connected() {
    let conn = Conn::new(
        "localhost:3301",
        ConnOptions {
            reconnect_after: Duration::from_secs(0),
            ..ConnOptions::default()
        },
    )
    .unwrap();
    assert_eq!(conn.is_connected(), false);
    conn.ping(&Options::default()).unwrap();
    assert_eq!(conn.is_connected(), true);
}

pub fn test_schema_sync() {
    let conn = Conn::new(
        "localhost:3301",
        ConnOptions {
            user: "test_user".to_string(),
            password: "password".to_string(),
            ..ConnOptions::default()
        },
    )
    .unwrap();

    assert!(conn.space("test_s2").unwrap().is_some());
    assert!(conn.space("test_s_tmp").unwrap().is_none());

    conn.call("test_schema_update", &Vec::<()>::new(), &Options::default())
        .unwrap();
    assert!(conn.space("test_s_tmp").unwrap().is_some());

    conn.call(
        "test_schema_cleanup",
        &Vec::<()>::new(),
        &Options::default(),
    )
    .unwrap();
}

pub fn test_select() {
    let conn = Conn::new(
        "localhost:3301",
        ConnOptions {
            user: "test_user".to_string(),
            password: "password".to_string(),
            ..ConnOptions::default()
        },
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
    let mut local_space = Space::find("test_s1").unwrap();
    local_space.truncate().unwrap();

    let conn = Conn::new(
        "localhost:3301",
        ConnOptions {
            user: "test_user".to_string(),
            password: "password".to_string(),
            ..ConnOptions::default()
        },
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
    let mut local_space = Space::find("test_s1").unwrap();
    local_space.truncate().unwrap();

    let original_input = S1Record {
        id: 1,
        text: "Original".to_string(),
    };
    local_space.insert(&original_input).unwrap();

    let conn = Conn::new(
        "localhost:3301",
        ConnOptions {
            user: "test_user".to_string(),
            password: "password".to_string(),
            ..ConnOptions::default()
        },
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
