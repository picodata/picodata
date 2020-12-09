use std::time::Duration;

use tarantool_module::index::IteratorType;
use tarantool_module::net_box::{Conn, ConnOptions, Options};

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
        .select(IteratorType::LE, &(2,))
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
