use std::time::Duration;
use tarantool_module::net_box::{Conn, ConnOptions, Options};

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
