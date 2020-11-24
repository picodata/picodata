use tarantool_module::net_box::{Conn, ConnOptions, Options};

pub fn test_ping() {
    let conn = Conn::new("localhost:3301", ConnOptions::default()).unwrap();
    conn.ping(&Options::default()).unwrap();
}

pub fn test_call() {
    let conn_options = ConnOptions {
        user: "test_user".to_string(),
        password: "password".to_string(),
        ..ConnOptions::default()
    };
    let conn = Conn::new("localhost:3301", conn_options).unwrap();
    conn.call("test_stored_proc", &(1, 2), &Options::default())
        .unwrap();
}
