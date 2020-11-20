use tarantool_module::net_box::{Conn, ConnOptions, Options};
use url::Url;

pub fn test_ping() {
    let conn = Conn::new(
        Url::parse("localhost:3301").unwrap(),
        ConnOptions::default(),
    );
    conn.ping(&Options::default()).unwrap();
}

pub fn test_call() {
    let conn = Conn::new(
        Url::parse("localhost:3301").unwrap(),
        ConnOptions::default(),
    );
    conn.call("test_stored_proc", &(1, 2), &Options::default())
        .unwrap();
}
