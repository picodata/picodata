use tarantool::session;

pub fn uid() {
    let uid = session::uid().unwrap();
    assert_eq!(uid, 1);
}

pub fn euid() {
    let euid = session::euid().unwrap();
    assert_eq!(euid, 1);
}
