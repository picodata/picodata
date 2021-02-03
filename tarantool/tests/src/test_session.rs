use tarantool::session;

pub fn test_uid() {
    let uid = session::uid().unwrap();
    assert_eq!(uid, 1);
}

pub fn test_euid() {
    let euid = session::euid().unwrap();
    assert_eq!(euid, 1);
}
