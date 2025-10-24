use tarantool::session;

const GUEST_UID: u32 = 0;
const ADMIN_UID: u32 = 1;

#[tarantool::test]
pub fn uid() {
    let uid = session::uid().unwrap();
    assert_eq!(uid, ADMIN_UID);
}

#[tarantool::test]
pub fn euid() {
    let euid = session::euid().unwrap();
    assert_eq!(euid, ADMIN_UID);
}

fn cur() -> u32 {
    session::uid().unwrap()
}

#[tarantool::test]
pub fn su() {
    assert_eq!(cur(), ADMIN_UID);

    let su = session::su(GUEST_UID).unwrap();
    assert_eq!(cur(), GUEST_UID);

    drop(su);

    assert_eq!(cur(), ADMIN_UID);
}

#[tarantool::test]
pub fn with_su() {
    assert_eq!(cur(), ADMIN_UID);

    session::with_su(GUEST_UID, || {
        assert_eq!(cur(), GUEST_UID);
    })
    .unwrap();

    assert_eq!(cur(), ADMIN_UID);
}

#[cfg(feature = "picodata")]
#[tarantool::test]
pub fn user_id_by_name() {
    assert_eq!(session::user_id_by_name("guest").unwrap(), GUEST_UID);
    assert_eq!(session::user_id_by_name("admin").unwrap(), ADMIN_UID);
}
