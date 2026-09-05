use tarantool::session;

#[cfg(feature = "picodata")]
use tarantool::{
    auth::AuthMethod,
    error::{Error, TarantoolErrorCode},
    net_box::{Conn, ConnOptions, Options},
    test::util::listen_port,
};

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

/// Previously, `su()` assumed that reading the current user ID was infallible
/// and called `Result::expect` on `uid()`. If another session deleted the
/// authenticated user, `uid()` returned an error and `su()` panicked even
/// though its API returns `Result`. Instead, `su()` should propagate that
/// error to its caller without changing the session.
///
/// To verify this, switch this session to `user_name`, then delete that user
/// through the separate `dropper` connection (Tarantool prevents a session from
/// dropping its own active user). Check that both `uid()` and `su()` return
/// errors, then drop the original `SuGuard` and verify that it restores the
/// admin user and leaves the session usable.
#[cfg(feature = "picodata")]
#[tarantool::test]
pub fn su_after_current_user_is_dropped() {
    let lua = tarantool::lua_state();
    let user_name = "test_su_dropped_user";
    lua.exec_with("box.schema.user.create(...)", user_name)
        .unwrap();
    let user_id = session::user_id_by_name(user_name).unwrap();
    let dropper = Conn::new(
        ("127.0.0.1", listen_port()),
        ConnOptions {
            user: "test_user".into(),
            password: "password".into(),
            auth_method: AuthMethod::ChapSha1,
            ..ConnOptions::default()
        },
        None,
    )
    .unwrap();

    let temporary_user = session::su(user_id).unwrap();
    dropper
        .eval(
            "box.schema.user.drop(...)",
            &(user_name,),
            &Options::default(),
        )
        .unwrap();

    let expected_code = TarantoolErrorCode::NoSuchUser as u32;
    assert!(matches!(
        session::uid(),
        Err(Error::Tarantool(error)) if error.error_code() == expected_code
    ));
    assert!(matches!(
        session::su(ADMIN_UID),
        Err(Error::Tarantool(error)) if error.error_code() == expected_code
    ));

    drop(temporary_user);
    assert_eq!(cur(), ADMIN_UID);
}

#[cfg(feature = "picodata")]
#[tarantool::test]
pub fn user_id_by_name() {
    assert_eq!(session::user_id_by_name("guest").unwrap(), GUEST_UID);
    assert_eq!(session::user_id_by_name("admin").unwrap(), ADMIN_UID);
}
