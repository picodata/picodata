#![cfg(feature = "picodata")]

use tarantool::access_control::{self, PrivType};
use tarantool::session;
use tarantool::space::Space;
use tarantool::space::SpaceCreateOptions;

#[tarantool::test]
pub fn box_check_access_space() {
    let user_name = "box_access_check_space_test_user";

    let lua = tarantool::lua_state();
    lua.exec_with(
        "box.schema.user.create(..., {password = 'foobar'})",
        user_name,
    )
    .unwrap();

    let user_id = session::user_id_by_name(user_name).unwrap();

    let space_name = "test_box_access_check_space";
    let opts = SpaceCreateOptions::default();

    let space = Space::create(space_name, &opts).unwrap();
    {
        let _su = session::su(user_id).unwrap();
        let e = access_control::box_access_check_space(space.id(), PrivType::Read).unwrap_err();
        assert_eq!(
            e.to_string(),
            format!("tarantool error: AccessDenied: Read access to space '{space_name}' is denied for user '{user_name}'"),
        );

        let e = access_control::box_access_check_space(space.id(), PrivType::Write).unwrap_err();
        assert_eq!(
            e.to_string(),
            format!("tarantool error: AccessDenied: Write access to space '{space_name}' is denied for user '{user_name}'"),
        );
    }

    lua.exec_with(
        "user, space = ...;box.schema.user.grant(user, 'read', 'space', space)",
        (user_name, space_name),
    )
    .unwrap();

    {
        let _su = session::su(user_id).unwrap();

        access_control::box_access_check_space(space.id(), PrivType::Read).unwrap();

        let e = access_control::box_access_check_space(space.id(), PrivType::Write).unwrap_err();
        assert_eq!(
            e.to_string(),
            format!("tarantool error: AccessDenied: Write access to space '{space_name}' is denied for user '{user_name}'"),
        );
    }

    lua.exec_with(
        "user, space = ...;box.schema.user.grant(user, 'write', 'space', space)",
        (user_name, space_name),
    )
    .unwrap();

    {
        let _su = session::su(user_id).unwrap();

        access_control::box_access_check_space(space.id(), PrivType::Read).unwrap();

        access_control::box_access_check_space(space.id(), PrivType::Write).unwrap();
    }
}
