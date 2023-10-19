#![cfg(feature = "picodata")]

use tarantool::access_control::{self, PrivType, SchemaObjectType};
use tarantool::space::Space;
use tarantool::space::SpaceCreateOptions;
use tarantool::{session, tlua};

#[track_caller]
fn grant(lua: &tlua::LuaThread, user: &str, privilege: &str, object_type: &str, object_name: &str) {
    lua.exec_with(
        "user, privilege, object_type, object_name = ...;box.schema.user.grant(user, privilege, object_type, object_name)",
        (user, privilege, object_type, object_name),
    )
    .unwrap();
}

#[track_caller]
fn revoke(
    lua: &tlua::LuaThread,
    user: &str,
    privilege: &str,
    object_type: &str,
    object_name: &str,
) {
    lua.exec_with(
        "user, privilege, object_type, object_name = ...;box.schema.user.revoke(user, privilege, object_type, object_name)",
        (user, privilege, object_type, object_name),
    )
    .unwrap();
}

#[track_caller]
fn make_user(lua: &tlua::LuaThread, name: &str) -> u32 {
    lua.exec_with("box.schema.user.create(..., {password = 'foobar'})", name)
        .unwrap();

    session::user_id_by_name(name).unwrap()
}

#[tarantool::test]
pub fn box_check_access_space() {
    let user_name = "box_access_check_space_test_user";

    let lua = tarantool::lua_state();
    let user_id = make_user(&lua, user_name);

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

    grant(&lua, user_name, "read", "space", space_name);

    {
        let _su = session::su(user_id).unwrap();

        access_control::box_access_check_space(space.id(), PrivType::Read).unwrap();

        let e = access_control::box_access_check_space(space.id(), PrivType::Write).unwrap_err();
        assert_eq!(
            e.to_string(),
            format!("tarantool error: AccessDenied: Write access to space '{space_name}' is denied for user '{user_name}'"),
        );
    }

    grant(&lua, user_name, "write", "space", space_name);

    {
        let _su = session::su(user_id).unwrap();

        access_control::box_access_check_space(space.id(), PrivType::Read).unwrap();

        access_control::box_access_check_space(space.id(), PrivType::Write).unwrap();
    }
}

#[tarantool::test]
pub fn box_check_access_ddl_space() {
    let user_name = "box_access_check_ddl_test_space";

    let lua = tarantool::lua_state();
    let user_id = make_user(&lua, user_name);

    // space
    let space_name = "test_box_access_check_ddl";
    let space = Space::create(space_name, &SpaceCreateOptions::default()).unwrap();

    // create works with passed id
    {
        let _su = session::su(user_id).unwrap();
        let e = access_control::box_access_check_ddl(
            "space_to_be_created",
            42,
            1,
            SchemaObjectType::Space,
            PrivType::Create,
        )
        .unwrap_err();

        assert_eq!(
            e.to_string(),
            format!("tarantool error: AccessDenied: Create access to space 'space_to_be_created' is denied for user '{user_name}'"),
        );
    }

    grant(&lua, user_name, "create", "space", "");

    {
        let _su = session::su(user_id).unwrap();
        access_control::box_access_check_ddl(
            "space_to_be_created",
            42,
            1,
            SchemaObjectType::Space,
            PrivType::Create,
        )
        .unwrap();
    }

    // alter drop can be granted with wildcard, check on particular entity works
    for (privilege, name) in [(PrivType::Drop, "Drop"), (PrivType::Alter, "Alter")] {
        {
            let _su = session::su(user_id).unwrap();
            let e = access_control::box_access_check_ddl(
                space_name,
                space.id(),
                1,
                SchemaObjectType::Space,
                privilege,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("tarantool error: AccessDenied: {name} access to space '{space_name}' is denied for user '{user_name}'"),
            );
        }

        grant(&lua, user_name, &name.to_lowercase(), "space", "");

        {
            let _su = session::su(user_id).unwrap();
            access_control::box_access_check_ddl(
                space_name,
                space.id(),
                1,
                SchemaObjectType::Space,
                privilege,
            )
            .unwrap();
        }
    }

    revoke(&lua, user_name, "alter", "space", "");
    revoke(&lua, user_name, "drop", "space", "");

    // alter drop on particular entity works
    for (privilege, name) in [(PrivType::Drop, "Drop"), (PrivType::Alter, "Alter")] {
        {
            let _su = session::su(user_id).unwrap();
            let e = access_control::box_access_check_ddl(
                space_name,
                space.id(),
                1,
                SchemaObjectType::Space,
                privilege,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("tarantool error: AccessDenied: {name} access to space '{space_name}' is denied for user '{user_name}'"),
            );
        }

        grant(&lua, user_name, &name.to_lowercase(), "space", &space_name);

        {
            let _su = session::su(user_id).unwrap();
            access_control::box_access_check_ddl(
                space_name,
                space.id(),
                1,
                SchemaObjectType::Space,
                privilege,
            )
            .unwrap();
        }
    }

    grant(&lua, user_name, "read,write", "space", "_space");
    grant(&lua, user_name, "read,write", "space", "_schema");
    grant(&lua, user_name, "read,write", "space", "_user");
    grant(&lua, user_name, "write", "space", "_priv");
    grant(&lua, user_name, "create", "user", "");

    // owner can grant permissions on the object to other users
    {
        let _su = session::su(user_id).unwrap();

        let grantee_user_name = format!("{user_name}_grantee");
        let grantee_user_id = make_user(&lua, &grantee_user_name);

        let space_name_grant = format!("{space_name}_grant");
        let space_grant = Space::create(&space_name_grant, &SpaceCreateOptions::default()).unwrap();

        // first check that we're allowed to grant
        access_control::box_access_check_ddl(
            &space_name_grant,
            space.id(),
            user_id,
            SchemaObjectType::Space,
            PrivType::Grant,
        )
        .unwrap();

        // check that after grant access check works
        for (privilege, name) in [
            (PrivType::Create, "Create"),
            (PrivType::Drop, "Drop"),
            (PrivType::Alter, "Alter"),
        ] {
            // owner himself has permission on an object
            access_control::box_access_check_ddl(
                &space_name_grant,
                space_grant.id(),
                user_id,
                SchemaObjectType::Space,
                privilege,
            )
            .unwrap();

            // run access check, it fails without grant
            {
                let _su = session::su(grantee_user_id).unwrap();
                let e = access_control::box_access_check_ddl(
                    &space_name_grant,
                    space_grant.id(),
                    user_id,
                    SchemaObjectType::Space,
                    privilege,
                )
                .unwrap_err();

                assert_eq!(
                    e.to_string(),
                    format!("tarantool error: AccessDenied: {name} access to space '{space_name_grant}' is denied for user '{grantee_user_name}'"),
                );
            }

            // grant permission from behalf of the user owning the space
            grant(
                &lua,
                &grantee_user_name,
                &name.to_lowercase(),
                "space",
                &space_name_grant,
            );

            {
                // access check should succeed
                let _su = session::su(grantee_user_id).unwrap();
                access_control::box_access_check_ddl(
                    &space_name_grant,
                    space_grant.id(),
                    user_id,
                    SchemaObjectType::Space,
                    privilege,
                )
                .unwrap();
            }
        }
    }
}

#[tarantool::test]
pub fn box_check_access_ddl_user() {
    // user
    // create works with passed id
    let actor_user_name = "box_access_check_ddl_test_user_actor";
    let user_name_under_test = "box_access_check_ddl_test_user";

    let lua = tarantool::lua_state();
    let actor_user_id = make_user(&lua, actor_user_name);
    let user_name_under_test_id = make_user(&lua, user_name_under_test);

    // create works with passed id
    {
        let _su = session::su(actor_user_id).unwrap();
        let e = access_control::box_access_check_ddl(
            "user_to_be_created",
            42,
            1,
            SchemaObjectType::User,
            PrivType::Create,
        )
        .unwrap_err();

        assert_eq!(
            e.to_string(),
            format!("tarantool error: AccessDenied: Create access to user 'user_to_be_created' is denied for user '{actor_user_name}'"),
        );
    }

    grant(&lua, actor_user_name, "create", "user", "");

    {
        let _su = session::su(actor_user_id).unwrap();
        access_control::box_access_check_ddl(
            "user_to_be_created",
            42,
            1,
            SchemaObjectType::User,
            PrivType::Create,
        )
        .unwrap();
    }

    // alter drop can be granted with wildcard, check on particular entity works
    for (privilege, name) in [(PrivType::Drop, "Drop"), (PrivType::Alter, "Alter")] {
        {
            let _su = session::su(actor_user_id).unwrap();
            let e = access_control::box_access_check_ddl(
                user_name_under_test,
                user_name_under_test_id,
                1,
                SchemaObjectType::User,
                privilege,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("tarantool error: AccessDenied: {name} access to user '{user_name_under_test}' is denied for user '{actor_user_name}'"),
            );
        }

        grant(&lua, actor_user_name, &name.to_lowercase(), "user", "");

        {
            let _su = session::su(actor_user_id).unwrap();
            access_control::box_access_check_ddl(
                user_name_under_test,
                user_name_under_test_id,
                1,
                SchemaObjectType::User,
                privilege,
            )
            .unwrap();
        }
    }

    revoke(&lua, actor_user_name, "alter", "user", "");
    revoke(&lua, actor_user_name, "drop", "user", "");

    // alter drop on particular entity works
    for (privilege, name) in [(PrivType::Drop, "Drop"), (PrivType::Alter, "Alter")] {
        {
            let _su = session::su(actor_user_id).unwrap();
            let e = access_control::box_access_check_ddl(
                user_name_under_test,
                user_name_under_test_id,
                1,
                SchemaObjectType::User,
                privilege,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("tarantool error: AccessDenied: {name} access to user '{user_name_under_test}' is denied for user '{actor_user_name}'"),
            );
        }

        grant(
            &lua,
            actor_user_name,
            &name.to_lowercase(),
            "user",
            user_name_under_test,
        );

        {
            let _su = session::su(actor_user_id).unwrap();
            access_control::box_access_check_ddl(
                user_name_under_test,
                user_name_under_test_id,
                1,
                SchemaObjectType::User,
                privilege,
            )
            .unwrap();
        }
    }

    grant(&lua, actor_user_name, "read,write", "space", "_space");
    grant(&lua, actor_user_name, "read,write", "space", "_schema");
    grant(&lua, actor_user_name, "read,write", "space", "_user");
    grant(&lua, actor_user_name, "write", "space", "_priv");

    // owner has all rights for created user
    // can grant permissions on it to other users
    {
        let _su = session::su(actor_user_id).unwrap();

        let grantee_user_name = format!("{actor_user_name}_grantee");
        let grantee_user_id = make_user(&lua, &grantee_user_name);

        let user_name_grant = format!("{actor_user_name}_grant");
        let user_id_grant = make_user(&lua, &user_name_grant);

        // first check that we're allowed to grant
        access_control::box_access_check_ddl(
            &grantee_user_name,
            grantee_user_id,
            actor_user_id,
            SchemaObjectType::User,
            PrivType::Grant,
        )
        .unwrap();

        // check that after grant access check works
        for (privilege, name) in [
            (PrivType::Create, "Create"),
            (PrivType::Drop, "Drop"),
            (PrivType::Alter, "Alter"),
        ] {
            // owner himself has permission on an object
            access_control::box_access_check_ddl(
                &user_name_grant,
                user_id_grant,
                actor_user_id,
                SchemaObjectType::User,
                privilege,
            )
            .unwrap();

            // run access check, it fails without grant
            {
                let _su = session::su(grantee_user_id).unwrap();
                let e = access_control::box_access_check_ddl(
                    &user_name_grant,
                    user_id_grant,
                    actor_user_id,
                    SchemaObjectType::User,
                    privilege,
                )
                .unwrap_err();

                assert_eq!(
                    e.to_string(),
                    format!("tarantool error: AccessDenied: {name} access to user '{user_name_grant}' is denied for user '{grantee_user_name}'"),
                );
            }

            // grant permission from behalf of the user owning the other user
            grant(
                &lua,
                &grantee_user_name,
                &name.to_lowercase(),
                "user",
                &user_name_grant,
            );

            {
                // access check should succeed
                let _su = session::su(grantee_user_id).unwrap();
                access_control::box_access_check_ddl(
                    &user_name_grant,
                    user_id_grant,
                    actor_user_id,
                    SchemaObjectType::User,
                    privilege,
                )
                .unwrap();
            }
        }
    }
}

#[tarantool::test]
pub fn box_check_access_ddl_role() {
    // create works with passed id
    let user_name = "box_access_check_ddl_test_role";

    let lua = tarantool::lua_state();
    let user_id = make_user(&lua, user_name);

    let role_name = "box_access_check_ddl_test_role_some_role";
    lua.exec_with("box.schema.role.create(...)", role_name)
        .unwrap();
    let role_id = lua
        .eval_with(
            "return box.space._user.index.name:select({...})[1][1];",
            role_name,
        )
        .unwrap();

    // create works with passed id
    {
        let _su = session::su(user_id).unwrap();
        let e = access_control::box_access_check_ddl(
            "role_to_be_created",
            42,
            1,
            SchemaObjectType::Role,
            PrivType::Create,
        )
        .unwrap_err();

        assert_eq!(
            e.to_string(),
            format!("tarantool error: AccessDenied: Create access to role 'role_to_be_created' is denied for user '{user_name}'"),
        );
    }

    grant(&lua, user_name, "create", "role", "");

    {
        let _su = session::su(user_id).unwrap();
        access_control::box_access_check_ddl(
            "user_to_be_created",
            42,
            1,
            SchemaObjectType::Role,
            PrivType::Create,
        )
        .unwrap();
    }

    // drop can be granted with wildcard, check on particular entity works
    {
        let _su = session::su(user_id).unwrap();
        let e = access_control::box_access_check_ddl(
            role_name,
            role_id,
            1,
            SchemaObjectType::Role,
            PrivType::Drop,
        )
        .unwrap_err();

        assert_eq!(
            e.to_string(),
            format!("tarantool error: AccessDenied: Drop access to role '{role_name}' is denied for user '{user_name}'"),
        );
    }

    grant(&lua, user_name, "drop", "role", "");

    {
        let _su = session::su(user_id).unwrap();
        access_control::box_access_check_ddl(
            role_name,
            role_id,
            1,
            SchemaObjectType::Role,
            PrivType::Drop,
        )
        .unwrap();
    }

    revoke(&lua, user_name, "drop", "role", "");

    // drop on particular entity works
    {
        let _su = session::su(user_id).unwrap();
        let e = access_control::box_access_check_ddl(
            role_name,
            role_id,
            1,
            SchemaObjectType::Role,
            PrivType::Drop,
        )
        .unwrap_err();

        assert_eq!(
            e.to_string(),
            format!("tarantool error: AccessDenied: Drop access to role '{role_name}' is denied for user '{user_name}'"),
        );
    }

    grant(&lua, user_name, "drop", "role", role_name);

    {
        let _su = session::su(user_id).unwrap();
        access_control::box_access_check_ddl(
            role_name,
            role_id,
            1,
            SchemaObjectType::Role,
            PrivType::Drop,
        )
        .unwrap();
    }

    // To check whether role can be granted box_access_check_ddl
    // is not sufficient. In order for grants to work correctly here
    // it needs help from `priv_def_check`. Internally it performs
    // the following check, it should succeed.
    {
        let _su = session::su(user_id).unwrap();
        access_control::box_access_check_ddl(
            role_name,
            role_id,
            user_id, // <-- this is unusual part, here user_id is not owner of the role but grantor.
            SchemaObjectType::Role,
            PrivType::Grant,
        )
        .unwrap();
    }
}

#[tarantool::test]
pub fn box_check_access_ddl_function() {
    let user_name = "box_access_check_ddl_test_function";

    let lua = tarantool::lua_state();
    let user_id = make_user(&lua, user_name);

    lua.exec("box.schema.func.create('sum', {body = [[function(a, b) return a + b end]]})")
        .unwrap();

    let func_name = "sum";

    // create works with passed id
    {
        let _su = session::su(user_id).unwrap();
        let e = access_control::box_access_check_ddl(
            "function_to_be_created",
            42,
            1,
            SchemaObjectType::Role,
            PrivType::Create,
        )
        .unwrap_err();

        assert_eq!(
            e.to_string(),
            format!("tarantool error: AccessDenied: Create access to role 'function_to_be_created' is denied for user '{user_name}'"),
        );
    }

    grant(&lua, user_name, "create", "function", "");

    {
        let _su = session::su(user_id).unwrap();
        access_control::box_access_check_ddl(
            "function_to_be_created",
            42,
            1,
            SchemaObjectType::Function,
            PrivType::Create,
        )
        .unwrap();
    }

    // drop execute can be granted with wildcard, check on particular entity works
    let func_id = lua
        .eval_with(
            "return box.space._func.index.name:select({...})[1][1];",
            func_name,
        )
        .unwrap();
    for (privilege, name) in [(PrivType::Drop, "Drop"), (PrivType::Execute, "Execute")] {
        {
            let _su = session::su(user_id).unwrap();
            let e = access_control::box_access_check_ddl(
                func_name,
                func_id,
                1,
                SchemaObjectType::Function,
                privilege,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("tarantool error: AccessDenied: {name} access to function '{func_name}' is denied for user '{user_name}'"),
            );
        }

        grant(&lua, user_name, &name.to_lowercase(), "function", "");

        {
            let _su = session::su(user_id).unwrap();
            access_control::box_access_check_ddl(
                func_name,
                func_id,
                1,
                SchemaObjectType::Function,
                privilege,
            )
            .unwrap();
        }
    }

    revoke(&lua, user_name, "drop", "function", "");
    revoke(&lua, user_name, "execute", "function", "");

    // drop execute on particular entity works
    for (privilege, name) in [(PrivType::Drop, "Drop"), (PrivType::Execute, "Execute")] {
        {
            let _su = session::su(user_id).unwrap();
            let e = access_control::box_access_check_ddl(
                func_name,
                func_id,
                1,
                SchemaObjectType::Function,
                privilege,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("tarantool error: AccessDenied: {name} access to function '{func_name}' is denied for user '{user_name}'"),
            );
        }

        grant(
            &lua,
            user_name,
            &name.to_lowercase(),
            "function",
            &func_name,
        );

        {
            let _su = session::su(user_id).unwrap();
            access_control::box_access_check_ddl(
                func_name,
                func_id,
                1,
                SchemaObjectType::Function,
                privilege,
            )
            .unwrap();
        }
    }
}

#[tarantool::test(should_panic)]
fn broken_entity() {
    let user_name = "broken_entity";

    let lua = tarantool::lua_state();
    let user_id = make_user(&lua, user_name);

    {
        let _su = session::su(user_id).unwrap();
        access_control::box_access_check_ddl(
            "",
            0,
            1,
            SchemaObjectType::EntitySpace,
            PrivType::Read,
        )
        .unwrap();
    }
}

#[tarantool::test(should_panic)]
fn broken_grant() {
    let user_name = "broken_grant";

    let lua = tarantool::lua_state();
    let user_id = make_user(&lua, user_name);

    {
        let _su = session::su(user_id).unwrap();
        access_control::box_access_check_ddl(
            "",
            0,
            1,
            SchemaObjectType::EntitySpace,
            PrivType::Grant,
        )
        .unwrap();
    }
}
