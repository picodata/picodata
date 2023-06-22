import pytest
from conftest import Cluster, TarantoolError, ReturnError
from tarantool.error import NetworkError  # type: ignore


def test_acl_lua_api(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    #
    # pico.create_user
    #

    # No user -> error.
    with pytest.raises(ReturnError, match="user should be a string"):
        i1.call("pico.create_user")

    # No password -> error.
    with pytest.raises(ReturnError, match="password should be a string"):
        i1.call("pico.create_user", "Dave")

    # This is probably not ok.
    i1.call("pico.create_user", "Dave", "")

    # Already exists -> error.
    with pytest.raises(ReturnError, match="User 'Dave' already exists"):
        i1.call("pico.create_user", "Dave", "")

    # Role already exists -> error.
    with pytest.raises(ReturnError, match="Role 'super' already exists"):
        i1.call("pico.create_user", "super", "")

    #
    # pico.create_role
    #

    # No role -> error.
    with pytest.raises(ReturnError, match="role should be a string"):
        i1.call("pico.create_role")

    # Ok.
    i1.call("pico.create_role", "Parent")

    # Already exists -> error.
    with pytest.raises(ReturnError, match="Role 'Parent' already exists"):
        i1.call("pico.create_role", "Parent")

    # User already exists -> error.
    with pytest.raises(ReturnError, match="User 'Dave' already exists"):
        i1.call("pico.create_role", "Dave")

    #
    # pico.grant_privilege / pico.revoke_privilege parameter verification
    #

    for f in ["grant_privilege", "revoke_privilege"]:
        # No user -> error.
        with pytest.raises(ReturnError, match="grantee should be a string"):
            i1.call(f"pico.{f}")

        # No such user -> error.
        with pytest.raises(ReturnError, match="User 'User is not found' is not found"):
            i1.call(f"pico.{f}", "User is not found", "execute", "universe")

        # No privilege -> error.
        with pytest.raises(ReturnError, match="privilege should be a string"):
            i1.call(f"pico.{f}", "Dave")

        # No such privilege -> error.
        with pytest.raises(
            ReturnError,
            match=rf"unsupported privilege 'boogie', see pico.help\('{f}'\) for details",
        ):
            i1.call(f"pico.{f}", "Dave", "boogie", "universe")

        # Comma separated list of privileges -> error.
        with pytest.raises(
            ReturnError,
            match=rf"unsupported privilege 'read,write', see pico.help\('{f}'\) for details",
        ):
            i1.call(f"pico.{f}", "Dave", "read,write", "universe")

        # No object_type -> error.
        with pytest.raises(ReturnError, match="object_type should be a string"):
            i1.call(f"pico.{f}", "Dave", "read")

        # No such object_type -> error.
        with pytest.raises(ReturnError, match="Unknown object type 'bible'"):
            i1.call(f"pico.{f}", "Dave", "read", "bible")

        # Wrong combo -> error.
        with pytest.raises(ReturnError, match="Unsupported space privilege 'grant'"):
            i1.call(f"pico.{f}", "Dave", "grant", "space")

        # No such role -> error.
        with pytest.raises(ReturnError, match="Role 'Joker' is not found"):
            i1.call(f"pico.{f}", "Dave", "execute", "role", "Joker")

    #
    # pico.grant_privilege semantics verification
    #

    # Grant privilege to user -> Ok.
    i1.call("pico.grant_privilege", "Dave", "read", "space", "_pico_property")

    # Already granted -> error.
    with pytest.raises(
        ReturnError,
        match="User 'Dave' already has read access on space '_pico_property'",
    ):
        i1.call("pico.grant_privilege", "Dave", "read", "space", "_pico_property")

    # Grant privilege to role -> Ok.
    i1.call("pico.grant_privilege", "Parent", "write", "space", "_pico_property")

    # Already granted -> error.
    # FIXME: tarantool says User instead of Role.
    with pytest.raises(
        ReturnError,
        match="User 'Parent' already has write access on space '_pico_property'",
    ):
        i1.call("pico.grant_privilege", "Parent", "write", "space", "_pico_property")

    # Assign role to user -> Ok.
    i1.call("pico.grant_privilege", "Dave", "execute", "role", "Parent")

    # Already assigned role to user -> error.
    with pytest.raises(
        ReturnError, match="User 'Dave' already has execute access on role 'Parent'"
    ):
        i1.call("pico.grant_privilege", "Dave", "execute", "role", "Parent")

    #
    # pico.revoke_privilege semantics verification
    #

    # Revoke privilege to user -> Ok.
    i1.call("pico.revoke_privilege", "Dave", "read", "space", "_pico_property")

    # Already revoked -> error.
    with pytest.raises(
        ReturnError,
        match="User 'Dave' does not have read access on space '_pico_property'",
    ):
        i1.call("pico.revoke_privilege", "Dave", "read", "space", "_pico_property")

    # Revoke privilege to role -> Ok.
    i1.call("pico.revoke_privilege", "Parent", "write", "space", "_pico_property")

    # Already revoked -> error.
    # FIXME: tarantool says User instead of Role.
    with pytest.raises(
        ReturnError,
        match="User 'Parent' does not have write access on space '_pico_property'",
    ):
        i1.call("pico.revoke_privilege", "Parent", "write", "space", "_pico_property")

    # Revoke role to user -> Ok.
    i1.call("pico.revoke_privilege", "Dave", "execute", "role", "Parent")

    # Already revoked role to user -> error.
    with pytest.raises(
        ReturnError, match="User 'Dave' does not have execute access on role 'Parent'"
    ):
        i1.call("pico.revoke_privilege", "Dave", "execute", "role", "Parent")

    #
    # pico.drop_user
    #

    # No user -> error.
    with pytest.raises(ReturnError, match="user should be a string"):
        i1.call("pico.drop_user")

    # No such user -> error.
    with pytest.raises(ReturnError, match="User 'User is not found' is not found"):
        i1.call("pico.drop_user", "User is not found")

    # Ok.
    i1.call("pico.drop_user", "Dave")

    # Repeat drop -> error.
    with pytest.raises(ReturnError, match="User 'Dave' is not found"):
        i1.call("pico.drop_user", "Dave")

    #
    # pico.drop_role
    #

    # No role -> error.
    with pytest.raises(ReturnError, match="role should be a string"):
        i1.call("pico.drop_role")

    # No such role -> error.
    with pytest.raises(ReturnError, match="Role 'Role is not found' is not found"):
        i1.call("pico.drop_role", "Role is not found")

    # Ok.
    i1.call("pico.drop_role", "Parent")

    # Repeat drop -> error.
    with pytest.raises(ReturnError, match="Role 'Parent' is not found"):
        i1.call("pico.drop_role", "Parent")

    #
    # Options validation
    #

    # Options is not table -> error.
    with pytest.raises(ReturnError, match="options should be a table"):
        i1.call("pico.create_user", "Dave", "pass", "timeout after 3 seconds please")

    # Unknown option -> error.
    with pytest.raises(ReturnError, match="unexpected option 'deadline'"):
        i1.call("pico.create_user", "Dave", "pass", dict(deadline="June 7th"))

    # Unknown option -> error.
    with pytest.raises(
        ReturnError, match="options parameter 'timeout' should be of type number"
    ):
        i1.call("pico.create_user", "Dave", "pass", dict(timeout="3s"))


def test_acl_basic(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)

    user = "Bobby"
    password = "s3cr3t"
    v = 0

    # Initial state.
    for i in cluster.instances:
        assert i.call("box.space._pico_property:get", "global_schema_version")[1] == v
        assert i.call("box.space._user.index.name:get", user) is None

    #
    #
    # Create user.
    index = i1.call("pico.create_user", user, password)
    cluster.raft_wait_index(index)
    v += 1

    # Schema was updated.
    for i in cluster.instances:
        assert i.call("box.space._pico_property:get", "global_schema_version")[1] == v
        assert i.call("box.space._user.index.name:get", user) is not None

    #
    #
    # Create a space to have something to grant privileges to.
    cluster.create_space(
        dict(
            name="money",
            format=[
                dict(name="owner", type="string", is_nullable=False),
                dict(name="amount", type="number", is_nullable=False),
            ],
            primary_key=["owner"],
            distribution="sharded",
            sharding_key=["owner"],
            sharding_fn="murmur3",
        )
    )
    v += 1

    # TODO: remove this
    user_id = i1.eval("return box.space._user.index.name:get(...).id", user)

    #
    #
    # Grant some privileges.
    # Doing anything via remote function execution requires execute access
    # to the "universe"
    index = i1.call("pico.grant_privilege", user, "execute", "universe")
    cluster.raft_wait_index(index)
    v += 1

    index = i1.call("pico.grant_privilege", user, "read", "space", "money")
    cluster.raft_wait_index(index)
    v += 1

    # Schema was updated once more.
    for i in cluster.instances:
        assert i.call("box.space._pico_property:get", "global_schema_version")[1] == v
        assert i.call("box.space._user:get", user_id) is not None
        assert (
            i.eval(
                """return box.execute([[
                select count(*) from "_priv" where "grantee" = ?
            ]], {...}).rows""",
                user_id,
            )
            != [[0]]
        )

    # Find replicasets masters.
    # TODO: use picodata's routing facilities.
    masters = [i for i in cluster.instances if not i.eval("return box.info.ro")]

    # Make sure we actually do call to somebody.
    assert len(masters) != 0

    # Try writing into space on behalf of the user.
    for i in masters:
        dummy_bucket_id = 69
        with pytest.raises(
            TarantoolError,
            match="Write access to space 'money' is denied for user 'Bobby'",
        ):
            i.call(
                "box.space.money:insert",
                [user, dummy_bucket_id, 1_000_000],
                user=user,
                password=password,
            )

    # Try reading from space on behalf of the user.
    for i in cluster.instances:
        assert i.call("box.space.money:select", user=user, password=password) == []

    #
    #
    # Revoke the privilege.
    index = i1.call("pico.revoke_privilege", user, "read", "space", "money")
    cluster.raft_wait_index(index)
    v += 1

    # Try reading from space on behalf of the user again.
    for i in cluster.instances:
        with pytest.raises(
            TarantoolError,
            match="Read access to space 'money' is denied for user 'Bobby'",
        ):
            assert i.call("box.space.money:select", user=user, password=password) == []

    #
    #
    # Change user's password.
    old_password = password
    new_password = "$3kr3T"
    index = i1.call("pico.change_password", user, new_password)
    cluster.raft_wait_index(index)
    v += 1

    # Old password doesn't work.
    for i in cluster.instances:
        with pytest.raises(
            NetworkError, match="User not found or supplied credentials are invalid"
        ):
            i.eval("return 1", user=user, password=old_password)

    # New password works.
    for i in cluster.instances:
        assert i.eval("return 1", user=user, password=new_password) == 1

    #
    #
    # Drop user.
    index = i1.call("pico.drop_user", user)

    for i in cluster.instances:
        i.raft_wait_index(index)

    # Schema was updated yet again.
    v += 1
    for i in cluster.instances:
        assert i.call("box.space._pico_property:get", "global_schema_version")[1] == v
        assert i.call("box.space._user:get", user_id) is None
        assert (
            i.eval(
                """return box.execute([[
                select count(*) from "_priv" where "grantee" = ?
            ]], {...}).rows""",
                user_id,
            )
            == [[0]]
        )

    # User was actually dropped.
    for i in cluster.instances:
        with pytest.raises(
            NetworkError, match="User not found or supplied credentials are invalid"
        ):
            i.eval("return 1", user=user, password=new_password)


def test_acl_roles_basic(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)

    user = "Steven"
    password = "1234"

    # Create user.
    index = i1.call("pico.create_user", user, password)
    cluster.raft_wait_index(index)

    # Doing anything via remote function execution requires execute access
    # to the "universe"
    index = i1.call("pico.grant_privilege", user, "execute", "universe")
    cluster.raft_wait_index(index)

    # Try reading from space on behalf of the user.
    for i in cluster.instances:
        with pytest.raises(
            TarantoolError,
            match="Read access to space '_pico_property' is denied for user 'Steven'",
        ):
            i.call("box.space._pico_property:select", user=user, password=password)

    #
    #
    # Create role.
    role = "PropertyReader"
    index = i1.call("pico.create_role", role)
    cluster.raft_wait_index(index)

    # Grant the role read access.
    index = i1.call("pico.grant_privilege", role, "read", "space", "_pico_property")
    cluster.raft_wait_index(index)

    # Assign role to user.
    index = i1.call("pico.grant_privilege", user, "execute", "role", role)
    cluster.raft_wait_index(index)

    # Try reading from space on behalf of the user again. Now succeed.
    for i in cluster.instances:
        rows = i.call("box.space._pico_property:select", user=user, password=password)
        assert len(rows) > 0

    # Revoke read access from the role.
    index = i1.call("pico.revoke_privilege", role, "read", "space", "_pico_property")
    cluster.raft_wait_index(index)

    # Try reading from space on behalf of the user yet again, which fails again.
    for i in cluster.instances:
        with pytest.raises(
            TarantoolError,
            match="Read access to space '_pico_property' is denied for user 'Steven'",
        ):
            i.call("box.space._pico_property:select", user=user, password=password)

    # Drop the role.
    index = i1.call("pico.drop_role", role)
    cluster.raft_wait_index(index)

    # Nothing changed here.
    for i in cluster.instances:
        with pytest.raises(
            TarantoolError,
            match="Read access to space '_pico_property' is denied for user 'Steven'",
        ):
            i.call("box.space._pico_property:select", user=user, password=password)


# TODO: test acl via snapshot
# TODO: test acl get denied when there's an unfinished ddl
