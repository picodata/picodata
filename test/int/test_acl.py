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
    i1.call("pico.create_user", "Dave", "", dict(timeout=3))

    # Already exists -> ok.
    i1.call("pico.create_user", "Dave", "", dict(timeout=3))

    # FIXME
    # Already exists but with different parameters -> should fail,
    # but doesn't currently.
    i1.call("pico.create_user", "Dave", "different password", dict(timeout=3))

    # Role already exists -> error.
    with pytest.raises(ReturnError, match="Role 'super' already exists"):
        i1.call("pico.create_user", "super", "", dict(timeout=3))

    #
    # pico.change_password
    #

    # Change password -> ok.
    i1.call("pico.change_password", "Dave", "no-one-will-know", dict(timeout=3))

    # Change password to the sameone -> ok.
    i1.call("pico.change_password", "Dave", "no-one-will-know", dict(timeout=3))

    # No such user -> error.
    with pytest.raises(ReturnError, match="User 'User is not found' is not found"):
        i1.call(
            "pico.change_password",
            "User is not found",
            "password",
            dict(timeout=3),
        )

    #
    # pico.create_role
    #

    # No role -> error.
    with pytest.raises(ReturnError, match="role should be a string"):
        i1.call("pico.create_role")

    # Ok.
    i1.call("pico.create_role", "Parent", dict(timeout=3))

    # Already exists -> ok.
    i1.call("pico.create_role", "Parent", dict(timeout=3))

    # User already exists -> error.
    with pytest.raises(ReturnError, match="User 'Dave' already exists"):
        i1.call("pico.create_role", "Dave", dict(timeout=3))

    #
    # pico.grant_privilege / pico.revoke_privilege parameter verification
    #

    for f in ["grant_privilege", "revoke_privilege"]:
        # No user -> error.
        with pytest.raises(ReturnError, match="grantee should be a string"):
            i1.call(f"pico.{f}")

        # No privilege -> error.
        with pytest.raises(ReturnError, match="privilege should be a string"):
            i1.call(f"pico.{f}", "Dave")

        # No such user -> error.
        with pytest.raises(ReturnError, match="User 'User is not found' is not found"):
            i1.call(
                f"pico.{f}",
                "User is not found",
                "execute",
                "universe",
                None,
                dict(timeout=3),
            )

        # No such privilege -> error.
        with pytest.raises(
            ReturnError,
            match=rf"unsupported privilege 'boogie', see pico.help\('{f}'\) for details",
        ):
            i1.call(f"pico.{f}", "Dave", "boogie", "universe", None, dict(timeout=3))

        # Comma separated list of privileges -> error.
        with pytest.raises(
            ReturnError,
            match=rf"unsupported privilege 'read,write', see pico.help\('{f}'\) for details",
        ):
            i1.call(
                f"pico.{f}", "Dave", "read,write", "universe", None, dict(timeout=3)
            )

        # No object_type -> error.
        with pytest.raises(ReturnError, match="object_type should be a string"):
            i1.call(f"pico.{f}", "Dave", "read")

        # No such object_type -> error.
        with pytest.raises(ReturnError, match="Unknown object type 'bible'"):
            i1.call(f"pico.{f}", "Dave", "read", "bible", None, dict(timeout=3))

        # Wrong combo -> error.
        with pytest.raises(ReturnError, match="Unsupported space privilege 'grant'"):
            i1.call(f"pico.{f}", "Dave", "grant", "space", None, dict(timeout=3))

        # No such role -> error.
        with pytest.raises(ReturnError, match="Role 'Joker' is not found"):
            i1.call(f"pico.{f}", "Dave", "execute", "role", "Joker", dict(timeout=3))

    #
    # pico.grant_privilege semantics verification
    #

    # Grant privilege to user -> Ok.
    i1.call(
        "pico.grant_privilege",
        "Dave",
        "read",
        "space",
        "_pico_property",
        dict(timeout=3),
    )

    # Already granted -> ok.
    i1.call(
        "pico.grant_privilege",
        "Dave",
        "read",
        "space",
        "_pico_property",
        dict(timeout=3),
    )

    # Grant privilege to role -> Ok.
    i1.call(
        "pico.grant_privilege",
        "Parent",
        "write",
        "space",
        "_pico_property",
        dict(timeout=3),
    )

    # Already granted -> ok.
    i1.call(
        "pico.grant_privilege",
        "Parent",
        "write",
        "space",
        "_pico_property",
        dict(timeout=3),
    )

    # Assign role to user -> Ok.
    i1.call(
        "pico.grant_privilege", "Dave", "execute", "role", "Parent", dict(timeout=3)
    )

    # Already assigned role to user -> error.
    i1.call(
        "pico.grant_privilege", "Dave", "execute", "role", "Parent", dict(timeout=3)
    )

    #
    # pico.revoke_privilege semantics verification
    #

    # Revoke privilege to user -> Ok.
    i1.call(
        "pico.revoke_privilege",
        "Dave",
        "read",
        "space",
        "_pico_property",
        dict(timeout=3),
    )

    # Already revoked -> ok.
    i1.call(
        "pico.revoke_privilege",
        "Dave",
        "read",
        "space",
        "_pico_property",
        dict(timeout=3),
    )

    # Revoke privilege to role -> Ok.
    i1.call(
        "pico.revoke_privilege",
        "Parent",
        "write",
        "space",
        "_pico_property",
        dict(timeout=3),
    )

    # Already revoked -> ok.
    i1.call(
        "pico.revoke_privilege",
        "Parent",
        "write",
        "space",
        "_pico_property",
        dict(timeout=3),
    )

    # Revoke role to user -> Ok.
    i1.call(
        "pico.revoke_privilege", "Dave", "execute", "role", "Parent", dict(timeout=3)
    )

    # Already revoked role to user -> ok.
    i1.call(
        "pico.revoke_privilege",
        "Dave",
        "execute",
        "role",
        "Parent",
        dict(timeout=3),
    )

    #
    # pico.drop_user
    #

    # No user -> error.
    with pytest.raises(ReturnError, match="user should be a string"):
        i1.call("pico.drop_user", dict(timeout=3))

    # No such user -> ok.
    i1.call("pico.drop_user", "User is not found", dict(timeout=3))

    # Ok.
    i1.call("pico.drop_user", "Dave", dict(timeout=3))

    # Repeat drop -> ok.
    i1.call("pico.drop_user", "Dave", dict(timeout=3))

    #
    # pico.drop_role
    #

    # No role -> error.
    with pytest.raises(ReturnError, match="role should be a string"):
        i1.call("pico.drop_role")

    # No such role -> ok.
    i1.call("pico.drop_role", "Role is not found", dict(timeout=3))

    # Ok.
    i1.call("pico.drop_role", "Parent", dict(timeout=3))

    # Repeat drop -> ok.
    i1.call("pico.drop_role", "Parent", dict(timeout=3))

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

    # No timeout -> error.
    with pytest.raises(ReturnError, match="opts.timeout is mandatory"):
        i1.call("pico.create_user", "Dave", "pass")


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
    index = i1.call("pico.create_user", user, password, dict(timeout=3))
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
    index = i1.call(
        "pico.grant_privilege", user, "execute", "universe", None, dict(timeout=3)
    )
    cluster.raft_wait_index(index)
    v += 1

    index = i1.call(
        "pico.grant_privilege", user, "read", "space", "money", dict(timeout=3)
    )
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
    index = i1.call(
        "pico.revoke_privilege", user, "read", "space", "money", dict(timeout=3)
    )
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
    index = i1.call("pico.change_password", user, new_password, dict(timeout=3))
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
    index = i1.call("pico.drop_user", user, dict(timeout=3))

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
    index = i1.call("pico.create_user", user, password, dict(timeout=3))
    cluster.raft_wait_index(index)

    # Doing anything via remote function execution requires execute access
    # to the "universe"
    index = i1.call(
        "pico.grant_privilege", user, "execute", "universe", None, dict(timeout=3)
    )
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
    index = i1.call("pico.create_role", role, dict(timeout=3))
    cluster.raft_wait_index(index)

    # Grant the role read access.
    index = i1.call(
        "pico.grant_privilege", role, "read", "space", "_pico_property", dict(timeout=3)
    )
    cluster.raft_wait_index(index)

    # Assign role to user.
    index = i1.call(
        "pico.grant_privilege", user, "execute", "role", role, dict(timeout=3)
    )
    cluster.raft_wait_index(index)

    # Try reading from space on behalf of the user again. Now succeed.
    for i in cluster.instances:
        rows = i.call("box.space._pico_property:select", user=user, password=password)
        assert len(rows) > 0

    # Revoke read access from the role.
    index = i1.call(
        "pico.revoke_privilege",
        role,
        "read",
        "space",
        "_pico_property",
        dict(timeout=3),
    )
    cluster.raft_wait_index(index)

    # Try reading from space on behalf of the user yet again, which fails again.
    for i in cluster.instances:
        with pytest.raises(
            TarantoolError,
            match="Read access to space '_pico_property' is denied for user 'Steven'",
        ):
            i.call("box.space._pico_property:select", user=user, password=password)

    # Drop the role.
    index = i1.call("pico.drop_role", role, dict(timeout=3))
    cluster.raft_wait_index(index)

    # Nothing changed here.
    for i in cluster.instances:
        with pytest.raises(
            TarantoolError,
            match="Read access to space '_pico_property' is denied for user 'Steven'",
        ):
            i.call("box.space._pico_property:select", user=user, password=password)


def to_set_of_tuples(list_of_lists):
    return set((tuple(list) for list in list_of_lists))


def test_acl_from_snapshot(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True, replicaset_id="R1")
    i2 = cluster.add_instance(wait_online=True, replicaset_id="R1")
    i3 = cluster.add_instance(wait_online=True, replicaset_id="R1")
    i4 = cluster.add_instance(wait_online=True, replicaset_id="R2")
    i5 = cluster.add_instance(wait_online=True, replicaset_id="R2")

    #
    # Initial state.
    #
    index = i1.call("pico.create_user", "Sam", "pass", dict(timeout=3))
    cluster.raft_wait_index(index)

    index = i1.call("pico.create_role", "Captain", dict(timeout=3))
    cluster.raft_wait_index(index)

    index = i1.call(
        "pico.grant_privilege",
        "Sam",
        "read",
        "space",
        "_pico_property",
        dict(timeout=3),
    )
    cluster.raft_wait_index(index)

    index = i1.call(
        "pico.grant_privilege",
        "Captain",
        "read",
        "space",
        "_pico_space",
        dict(timeout=3),
    )
    cluster.raft_wait_index(index)

    #
    # Before:
    #
    for i in [i4, i5]:
        assert to_set_of_tuples(i.call("box.schema.user.info", "Sam")) == {
            ("execute", "role", "public"),
            ("read", "space", "_pico_property"),
            ("session,usage", "universe", ""),
            ("alter", "user", "Sam"),
        }

        with pytest.raises(TarantoolError, match="User 'Blam' is not found"):
            i.call("box.schema.user.info", "Blam")

        assert to_set_of_tuples(i.call("box.schema.role.info", "Captain")) == {
            ("read", "space", "_pico_space"),
        }

        with pytest.raises(TarantoolError, match="Role 'Executor' is not found"):
            i.call("box.schema.role.info", "Executor")

    #
    # These will be catching up by snapshot.
    #
    i5.terminate()
    i4.terminate()

    #
    # These changes will arive by snapshot.
    #
    index = i1.call("pico.drop_user", "Sam", dict(timeout=3))
    cluster.raft_wait_index(index)

    index = i1.call("pico.create_user", "Blam", "pass", dict(timeout=3))
    cluster.raft_wait_index(index)

    index = i1.call(
        "pico.revoke_privilege",
        "Captain",
        "read",
        "space",
        "_pico_space",
        dict(timeout=3),
    )
    cluster.raft_wait_index(index)

    index = i1.call(
        "pico.grant_privilege",
        "Captain",
        "read",
        "space",
        "_pico_instance",
        dict(timeout=3),
    )
    cluster.raft_wait_index(index)

    index = i1.call("pico.create_role", "Executor", dict(timeout=3))
    cluster.raft_wait_index(index)

    index = i1.call(
        "pico.grant_privilege", "Executor", "execute", "universe", None, dict(timeout=3)
    )
    cluster.raft_wait_index(index)

    index = i1.call(
        "pico.grant_privilege", "Blam", "execute", "role", "Executor", dict(timeout=3)
    )
    cluster.raft_wait_index(index)

    # Compact log to trigger snapshot generation.
    i1.raft_compact_log()
    i2.raft_compact_log()
    i3.raft_compact_log()

    #
    # Cacthup by snapshot.
    #
    i4.start()
    i5.start()
    i4.wait_online()
    i5.wait_online()

    #
    # After:
    #
    for i in [i4, i5]:
        with pytest.raises(TarantoolError, match="User 'Sam' is not found"):
            i.call("box.schema.user.info", "Sam")

        assert to_set_of_tuples(i.call("box.schema.user.info", "Blam")) == {
            ("execute", "role", "public"),
            ("execute", "role", "Executor"),
            ("session,usage", "universe", ""),
            ("alter", "user", "Blam"),
        }

        assert to_set_of_tuples(i.call("box.schema.role.info", "Captain")) == {
            ("read", "space", "_pico_instance"),
        }

        assert to_set_of_tuples(i.call("box.schema.role.info", "Executor")) == {
            ("execute", "universe", ""),
        }


# TODO: test acl get denied when there's an unfinished ddl
# TODO: check various retryable cas outcomes when doing schema change requests
