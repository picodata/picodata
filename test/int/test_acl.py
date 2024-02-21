import pytest
from conftest import MAX_LOGIN_ATTEMPTS, Cluster, Instance, TarantoolError, ReturnError
from tarantool.error import NetworkError  # type: ignore
from tarantool.connection import Connection  # type: ignore

VALID_PASSWORD = "L0ng enough"
PASSWORD_MIN_LENGTH_KEY = "password_min_length"


def expected_min_password_violation_error(min_length: int):
    return f"password is too short: expected at least {min_length}, got"


def set_min_password_len(cluster: Cluster, i1: Instance, min_password_len: int):
    read_index = cluster.instances[0].raft_read_index()

    # Successful insert
    ret = cluster.cas(
        "replace",
        "_pico_property",
        [PASSWORD_MIN_LENGTH_KEY, min_password_len],
        index=read_index,
    )
    assert ret == read_index + 1
    cluster.raft_wait_index(ret)
    check = i1.call("box.space._pico_property:get", PASSWORD_MIN_LENGTH_KEY)
    assert check[1] == min_password_len


def test_max_login_attempts(cluster: Cluster):
    i1, i2, _ = cluster.deploy(instance_count=3)

    i1.sql(
        """ CREATE USER "foo" WITH PASSWORD 'T0psecret' USING chap-sha1 OPTION (timeout = 3) """
    )

    def connect(
        i: Instance, user: str | None = None, password: str | None = None
    ) -> Connection:
        return Connection(
            i.host,
            i.port,
            user=user,
            password=password,
            connect_now=True,
            reconnect_max_attempts=0,
        )

    # First login is successful
    c = connect(i1, user="foo", password="T0psecret")
    assert c

    # Several failed login attempts but one less than maximum
    for _ in range(MAX_LOGIN_ATTEMPTS - 1):
        with pytest.raises(
            NetworkError, match="User not found or supplied credentials are invalid"
        ):
            # incorrect password
            connect(i1, user="foo", password="baz")

    # Still possible to login. Resets the attempts counter
    c = connect(
        i1,
        user="foo",
        password="T0psecret",
    )
    assert c

    # Maximum failed login attempts
    for _ in range(MAX_LOGIN_ATTEMPTS):
        with pytest.raises(
            NetworkError, match="User not found or supplied credentials are invalid"
        ):
            # incorrect password
            connect(i1, user="foo", password="baz")

    # Next login even with correct password fails as the limit is reached
    with pytest.raises(NetworkError, match="Maximum number of login attempts exceeded"):
        connect(i1, user="foo", password="T0psecret")

    # Unlock user - alter user login is interpreted as grant session
    # which resets the login attempts counter.
    # Works from any peer
    acl = i2.sudo_sql('alter user "foo" with login')
    assert acl["row_count"] == 1

    # Now user can connect again
    c = connect(i1, user="foo", password="T0psecret")
    assert c


def test_acl_lua_api(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    # No user -> error.
    with pytest.raises(ReturnError, match="user should be a string"):
        i1.call("pico.create_user")

    # No password -> error.
    with pytest.raises(ReturnError, match="password should be a string"):
        i1.call("pico.create_user", "Dave")

    initial_password_min_length = i1.call(
        "box.space._pico_property:get", PASSWORD_MIN_LENGTH_KEY
    )[1]
    expected = expected_min_password_violation_error(initial_password_min_length)

    with pytest.raises(ReturnError, match=expected):
        i1.call("pico.create_user", "Dave", "")

    i1.call("pico.create_user", "Dave", VALID_PASSWORD)

    # Already exists -> ok.
    i1.call("pico.create_user", "Dave", VALID_PASSWORD)

    # FIXME
    # Already exists but with different parameters -> should fail,
    # but doesn't currently.
    i1.call("pico.create_user", "Dave", "different password")

    # Role already exists -> error.
    with pytest.raises(ReturnError, match="Role 'super' already exists"):
        i1.call("pico.create_user", "super", VALID_PASSWORD)

    # Test the behavior when password_min_length is missing form properties
    # possibly because instance was upgraded from one that didnt have the value
    # populated at bootstrap time.
    i1.call("box.space._pico_property:delete", PASSWORD_MIN_LENGTH_KEY)

    i1.call("pico.create_user", "Dave", "short")

    # restore
    i1.eval(
        f'box.space._pico_property:insert{{"{PASSWORD_MIN_LENGTH_KEY}", '
        f"{initial_password_min_length}}}"
    )

    # change the value using cas
    set_min_password_len(cluster, i1, 20)

    with pytest.raises(ReturnError, match=expected_min_password_violation_error(20)):
        i1.call("pico.create_user", "Dave", VALID_PASSWORD)

    set_min_password_len(cluster, i1, initial_password_min_length)

    #
    # pico.change_password
    #

    # Change password to invalid one -> ok.
    with pytest.raises(ReturnError, match=expected):
        i1.call("pico.change_password", "Dave", "secret")

    # Change password -> ok.
    i1.call("pico.change_password", "Dave", "no-one-will-know")

    # Change password to the sameone -> ok.
    i1.call("pico.change_password", "Dave", "no-one-will-know")

    # No such user -> error.
    with pytest.raises(ReturnError, match="User 'User is not found' is not found"):
        i1.call(
            "pico.change_password",
            "User is not found",
            "password",
        )

    #
    # pico.create_role
    #

    # No role -> error.
    with pytest.raises(ReturnError, match="role should be a string"):
        i1.call("pico.create_role")

    # Ok.
    i1.call("pico.create_role", "Parent")

    # Already exists -> ok.
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

        # No privilege -> error.
        with pytest.raises(ReturnError, match="privilege should be a string"):
            i1.call(f"pico.{f}", "Dave")

        # No such user -> error.
        with pytest.raises(ReturnError, match="User 'User is not found' is not found"):
            i1.call(
                f"pico.{f}",
                "User is not found",
                "read",
                "table",
                "_pico_property",
            )

        # No such privilege -> error.
        with pytest.raises(
            ReturnError,
            match=rf"unsupported privilege 'boogie', see pico.help\('{f}'\) for details",
        ):
            i1.call(f"pico.{f}", "Dave", "boogie", "universe", None)

        # Comma separated list of privileges -> error.
        with pytest.raises(
            ReturnError,
            match=rf"unsupported privilege 'read,write', see pico.help\('{f}'\) for details",
        ):
            i1.call(f"pico.{f}", "Dave", "read,write", "role", None)

        # No object_type -> error.
        with pytest.raises(ReturnError, match="object_type should be a string"):
            i1.call(f"pico.{f}", "Dave", "read")

        # No such object_type -> error.
        with pytest.raises(ReturnError, match="Unknown object type 'bible'"):
            i1.call(f"pico.{f}", "Dave", "read", "bible", None)

        # Wrong combo -> error.
        with pytest.raises(ReturnError, match="Unsupported table privilege 'grant'"):
            i1.call(f"pico.{f}", "Dave", "grant", "table", None)

        # No such role -> error.
        with pytest.raises(ReturnError, match="Role 'Joker' is not found"):
            i1.call(f"pico.{f}", "Dave", "execute", "role", "Joker")

    #
    # pico.grant_privilege semantics verification
    #

    # Grant privilege to user -> Ok.
    i1.grant_privilege(
        "Dave",
        "read",
        "table",
        "_pico_property",
    )

    dave_id = i1.call("box.space._pico_user.index.name:get", "Dave")[0]

    pico_property_id = i1.eval("return box.space._pico_property.id")
    priv = i1.call(
        "box.space._pico_privilege:get",
        (dave_id, "table", pico_property_id, "read"),
    )

    # grantor_id is the field at index 4 in schema::PrivilegeDef
    # The above grant was executed from admin. 1 is admin user id.
    assert priv[4] == 1

    # Grant privilege to user without object_name -> Ok.
    i1.grant_privilege(
        "Dave",
        "create",
        "user",
    )

    priv = i1.call(
        "box.space._pico_privilege:get",
        (dave_id, "user", -1, "create"),
    )

    # grantor_id is the field at index 4 in schema::PrivilegeDef
    # The above grant was executed from admin. 1 is admin user id.
    assert priv[4] == 1

    # Already granted -> ok.
    i1.grant_privilege(
        "Dave",
        "read",
        "table",
        "_pico_property",
    )

    # Grant privilege to role -> Ok.
    i1.grant_privilege(
        "Parent",
        "write",
        "table",
        "_pico_property",
    )

    # Already granted -> ok.
    i1.grant_privilege(
        "Parent",
        "write",
        "table",
        "_pico_property",
    )

    # Assign role to user -> Ok.
    i1.grant_privilege("Dave", "execute", "role", "Parent")

    # Already assigned role to user -> error.
    i1.grant_privilege("Dave", "execute", "role", "Parent")

    #
    # pico.revoke_privilege semantics verification
    #

    # Revoke privilege from user -> Ok.
    i1.revoke_privilege(
        "Dave",
        "read",
        "table",
        "_pico_property",
    )

    # Revoke privilege from user without object_name -> Ok.
    i1.revoke_privilege(
        "Dave",
        "create",
        "user",
    )

    # Already revoked -> ok.
    i1.revoke_privilege(
        "Dave",
        "read",
        "table",
        "_pico_property",
    )

    # Revoke privilege from role -> Ok.
    i1.revoke_privilege(
        "Parent",
        "write",
        "table",
        "_pico_property",
    )

    # Already revoked -> ok.
    i1.revoke_privilege(
        "Parent",
        "write",
        "table",
        "_pico_property",
    )

    # Revoke role from user -> Ok.
    i1.revoke_privilege("Dave", "execute", "role", "Parent")

    # Already revoked role to user -> ok.
    i1.revoke_privilege(
        "Dave",
        "execute",
        "role",
        "Parent",
    )

    #
    # pico.drop_user
    #

    # No user -> error.
    with pytest.raises(ReturnError, match="user should be a string"):
        i1.call("pico.drop_user")

    # No such user -> ok.
    i1.call("pico.drop_user", "User is not found")

    # Ok.
    i1.call("pico.drop_user", "Dave")

    # Repeat drop -> ok.
    i1.call("pico.drop_user", "Dave")

    #
    # pico.drop_role
    #

    # No role -> error.
    with pytest.raises(ReturnError, match="role should be a string"):
        i1.call("pico.drop_role")

    # No such role -> ok.
    i1.call("pico.drop_role", "Role is not found")

    # Ok.
    i1.call("pico.drop_role", "Parent")

    # Repeat drop -> ok.
    i1.call("pico.drop_role", "Parent")

    #
    # Options validation
    #

    # Options is not table -> error.
    with pytest.raises(ReturnError, match="options should be a table"):
        i1.call(
            "pico.create_user", "Dave", VALID_PASSWORD, "timeout after 3 seconds please"
        )

    # Unknown option -> error.
    with pytest.raises(ReturnError, match="unexpected option 'deadline'"):
        i1.call("pico.create_user", "Dave", VALID_PASSWORD, dict(deadline="June 7th"))

    # Unknown option -> error.
    with pytest.raises(
        ReturnError, match="options parameter 'timeout' should be of type number"
    ):
        i1.call("pico.create_user", "Dave", VALID_PASSWORD, dict(timeout="3s"))


def test_acl_basic(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)

    user = "Bobby"
    v = 0

    # Initial state.
    for i in cluster.instances:
        assert i.call("box.space._pico_property:get", "global_schema_version")[1] == v
        assert i.call("box.space._user.index.name:get", user) is None

    #
    #
    # Create user.
    index = i1.call("pico.create_user", user, VALID_PASSWORD)
    cluster.raft_wait_index(index)
    v += 1

    # Schema was updated.
    for i in cluster.instances:
        assert i.call("box.space._pico_property:get", "global_schema_version")[1] == v
        assert i.call("box.space._user.index.name:get", user) is not None

    #
    #
    # Create a table to have something to grant privileges to.
    cluster.create_table(
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

    # Grant some privileges.
    # Doing anything via remote function execution requires execute access
    # to the "universe"
    cluster.grant_box_privilege(user, "execute", "universe")

    index = i1.grant_privilege(user, "read", "table", "money")
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

    # Try writing into table on behalf of the user.
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
                password=VALID_PASSWORD,
            )

    # Try reading from table on behalf of the user.
    for i in cluster.instances:
        assert (
            i.call("box.space.money:select", user=user, password=VALID_PASSWORD) == []
        )

    #
    #
    # Revoke the privilege.
    index = i1.revoke_privilege(user, "read", "table", "money")
    cluster.raft_wait_index(index)
    v += 1

    # Try reading from table on behalf of the user again.
    for i in cluster.instances:
        with pytest.raises(
            TarantoolError,
            match="Read access to space 'money' is denied for user 'Bobby'",
        ):
            assert (
                i.call("box.space.money:select", user=user, password=VALID_PASSWORD)
                == []
            )

    #
    #
    # Change user's password.
    old_password = VALID_PASSWORD
    new_password = "L0ng$3kr3T"
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

    # Create user.
    index = i1.call("pico.create_user", user, VALID_PASSWORD)
    cluster.raft_wait_index(index)

    # Doing anything via remote function execution requires execute access
    # to the "universe"
    cluster.grant_box_privilege(user, "execute", "universe", None)

    # Try reading from table on behalf of the user.
    for i in cluster.instances:
        with pytest.raises(
            TarantoolError,
            match="Read access to space '_pico_property' is denied for user 'Steven'",
        ):
            i.call(
                "box.space._pico_property:select", user=user, password=VALID_PASSWORD
            )

    #
    #
    # Create role.
    role = "PropertyReader"
    index = i1.call("pico.create_role", role)
    cluster.raft_wait_index(index)

    # Grant the role read access.
    index = i1.grant_privilege(role, "read", "table", "_pico_property")
    cluster.raft_wait_index(index)

    # Assign role to user.
    index = i1.grant_privilege(user, "execute", "role", role)
    cluster.raft_wait_index(index)

    # Try reading from table on behalf of the user again. Now succeed.
    for i in cluster.instances:
        rows = i.call(
            "box.space._pico_property:select", user=user, password=VALID_PASSWORD
        )
        assert len(rows) > 0

    # Revoke read access from the role.
    index = i1.revoke_privilege(
        role,
        "read",
        "table",
        "_pico_property",
    )
    cluster.raft_wait_index(index)

    # Try reading from table on behalf of the user yet again, which fails again.
    for i in cluster.instances:
        with pytest.raises(
            TarantoolError,
            match="Read access to space '_pico_property' is denied for user 'Steven'",
        ):
            i.call(
                "box.space._pico_property:select", user=user, password=VALID_PASSWORD
            )

    # Drop the role.
    index = i1.call("pico.drop_role", role)
    cluster.raft_wait_index(index)

    # Nothing changed here.
    for i in cluster.instances:
        with pytest.raises(
            TarantoolError,
            match="Read access to space '_pico_property' is denied for user 'Steven'",
        ):
            i.call(
                "box.space._pico_property:select", user=user, password=VALID_PASSWORD
            )


def test_cas_permissions(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)

    user = "Steven"

    # Create user.
    index = i1.call("pico.create_user", user, VALID_PASSWORD)
    cluster.raft_wait_index(index)

    with pytest.raises(
        Exception,
        match="Write access to space '_pico_property' is denied for user 'Steven'",
    ):
        cluster.cas(
            "replace",
            "_pico_property",
            ["foo", 128],
            user=user,
            password=VALID_PASSWORD,
        )

    index = i1.grant_privilege(
        user,
        "write",
        "table",
        "_pico_property",
    )
    cluster.raft_wait_index(index)

    index = cluster.cas(
        "replace",
        "_pico_property",
        ["foo", 128],
        user=user,
        password=VALID_PASSWORD,
    )

    cluster.raft_wait_index(index)

    assert i1.pico_property("foo") == 128


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
    index = i1.call("pico.create_user", "Sam", VALID_PASSWORD)
    cluster.raft_wait_index(index)

    index = i1.call("pico.create_role", "Captain")
    cluster.raft_wait_index(index)

    index = i1.grant_privilege(
        "Sam",
        "read",
        "table",
        "_pico_property",
    )
    cluster.raft_wait_index(index)

    index = i1.grant_privilege(
        "Captain",
        "read",
        "table",
        "_pico_table",
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
            ("read", "space", "_pico_table"),
        }

        with pytest.raises(TarantoolError, match="Role 'Writer' is not found"):
            i.call("box.schema.role.info", "Writer")

    #
    # These will be catching up by snapshot.
    #
    i5.terminate()
    i4.terminate()

    #
    # These changes will arive by snapshot.
    #
    index = i1.call("pico.drop_user", "Sam")
    cluster.raft_wait_index(index)

    index = i1.call("pico.create_user", "Blam", VALID_PASSWORD)
    cluster.raft_wait_index(index)

    index = i1.revoke_privilege(
        "Captain",
        "read",
        "table",
        "_pico_table",
    )
    cluster.raft_wait_index(index)

    index = i1.grant_privilege(
        "Captain",
        "read",
        "table",
        "_pico_instance",
    )
    cluster.raft_wait_index(index)

    index = i1.call("pico.create_role", "Writer")
    cluster.raft_wait_index(index)

    index = i1.grant_privilege("Writer", "write", "table", None)
    cluster.raft_wait_index(index)

    index = i1.grant_privilege("Blam", "execute", "role", "Writer")
    cluster.raft_wait_index(index)

    # Compact log to trigger snapshot generation.
    i1.raft_compact_log()
    i2.raft_compact_log()
    i3.raft_compact_log()

    #
    # Catchup by snapshot.
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
            ("execute", "role", "Writer"),
            ("session,usage", "universe", ""),
            ("alter", "user", "Blam"),
        }

        assert to_set_of_tuples(i.call("box.schema.role.info", "Captain")) == {
            ("read", "space", "_pico_instance"),
        }

        assert to_set_of_tuples(i.call("box.schema.role.info", "Writer")) == {
            ("write", "space", ""),
        }


def test_acl_drop_table_with_privileges(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    # Check that we can drop a table with privileges granted on it.
    index = i1.call("pico.create_user", "Dave", VALID_PASSWORD)

    dave_id = i1.sql(""" select "id" from "_pico_user" where "name" = 'Dave' """)[
        "rows"
    ][0][0]

    def dave_privileges_count():
        return i1.sql(
            f""" select count(*) from "_pico_privilege" where "grantee_id" = {dave_id} """,
        )["rows"][0][0]

    dave_privileges_count_at_start = dave_privileges_count()

    cluster.raft_wait_index(index)
    ddl = i1.sql(
        """
        create table t (a int not null, primary key (a)) distributed by (a)
        """
    )
    assert ddl["row_count"] == 1
    assert dave_privileges_count_at_start == dave_privileges_count()

    index = i1.grant_privilege("Dave", "read", "table", "T")
    cluster.raft_wait_index(index)

    assert dave_privileges_count_at_start + 1 == dave_privileges_count()

    ddl = i1.sql(""" drop table t """)
    assert ddl["row_count"] == 1

    # Check that the picodata privilege on table t are gone.
    assert dave_privileges_count_at_start == dave_privileges_count()


def test_builtin_users_and_roles(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    # validate that builtin users and roles can be referenced in pico.{grant,revoke}_privilege
    for user in ["admin", "guest", "public", "super"]:
        i1.grant_privilege(
            user,
            "read",
            "table",
            "_pico_property",
        )

    index = i1.call("pico.create_user", "Dave", VALID_PASSWORD)
    i1.raft_wait_index(index)

    # granting already granted privilege does not raise an error
    index = i1.call("pico.raft_get_index")
    new_index = i1.grant_privilege(
        "Dave",
        "login",
        "universe",
    )
    assert index == new_index

    new_index = i1.grant_privilege(
        "Dave",
        "execute",
        "role",
        "super",
    )


def test_create_table_smoke(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    index = i1.call("pico.create_user", "Dave", VALID_PASSWORD)
    cluster.raft_wait_index(index)
    with pytest.raises(
        Exception,
        match="Create access to space 'T' is denied for user 'Dave'",
    ):
        i1.sql(
            """
            create table t (a int not null, primary key (a)) distributed by (a)
            """,
            user="Dave",
            password=VALID_PASSWORD,
        )

    i1.grant_privilege("Dave", "create", "table")

    with i1.connect(timeout=1, user="Dave", password=VALID_PASSWORD) as conn:
        ddl = conn.sql(
            """
            create table t (a int not null, primary key (a)) distributed by (a)
            """,
        )
        assert ddl["row_count"] == 1

        # Dave is the owner thus has all privileges on t
        ret = conn.sql("insert into t values(1);")
        assert ret["row_count"] == 1

        ret = conn.sql("select * from t")

        assert ret == {"metadata": [{"name": "A", "type": "integer"}], "rows": [[1]]}

        ddl = conn.sql("drop table t")
        assert ddl["row_count"] == 1


def test_grant_and_revoke_default_users_privileges(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    index = i1.call("pico.create_user", "Dave", VALID_PASSWORD)

    # granting default privilege does not raise an error
    new_index = i1.grant_privilege(
        "Dave",
        "execute",
        "role",
        "public",
    )
    assert index == new_index

    # granting default privilege does not raise an error
    new_index = i1.grant_privilege(
        "Dave",
        "login",
        "universe",
    )
    assert index == new_index

    # granting default privilege does not raise an error
    new_index = i1.grant_privilege(
        "Dave",
        "alter",
        "user",
        "Dave",
    )
    assert index == new_index

    # revoke default privilege, so raft_index should change
    new_index = i1.revoke_privilege("Dave", "login", "universe")
    assert new_index != index

    index = new_index
    # already revoked, so it should be idempotent
    new_index = i1.revoke_privilege("Dave", "login", "universe")
    assert new_index == index


# it's part of https://git.picodata.io/picodata/picodata/picodata/-/issues/421
# https://git.picodata.io/picodata/tarantool/-/blob/82123356764155d1f4b294c0f2b96d919c2a8ec7/test/box/role.test.lua#L34
def test_circular_grants_for_role(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    i1.sql("CREATE ROLE Grandfather")

    i1.sql("CREATE ROLE Father")

    i1.sql("CREATE ROLE Son")

    i1.sql("CREATE ROLE Daughter")

    i1.sql("GRANT Father to Grandfather")

    i1.sql("GRANT Son to Father")

    i1.sql("GRANT Daughter to Father")

    # Graph of grants isn't tree
    i1.sql("GRANT Son to Daughter")

    # At this point graph of grants looks like:
    #               Grandfather
    #              /
    #             /
    #            /--->   Father
    #                /           \
    #               /             \
    #              /               \
    #             /                 \
    #            /                   \
    #           /                     \
    #          /->  Son <-- Daugther <-\

    def throws_on_grant(grantee, granted):
        with pytest.raises(
            ReturnError,
            match=f"Granting role {granted.upper()} to role {grantee.upper()} would create a loop",
        ):
            i1.sql(f"GRANT {granted} to {grantee}")

    # Following grants creating a loop
    throws_on_grant("Son", "Grandfather")
    throws_on_grant("Son", "Father")
    throws_on_grant("Son", "Daughter")
    throws_on_grant("Daughter", "Grandfather")
    throws_on_grant("Daughter", "Father")
    throws_on_grant("Father", "Grandfather")

    # Giving user nested roles is allowed
    i1.sql(f"CREATE USER Dave WITH PASSWORD '{VALID_PASSWORD}'")

    i1.sql("GRANT Grandfather to Dave")

    i1.sql("GRANT Father to Dave")

    i1.sql("GRANT Son to Dave")

    # Granting role to itself isn't allowed
    throws_on_grant("Son", "Son")


def test_alter_system_user(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    with pytest.raises(
        ReturnError,
        match="altering guest user's password is not allowed",
    ):
        i1.sql("alter user \"guest\" with password 'Validpa55word'")

    with pytest.raises(
        ReturnError,
        match="dropping system user is not allowed",
    ):
        i1.sql('drop user "guest"')

    with pytest.raises(
        ReturnError,
        match="dropping system role is not allowed",
    ):
        i1.sql('drop role "public"')


def test_submit_sql_after_revoke_login(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    password = "Validpa55word"

    acl = i1.sudo_sql(f"create user \"alice\" with password '{password}'")
    assert acl["row_count"] == 1

    acl = i1.sudo_sql('grant create table to "alice"')
    assert acl["row_count"] == 1

    with i1.connect(timeout=2, user="alice", password=password) as conn:
        ddl = conn.sql(
            """
            create table t (a int not null, primary key (a)) distributed by (a)
            """,
        )
        assert ddl["row_count"] == 1

        # alice is the owner thus has all privileges on t
        dml = conn.sql("insert into t values(1);")
        assert dml["row_count"] == 1

        # admin revokes login privilege
        acl = i1.sudo_sql('alter user "alice" with nologin')
        assert acl["row_count"] == 1

        # alice on the same connection should receive error that access is denied
        with pytest.raises(
            Exception,
            match="Execute access to function 'pico.sql' is denied for user 'alice'",
        ):
            conn.sql("insert into t values(2);")

    # alice on a new connection should not ba able to login
    with pytest.raises(
        Exception,
        match="User does not have login privilege",
    ):
        i1.sql("insert into t values(2);", user="alice", password=password)


# TODO: test acl get denied when there's an unfinished ddl
# TODO: check various retryable cas outcomes when doing schema change requests
