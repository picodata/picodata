import pytest
from conftest import MAX_LOGIN_ATTEMPTS, Cluster, Instance, TarantoolError
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
    acl = i2.sql('alter user "foo" with login', sudo=True)
    assert acl["row_count"] == 1

    # Now user can connect again
    c = connect(i1, user="foo", password="T0psecret")
    assert c


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
    i1.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{VALID_PASSWORD}'")
    index = i1.call(".proc_get_index")
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

    i1.grant_privilege(user, "read", "table", "money")
    index = i1.call(".proc_get_index")
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
    i1.revoke_privilege(user, "read", "table", "money")
    index = i1.call(".proc_get_index")
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
    i1.sql(f'DROP USER "{user}"')
    index = i1.call(".proc_get_index")

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
    i1.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{VALID_PASSWORD}'")
    index = i1.call(".proc_get_index")
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
    i1.sql(f'CREATE ROLE "{role}"')
    index = i1.call(".proc_get_index")
    cluster.raft_wait_index(index)

    # Grant the role read access.
    i1.grant_privilege(role, "read", "table", "_pico_property")
    index = i1.call(".proc_get_index")
    cluster.raft_wait_index(index)

    # Assign role to user.
    i1.grant_privilege(user, "execute", "role", role)
    index = i1.call(".proc_get_index")
    cluster.raft_wait_index(index)

    # Try reading from table on behalf of the user again. Now succeed.
    for i in cluster.instances:
        rows = i.call(
            "box.space._pico_property:select", user=user, password=VALID_PASSWORD
        )
        assert len(rows) > 0

    # Revoke read access from the role.
    i1.revoke_privilege(role, "read", "table", "_pico_property")
    index = i1.call(".proc_get_index")
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
    i1.sql(f'DROP ROLE "{role}"')
    index = i1.call(".proc_get_index")
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
    i1.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{VALID_PASSWORD}'")
    index = i1.call(".proc_get_index")
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

    i1.grant_privilege(user, "write", "table", "_pico_property")
    index = i1.call(".proc_get_index")
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
    i1.sql(f"CREATE USER \"Sam\" WITH PASSWORD '{VALID_PASSWORD}'")
    index = i1.call(".proc_get_index")
    cluster.raft_wait_index(index)

    i1.sql('CREATE ROLE "Captain"')
    index = i1.call(".proc_get_index")
    cluster.raft_wait_index(index)

    i1.grant_privilege("Sam", "read", "table", "_pico_property")
    index = i1.call(".proc_get_index")
    cluster.raft_wait_index(index)

    i1.grant_privilege("Captain", "read", "table", "_pico_table")
    index = i1.call(".proc_get_index")
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
    i1.sql('DROP USER "Sam"')
    index = i1.call(".proc_get_index")
    cluster.raft_wait_index(index)

    i1.sql(f"CREATE USER \"Blam\" WITH PASSWORD '{VALID_PASSWORD}'")
    index = i1.call(".proc_get_index")
    cluster.raft_wait_index(index)

    i1.revoke_privilege("Captain", "read", "table", "_pico_table")
    index = i1.call(".proc_get_index")
    cluster.raft_wait_index(index)

    i1.grant_privilege("Captain", "read", "table", "_pico_instance")
    index = i1.call(".proc_get_index")
    cluster.raft_wait_index(index)

    i1.sql('CREATE ROLE "Writer"')
    index = i1.call(".proc_get_index")
    cluster.raft_wait_index(index)

    i1.grant_privilege("Writer", "write", "table")
    index = i1.call(".proc_get_index")
    cluster.raft_wait_index(index)

    i1.grant_privilege("Blam", "execute", "role", "Writer")
    index = i1.call(".proc_get_index")
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
    i1.sql(f"CREATE USER \"Dave\" WITH PASSWORD '{VALID_PASSWORD}'")
    index = i1.call(".proc_get_index")

    dave_id = i1.sql(""" select "id" from "_pico_user" where "name" = 'Dave' """)[0][0]

    def dave_privileges_count():
        return i1.sql(
            f""" select count(*) from "_pico_privilege" where "grantee_id" = {dave_id} """,
        )[0][0]

    dave_privileges_count_at_start = dave_privileges_count()

    cluster.raft_wait_index(index)
    ddl = i1.sql(
        """
        create table t (a int not null, primary key (a)) distributed by (a)
        """
    )
    assert ddl["row_count"] == 1
    assert dave_privileges_count_at_start == dave_privileges_count()

    i1.grant_privilege("Dave", "read", "table", "T")
    index = i1.call(".proc_get_index")
    cluster.raft_wait_index(index)

    assert dave_privileges_count_at_start + 1 == dave_privileges_count()

    ddl = i1.sql(""" drop table t """)
    assert ddl["row_count"] == 1

    # Check that the picodata privilege on table t are gone.
    assert dave_privileges_count_at_start == dave_privileges_count()


def test_builtin_users_and_roles(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    # validate that builtin users and roles can be referenced in the GRANT command
    for user in ["admin", "guest", "public", "super"]:
        i1.grant_privilege(user, "read", "table", "_pico_property")

    i1.sql(f"CREATE USER \"Dave\" WITH PASSWORD '{VALID_PASSWORD}'")
    index = i1.call(".proc_get_index")
    i1.raft_wait_index(index)

    # granting already granted privilege does not raise an error
    index = i1.call("pico.raft_get_index")
    i1.sql('ALTER USER "DAVE" WITH LOGIN')
    new_index = i1.call(".proc_get_index")
    assert index == new_index

    i1.grant_privilege("Dave", "execute", "role", "super")
    new_index = i1.call(".proc_get_index")
    assert index != new_index


def test_create_table_smoke(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    i1.sql(f"CREATE USER \"Dave\" WITH PASSWORD '{VALID_PASSWORD}'")
    index = i1.call(".proc_get_index")
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

    i1.sql(f"CREATE USER \"Dave\" WITH PASSWORD '{VALID_PASSWORD}'")
    index = i1.call(".proc_get_index")

    # granting default privilege does not raise an error
    i1.grant_privilege("Dave", "execute", "role", "public")
    new_index = i1.call(".proc_get_index")
    assert index == new_index

    # granting default privilege does not raise an error
    i1.sql('ALTER USER "DAVE" WITH LOGIN')
    new_index = i1.call(".proc_get_index")
    assert index == new_index

    # granting default privilege does not raise an error
    i1.grant_privilege("Dave", "alter", "user", "Dave")
    new_index = i1.call(".proc_get_index")
    assert index == new_index

    # revoke default privilege does not raise an error
    i1.sql('ALTER USER "DAVE" WITH NOLOGIN')
    new_index = i1.call(".proc_get_index")
    assert index == new_index

    index = new_index
    # already revoked, so it should be idempotent
    i1.sql('ALTER USER "DAVE" WITH NOLOGIN')
    new_index = i1.call(".proc_get_index")
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
            TarantoolError,
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
        TarantoolError,
        match="altering guest user's password is not allowed",
    ):
        i1.sql("alter user \"guest\" with password 'Validpa55word'")

    with pytest.raises(
        TarantoolError,
        match="dropping system user is not allowed",
    ):
        i1.sql('drop user "guest"')

    with pytest.raises(
        TarantoolError,
        match="dropping system role is not allowed",
    ):
        i1.sql('drop role "public"')


def test_submit_sql_after_revoke_login(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    password = "Validpa55word"

    acl = i1.sql(f"create user \"alice\" with password '{password}'", sudo=True)
    assert acl["row_count"] == 1

    acl = i1.sql('grant create table to "alice"', sudo=True)
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
        acl = i1.sql('alter user "alice" with nologin', sudo=True)
        assert acl["row_count"] == 1

        # alice on the same connection should receive error that access is denied
        with pytest.raises(
            Exception,
            match="Execute access to function '.proc_sql_dispatch' is denied for user 'alice'",
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


def test_admin_set_password(cluster: Cluster):
    import os

    os.environ["PICODATA_ADMIN_PASSWORD"] = "#AdminX12345"

    i1 = cluster.add_instance(wait_online=False)
    i1.env.update(os.environ)
    i1.start()
    i1.wait_online()

    password = os.getenv("PICODATA_ADMIN_PASSWORD")
    with i1.connect(timeout=5, user="admin", password=password) as conn:
        query = conn.sql(
            'select * from "_pico_user"',
        )

        users = query["rows"]
        user_id = 1
        expected_user = "admin"
        expected_hash = ["chap-sha1", "oRcPNLNmy72trKXUATrE3RFCYtw="]
        for user in users:
            if user[0] == user_id:
                assert expected_user in user
                assert expected_hash in user

    i2 = cluster.add_instance(wait_online=False)
    i2.env.update(os.environ)
    i2.start()
    i2.wait_online()

    is_connected = False
    try:
        with i2.connect(timeout=5, user="admin", password="") as conn:
            conn.sql(
                'select * from "_pico_user"',
            )
        is_connected = True
    except Exception as err:
        print(f"Expected error occurred: {err}")

    assert not is_connected
