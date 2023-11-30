import pytest
import re

from conftest import (
    Cluster,
    KeyDef,
    KeyPart,
    ReturnError,
)


def test_pico_sql(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    usage_msg = re.escape("Usage: sql(query[, params, traceable])")
    with pytest.raises(ReturnError, match=usage_msg):
        i1.call(
            "pico.sql",
        )
    with pytest.raises(ReturnError, match=usage_msg):
        i1.call(
            "pico.sql",
            "select * from t",
            {},
            False,
            "extra",
        )

    first_arg_msg = re.escape("SQL query must be a string")
    with pytest.raises(ReturnError, match=first_arg_msg):
        i1.call(
            "pico.sql",
            1,
        )

    second_arg_msg = re.escape("SQL params must be a table")
    with pytest.raises(ReturnError, match=second_arg_msg):
        i1.call(
            "pico.sql",
            "select * from t",
            1,
        )

    third_arg_msg = "SQL trace flag must be a boolean"
    with pytest.raises(ReturnError, match=third_arg_msg):
        i1.call("pico.sql", "select * from t", {}, "tracer")

    invalid_meta_msg = re.escape("sbroad: space")
    with pytest.raises(ReturnError, match=invalid_meta_msg):
        i1.call(
            "pico.sql",
            "select * from absent_table",
        )
    with pytest.raises(ReturnError, match=invalid_meta_msg):
        i1.call(
            "pico.sql",
            "select * from absent_table where a = ?",
            (1,),
        )


def test_select(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        create table t (a int not null, primary key (a))
        using memtx
        distributed by (a)
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    data = i1.sql("""insert into t values(1);""")
    assert data["row_count"] == 1
    i1.sql("""insert into t values(2);""")
    i1.sql("""insert into t values(?);""", 2000)
    data = i1.sql("""select * from t where a = ?""", 2)
    assert data["rows"] == [[2]]
    data = i1.sql("""select * from t""")
    assert data["rows"] == [[1], [2], [2000]]
    data = i1.sql(
        """select * from t as t1
           join (select a as a2 from t) as t2
           on t1.a = t2.a2 where t1.a = ?""",
        2,
    )
    assert data["rows"] == [[2, 2]]


def test_read_from_global_tables(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    cluster.create_table(
        dict(
            name="global_t",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        )
    )
    index = i1.cas("insert", "global_t", [1])
    i1.raft_wait_index(index, 3)
    i2.raft_wait_index(index, 3)

    data = i2.sql(
        """
        select * from "global_t"
        """,
    )
    assert data["rows"] == [[1]]

    data = i1.sql(
        """
        select * from "_pico_table"
        """,
    )
    assert len(data["rows"]) == 1


def test_subqueries_on_global_tbls(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        create table g (a int not null, b int not null, primary key (a))
        using memtx
        distributed globally
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1
    for i in range(1, 6):
        index = i1.cas("insert", "G", [i, i])
        i1.raft_wait_index(index, 3)

    ddl = i1.sql(
        """
        create table s (c int not null, primary key (c))
        using memtx
        distributed by (c)
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1
    data = i1.sql("""insert into s values (1), (2), (3), (10);""")
    assert data["row_count"] == 4

    data = i1.sql(
        """
        select b from g
        where b in (select c from s where c in (2, 10))
        """,
    )
    assert data["rows"] == [[2]]

    data = i1.sql(
        """
        select b from g
        where b in (select sum(c) from s)
        """,
    )
    assert len(data["rows"]) == 0

    data = i1.sql(
        """
        select b from g
        where b in (select c * 5 from s)
        """,
    )
    assert data["rows"] == [[5]]

    # first subquery selects [1], [2], [3]
    # second subquery must add additional [4] tuple
    data = i1.sql(
        """
        select b from g
        where b in (select c from s) or a in (select count(*) from s)
        """,
    )
    assert data["rows"] == [[1], [2], [3], [4]]

    data = i1.sql(
        """
        select b from g
        where b in (select c from s) and a in (select count(*) from s)
        """,
    )
    assert len(data["rows"]) == 0

    data = i1.sql(
        """
        select c from s inner join
        (select c as c1 from s)
        on c = c1 + 3 and c in (select a from g)
        """,
    )
    assert data["rows"] == []

    # Full join because of 'OR'
    data = i1.sql(
        """
        select min(c) from s inner join
        (select c as c1 from s)
        on c = c1 + 3 or c in (select a from g)
        """,
    )
    assert data["rows"] == [[1]]

    data = i1.sql(
        """
        select a from g
        where b in (select c from s where c = 1) or
        b in (select c from s where c = 3)
        """,
    )
    assert data["rows"] == [[1], [3]]

    # TODO: uncomment when
    # https://git.picodata.io/picodata/picodata/sbroad/-/issues/542
    # is done.
    # data = i1.sql(
    #     """
    #     select a from g
    #     where b in (select c from s where c = 1) or
    #     b in (select c from s where c = 3) and
    #     a < (select sum(c) from s)
    #     """,
    # )
    # assert data["rows"] == [[1], [3]]


def test_aggregates_on_global_tbl(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        create table g (a int not null, b int not null, primary key (a))
        using memtx
        distributed globally
        option (timeout = 3)
        """
    )
    assert ddl["row_count"] == 1

    for i, j in [(1, 1), (2, 2), (3, 1), (4, 1), (5, 2)]:
        index = i1.cas("insert", "G", [i, j])
        i1.raft_wait_index(index, 3)

    data = i1.sql(
        """
        select count(*), min(b), max(b), min(b) + max(b) from g
        """
    )
    assert data["rows"] == [[5, 1, 2, 3]]

    data = i1.sql(
        """
        select b*b, sum(a + 1) from g
        group by b*b
        """
    )
    assert data["rows"] == [[1, 11], [4, 9]]

    data = i1.sql(
        """
        select b*b, sum(a + 1) from g
        group by b*b
        having count(a) > 2
        """
    )
    assert data["rows"] == [[1, 11]]


def test_hash(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        create table t (a int not null, primary key (a))
        using memtx
        distributed by (a)
    """
    )
    assert ddl["row_count"] == 1

    # Calculate tuple hash with Lua
    tup = (1,)
    key_def = KeyDef([KeyPart(1, "integer", True)])
    lua_hash = i1.hash(tup, key_def)
    bucket_count = 3000

    # Compare SQL and Lua bucket_id
    data = i1.sql("""insert into t values(?);""", 1)
    assert data["row_count"] == 1
    data = i1.sql(""" select "bucket_id" from t where a = ?""", 1)
    assert data["rows"] == [[lua_hash % bucket_count + 1]]


def test_select_lowercase_name(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    ddl = i1.sql(
        """
        create table "lowercase_name" ("id" int not null, primary key ("id"))
        distributed by ("id")
    """
    )
    assert ddl["row_count"] == 1

    assert i1.call("box.space.lowercase_name:select") == []

    data = i1.sql("""insert into "lowercase_name" values(420);""")
    assert data["row_count"] == 1
    data = i1.sql("""select * from "lowercase_name" """)
    assert data["rows"] == [[420]]


def test_select_string_field(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    ddl = i1.sql(
        """
        create table "STUFF" ("id" integer not null, "str" string null, primary key ("id"))
        distributed by ("id")
    """
    )
    assert ddl["row_count"] == 1

    data = i1.sql("""insert into STUFF values(1337, 'foo');""")
    assert data["row_count"] == 1
    data = i1.sql("""select * from STUFF """)
    assert data["rows"] == [[1337, "foo"]]


def test_create_drop_table(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    ddl = i1.sql(
        """
        create table "t" ("a" integer not null, "b" int not null, primary key ("b", "a"))
        using memtx
        distributed by ("a", "b")
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    # Already exists -> ok.
    ddl = i1.sql(
        """
        create table "t" ("a" integer not null, "b" int not null, primary key ("b", "a"))
        using memtx
        distributed by ("a", "b")
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 0

    # FIXME: this should fail
    # see https://git.picodata.io/picodata/picodata/picodata/-/issues/331
    # Already exists with different format -> error.
    ddl = i1.sql(
        """
        create table "t" ("key" string not null, "value" string not null, primary key ("key"))
        using memtx
        distributed by ("key")
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 0

    ddl = i2.sql(
        """
        drop table "t"
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    # Already dropped -> ok.
    ddl = i2.sql(
        """
        drop table "t"
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 0

    ddl = i2.sql(
        """
        create table "t" ("a" integer not null, "b" int not null, primary key ("b", "a"))
        using memtx
        distributed by ("a", "b")
    """
    )
    assert ddl["row_count"] == 1

    ddl = i1.sql(
        """
        drop table "t"
    """
    )
    assert ddl["row_count"] == 1

    # Check vinyl space
    ddl = i1.sql(
        """
        create table "t" ("key" string not null, "value" string not null, primary key ("key"))
        using vinyl
        distributed by ("key")
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    ddl = i2.sql(
        """
        drop table "t"
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    # Check global space
    ddl = i1.sql(
        """
        create table "global_t" ("key" string not null, "value" string not null,
        primary key ("key"))
        using memtx
        distributed globally
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    with pytest.raises(ReturnError, match="global spaces can use only memtx engine"):
        i1.sql(
            """
            create table "t" ("key" string not null, "value" string not null,
            primary key ("key"))
            using vinyl
            distributed globally
            option (timeout = 3)
            """
        )


def test_check_format(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    # Primary key missing.
    with pytest.raises(ReturnError, match="Primary key column b not found"):
        i1.sql(
            """
        create table "error" ("a" integer, primary key ("b"))
        using memtx
        distributed by ("a")
    """
        )
    # Sharding key missing.
    with pytest.raises(ReturnError, match="Sharding key column b not found"):
        i1.sql(
            """
        create table "error" ("a" integer not null, primary key ("a"))
        using memtx
        distributed by ("b")
    """
        )
    # Nullable primary key.
    with pytest.raises(
        ReturnError, match="Primary key mustn't contain nullable columns"
    ):
        i1.sql(
            """
        create table "error" ("a" integer null, primary key ("a"))
        using memtx
        distributed by ("a")
    """
        )

    # Check format
    ddl = i1.sql(
        """
        create table "t" (
            "non_nullable" string not null,
            "nullable" Boolean null,
            "default" Decimal,
            primary key ("non_nullable")
            )
        using memtx
        distributed by ("non_nullable")
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1
    format = i1.call("box.space.t:format")
    assert format == [
        {"is_nullable": False, "name": "non_nullable", "type": "string"},
        {"is_nullable": False, "name": "bucket_id", "type": "unsigned"},
        {"is_nullable": True, "name": "nullable", "type": "boolean"},
        {"is_nullable": True, "name": "default", "type": "decimal"},
    ]

    # Inserting with nulls/nonnulls works.
    dml = i1.sql(
        """
        insert into "t" values
            ('Name1', true, 0.0),
            ('Name2', null, -0.12974679036997294),
            ('Name3', false, null)
    """
    )
    assert dml["row_count"] == 3
    # Inserting with nulls/nonnulls works using params.
    dml = i1.sql(
        """
        insert into "t" ("non_nullable", "nullable") values
            ('Name4', ?),
            ('Name5', ?)
    """,
        True,
        None,
    )
    assert dml["row_count"] == 2


def test_insert(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, _ = cluster.instances

    ddl = i1.sql(
        """
        create table "t" ("a" integer not null, primary key ("a"))
        using memtx
        distributed by ("a")
    """
    )
    assert ddl["row_count"] == 1

    # Wrong parameters number.
    with pytest.raises(
        ReturnError,
        match="invalid node: parameter node does not refer to an expression",
    ):
        i1.sql(
            """
        insert into "t" values (?)
        """
        )
    with pytest.raises(
        ReturnError, match="Expected at least 2 values for parameters. Got 1"
    ):
        i1.sql(
            """
        insert into "t" values (?), (?)
            """,
            1,
        )


def test_insert_on_conflict(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        create table "t" ("a" integer not null, "b" int not null, primary key ("a"))
        using memtx
        distributed by ("b")
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    dml = i1.sql(
        """
        insert into "t" values (1, 1)
    """
    )
    assert dml["row_count"] == 1

    dml = i1.sql(
        """
        insert into "t" values (1, 1) on conflict do nothing
    """
    )
    assert dml["row_count"] == 0

    data = i1.sql(
        """select * from "t"
    """
    )
    assert data["rows"] == [[1, 1]]

    dml = i1.sql(
        """
        insert into "t" values (1, 2) on conflict do replace
    """
    )
    assert dml["row_count"] == 1

    data = i1.sql(
        """select * from "t"
    """
    )
    assert data["rows"] == [[1, 2]]


def test_sql_limits(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        create table "t" ("a" integer not null, "b" int not null, primary key ("a"))
        using memtx
        distributed by ("b")
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    dml = i1.sql(
        """
    insert into "t" values (1, 1), (2, 1)
    """
    )
    assert dml["row_count"] == 2

    with pytest.raises(
        ReturnError, match="Reached a limit on max executed vdbe opcodes. Limit: 5"
    ):
        i1.sql(
            """
        select * from "t" where "a" = 1 option(sql_vdbe_max_steps=5)
    """
        )

    dql = i1.sql(
        """
        select * from "t" where "a" = 1 option(sql_vdbe_max_steps=50)
    """
    )
    assert dql["rows"] == [[1, 1]]

    with pytest.raises(
        ReturnError,
        match=r"Exceeded maximum number of rows \(1\) in virtual table: 2",
    ):
        i1.sql(
            """
        select * from "t" option(vtable_max_rows=1, sql_vdbe_max_steps=50)
    """
        )


def test_sql_acl_password_length(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    username = "USER"
    password_short = "pwd"
    password_long = "password"

    acl = i1.sql(
        f"""
    create user {username} with password '{password_long}'
    using md5 option (timeout = 3)
"""
    )
    assert acl["row_count"] == 1
    acl = i1.sql(f"drop user {username}")
    assert acl["row_count"] == 1

    with pytest.raises(ReturnError, match="password is too short"):
        i1.sql(
            """
            create user {username} with password '{password}'
            using md5 option (timeout = 3)
        """.format(
                username=username, password=password_short
            )
        )

    acl = i1.sql(
        f"""
    create user {username} with password '{password_short}'
    using ldap option (timeout = 3)
"""
    )
    assert acl["row_count"] == 1
    acl = i1.sql(f"drop user {username}")
    assert acl["row_count"] == 1


def test_sql_acl_users_roles(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    username = "User"
    password = "Password"
    upper_username = "USER"
    rolename = "Role"
    upper_rolename = "ROLE"
    default_users = [
        [0, "guest", 0, ["chap-sha1", "vhvewKp0tNyweZQ+cFKAlsyphfg="], 1],
        [1, "admin", 0, ["chap-sha1", ""], 1],
    ]
    default_roles = [[2, "public", 0, 1], [31, "super", 0, 1]]
    acl = i1.sql(
        f"""
        create user "{username}" with password '{password}'
        using md5 option (timeout = 3)
    """
    )
    assert acl["row_count"] == 1

    # Dropping user that doesn't exist should return 0.
    acl = i1.sql(f"drop user {upper_username}")
    assert acl["row_count"] == 0

    # Dropping user that does exist should return 1.
    acl = i1.sql(f'drop user "{username}"')
    assert acl["row_count"] == 1
    assert i1.call("box.space._pico_user:select") == default_users

    # All the usernames below should match the same user.
    # * Upcasted username in double parentheses shouldn't change.
    acl = i1.sql(
        f"""
        create user "{upper_username}" password '{password}'
        using chap-sha1
    """
    )
    assert acl["row_count"] == 1
    # * Username as is in double parentheses.
    acl = i1.sql(f'drop user "{upper_username}"')
    assert acl["row_count"] == 1
    acl = i1.sql(
        f"""
        create user "{upper_username}" password '' using ldap
    """
    )
    assert acl["row_count"] == 1
    acl = i1.sql(f"drop user {username}")
    assert acl["row_count"] == 1
    # * Username without parentheses should be upcasted.
    acl = i1.sql(
        f"""
        create user {username} with password '{password}'
        option (timeout = 3)
    """
    )
    assert acl["row_count"] == 1
    acl = i1.sql(f'drop user "{upper_username}"')
    assert acl["row_count"] == 1

    # Check user creation with LDAP works well with non-empty password specification
    # (it must be ignored).
    acl = i1.sql(
        f"""
        create user "{upper_username}" password 'smth' using ldap
    """
    )
    assert acl["row_count"] == 1

    acl = i1.sql(f"drop user {username}")
    assert acl["row_count"] == 1

    # We can safely retry creating the same user.
    acl = i1.sql(f"create user {username} with password '{password}' using md5")
    assert acl["row_count"] == 1
    acl = i1.sql(f"create user {username} with password '{password}' using md5")
    assert acl["row_count"] == 0
    acl = i1.sql(f"drop user {username}")
    assert acl["row_count"] == 1

    # Zero timeout should return timeout error.
    with pytest.raises(ReturnError, match="timeout"):
        i1.sql(f"drop user {username} option (timeout = 0)")
    with pytest.raises(ReturnError, match="timeout"):
        i1.sql(f"drop role {username} option (timeout = 0)")
    with pytest.raises(ReturnError, match="timeout"):
        i1.sql(
            f"""
            create user {username} with password '{password}'
            option (timeout = 0)
        """
        )
    with pytest.raises(ReturnError, match="timeout"):
        i1.sql(
            f"""
            alter user {username} with password '{password}'
            option (timeout = 0)
        """
        )

    # Username in single quotes is unsupported.
    with pytest.raises(ReturnError, match="rule parsing error"):
        i1.sql(f"drop user '{username}'")
    with pytest.raises(ReturnError, match="rule parsing error"):
        i1.sql(f"create user '{username}' with password '{password}'")
    with pytest.raises(ReturnError, match="rule parsing error"):
        i1.sql(f"alter user '{username}' with password '{password}'")
    # Rolename in single quotes is unsupported.
    with pytest.raises(ReturnError, match="rule parsing error"):
        i1.sql(f"drop role '{username}'")

    # Can't create same user with different auth methods.
    with pytest.raises(ReturnError, match="already exists with different auth method"):
        i1.sql(f"create user {username} with password '{password}' using md5")
        i1.sql(f"create user {username} with password '{password}' using chap-sha1")

    # Can't create same user with different password.
    with pytest.raises(ReturnError, match="already exists with different auth method"):
        i1.sql(f"create user {username} with password '123456789' using md5")
        i1.sql(f"create user {username} with password '987654321' using md5")
    acl = i1.sql(f"drop user {username}")
    assert acl["row_count"] == 1

    another_password = "qwerty123"
    # Alter of unexisted user should do nothing.
    acl = i1.sql(f"alter user \"nobody\" with password '{another_password}'")
    assert acl["row_count"] == 0

    # Check altering works.
    acl = i1.sql(f"create user {username} with password '{password}' using md5")
    assert acl["row_count"] == 1
    users_auth_was = i1.call("box.space._pico_user:select")[2][3]
    # * Password and method aren't changed -> update nothing.
    acl = i1.sql(f"alter user {username} with password '{password}' using md5")
    assert acl["row_count"] == 0
    users_auth_became = i1.call("box.space._pico_user:select")[2][3]
    assert users_auth_was == users_auth_became
    # * Password is changed -> update hash.
    acl = i1.sql(f"alter user {username} with password '{another_password}' using md5")
    assert acl["row_count"] == 1
    users_auth_became = i1.call("box.space._pico_user:select")[2][3]
    assert users_auth_was[0] == users_auth_became[0]
    assert users_auth_was[1] != users_auth_became[1]
    # * Password and method are changed -> update method and hash.
    acl = i1.sql(
        f"alter user {username} with password '{another_password}' using chap-sha1"
    )
    assert acl["row_count"] == 1
    users_auth_became = i1.call("box.space._pico_user:select")[2][3]
    assert users_auth_was[0] != users_auth_became[0]
    assert users_auth_was[1] != users_auth_became[1]
    # * LDAP should ignore password -> update method and hash.
    acl = i1.sql(f"alter user {username} with password '{another_password}' using ldap")
    assert acl["row_count"] == 1
    users_auth_became = i1.call("box.space._pico_user:select")[2][3]
    assert users_auth_was[0] != users_auth_became[0]
    assert users_auth_became[1] == ""
    acl = i1.sql(f"drop user {username}")
    assert acl["row_count"] == 1

    # Attempt to create role with the name of already existed user
    # should lead to an error.
    acl = i1.sql(f""" create user "{username}" with password '123456789' using md5 """)
    assert acl["row_count"] == 1
    with pytest.raises(ReturnError, match="User with the same name already exists"):
        i1.sql(f'create role "{username}"')
    acl = i1.sql(f'drop user "{username}"')
    assert acl["row_count"] == 1

    # Dropping role that doesn't exist should return 0.
    acl = i1.sql(f"drop role {rolename}")
    assert acl["row_count"] == 0

    # Successive creation of role.
    acl = i1.sql(f'create role "{rolename}"')
    assert acl["row_count"] == 1
    # Unable to alter role.
    with pytest.raises(
        ReturnError, match=f"Role {rolename} exists. Unable to alter role."
    ):
        i1.sql(f"alter user \"{rolename}\" with password '{password}'")

    # Creation of the role that already exists shouldn't do anything.
    acl = i1.sql(f'create role "{rolename}"')
    assert acl["row_count"] == 0

    # Dropping role that does exist should return 1.
    acl = i1.sql(f'drop role "{rolename}"')
    assert acl["row_count"] == 1
    assert i1.call("box.space._pico_role:select") == default_roles

    # All the rolenames below should match the same role.
    acl = i1.sql(f'create role "{upper_rolename}"')
    assert acl["row_count"] == 1
    acl = i1.sql(f"drop role {upper_rolename}")
    assert acl["row_count"] == 1
    acl = i1.sql(f'create role "{upper_rolename}"')
    assert acl["row_count"] == 1
    acl = i1.sql(f"drop role {rolename}")
    assert acl["row_count"] == 1
    acl = i1.sql(f'create role "{upper_rolename}"')
    assert acl["row_count"] == 1
    acl = i1.sql(f'drop role "{upper_rolename}"')
    assert acl["row_count"] == 1


@pytest.mark.skip(
    reason="session privilege is not automatically given during user creation"
)
def test_sql_alter_login(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    username = "USER"
    password = "PASSWORD"
    # Create user.
    acl = i1.sql(f"create user {username} with password '{password}'")
    assert acl["row_count"] == 1

    # Alter user with LOGIN option do nothing.
    acl = i1.sql(f""" alter user {username} with login """)
    assert acl["row_count"] == 0
    # * Alter user with NOLOGIN option.
    acl = i1.sql(f""" alter user {username} with nologin """)
    assert acl["row_count"] == 1
    # * Alter user with NOLOGIN again do nothing.
    acl = i1.sql(f""" alter user {username} with nologin """)
    assert acl["row_count"] == 0
    # * TODO: Check SESSION privilege is removed.


def test_sql_acl_privileges(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    username = "USER"
    another_username = "ANOTHER_USER"
    password = "PASSWORD"
    rolename = "ROLE"
    another_rolename = "ANOTHER_ROLE"

    # Create users.
    acl = i1.sql(f"create user {username} with password '{password}'")
    assert acl["row_count"] == 1
    acl = i1.sql(f"create user {another_username} with password '{password}'")
    assert acl["row_count"] == 1
    # Create roles.
    acl = i1.sql(f"create role {rolename}")
    assert acl["row_count"] == 1
    acl = i1.sql(f"create role {another_rolename}")
    assert acl["row_count"] == 1
    # Create tables.
    table_name = "T1"
    another_table_name = "T2"
    ddl = i1.sql(
        f"""
        create table {table_name} ("a" int not null, primary key ("a"))
        distributed by ("a")
    """
    )
    assert ddl["row_count"] == 1

    ddl = i1.sql(
        f"""
        create table {another_table_name} ("a" int not null, primary key ("a"))
        distributed by ("a")
    """
    )
    assert ddl["row_count"] == 1

    # Remember number of default privileges.
    default_privileges_number = len(
        i1.sql(""" select * from "_pico_privilege" """)["rows"]
    )

    # =========================ERRORs======================
    # Attempt to grant unsupported privileges.
    with pytest.raises(
        ReturnError, match=r"Supported privileges are: \[Read, Write, Alter, Drop\]"
    ):
        i1.sql(f""" grant create on table {table_name} to {username} """)
    with pytest.raises(
        ReturnError, match=r"Supported privileges are: \[Create, Alter, Drop\]"
    ):
        i1.sql(f""" grant read user to {username} """)
    with pytest.raises(ReturnError, match=r"Supported privileges are: \[Alter, Drop\]"):
        i1.sql(f""" grant create on user {username} to {rolename} """)
    with pytest.raises(
        ReturnError, match=r"Supported privileges are: \[Create, Drop\]"
    ):
        i1.sql(f""" grant alter role to {username} """)
    with pytest.raises(ReturnError, match=r"Supported privileges are: \[Drop\]"):
        i1.sql(f""" grant create on role {rolename} to {username} """)

    # Attempt to grant unexisted role.
    with pytest.raises(ReturnError, match="There is no role with name SUPER"):
        i1.sql(f""" grant SUPER to {username} """)
    # Attempt to grant TO unexisted role.
    with pytest.raises(
        ReturnError, match="Nor user, neither role with name SUPER exists"
    ):
        i1.sql(f""" grant {rolename} to SUPER """)
    # Attempt to revoke unexisted role.
    with pytest.raises(ReturnError, match="There is no role with name SUPER"):
        i1.sql(f""" revoke SUPER from {username} """)
    # Attempt to revoke privilege that hasn't been granted yet do noting.
    acl = i1.sql(f""" revoke read on table {table_name} from {username} """)
    assert acl["row_count"] == 0
    # TODO: Attempt to grant role that doesn't visible for user.
    # TODO: Attempt to revoke role that doesn't visible for user.
    # TODO: Attempt to grant TO a user that doesn't visible for user.
    # TODO: Attempt to grant TO a role that doesn't visible for user.
    # TODO: Attempt to revoke FROM a user that doesn't visible for user.
    # TODO: Attempt to revoke FROM a role that doesn't visible for user.

    # ===============ALTER Login/NoLogin=====================
    # TODO: replace with logic from `test_sql_alter_login`.

    # TODO: ================USERs interaction================
    # * TODO: User creation is prohibited.
    # * Grant CREATE to user.
    # Need to grant as admin because only admin is allowed to make wildcard grants
    acl = i1.sudo_sql(f""" grant create user to {username} """)
    assert acl["row_count"] == 1
    # * Check privileges table is updated.
    privs_rows = i1.sql(""" select * from "_pico_privilege" """)["rows"]
    assert len(privs_rows) == default_privileges_number + 1
    # * TODO: User creation is available.
    # * Revoke CREATE from user.
    acl = i1.sudo_sql(f""" revoke create user from {username} """)
    assert acl["row_count"] == 1
    # * Check privileges table is updated.
    privs_rows = i1.sql(""" select * from "_pico_privilege" """)["rows"]
    assert len(privs_rows) == default_privileges_number
    # * TODO: Check that user with granted privileges can ALTER and DROP created user
    #         as it's the owner.
    # * TODO: Revoke automatically granted privileges.
    # * TODO: Check ALTER and DROP are prohibited.
    # * Grant global ALTER on users.
    acl = i1.sudo_sql(f""" grant alter user to {username} """)
    assert acl["row_count"] == 1
    # * Check privileges table is updated.
    privs_rows = i1.sql(""" select * from "_pico_privilege" """)["rows"]
    assert len(privs_rows) == default_privileges_number + 1
    # * TODO: Check ALTER is available.
    # * Revoke global ALTER.
    acl = i1.sudo_sql(f""" revoke alter user from {username} """)
    assert acl["row_count"] == 1

    privs_rows = i1.sql(""" select * from "_pico_privilege" """)["rows"]
    assert len(privs_rows) == default_privileges_number

    # * TODO: Check another user can't initially interact with previously created new user.
    # * TODO: Grant ALTER and DROP user privileges to another user.
    # * TODO: Check user alternation is available.
    # * TODO: Check user drop is available.

    # TODO: ================ROLEs interaction================
    # * TODO: Role creation is prohibited.
    # * Grant CREATE to user.
    acl = i1.sudo_sql(f""" grant create role to {username} """)
    assert acl["row_count"] == 1
    # * Check privileges table is updated.
    privs_rows = i1.sql(""" select * from "_pico_privilege" """)["rows"]
    assert len(privs_rows) == default_privileges_number + 1
    # * TODO: Role creation is available.
    # * Revoke CREATE from user.
    acl = i1.sudo_sql(f""" revoke create role from {username} """)
    assert acl["row_count"] == 1
    # * Check privileges table is updated.
    privs_rows = i1.sql(""" select * from "_pico_privilege" """)["rows"]
    assert len(privs_rows) == default_privileges_number
    # * TODO: Check that user with granted privileges can DROP created role as it's the owner.
    # * TODO: Revoke automatically granted privileges.
    # * TODO: Check DROP are prohibited.
    # * Grant global drop on role.
    acl = i1.sudo_sql(f""" grant drop role to {username} """)
    assert acl["row_count"] == 1
    # * Check privileges table is updated.
    privs_rows = i1.sql(""" select * from "_pico_privilege" """)["rows"]
    assert len(privs_rows) == default_privileges_number + 1
    # * TODO: Check DROP is available.
    # * Revoke global DROP.
    acl = i1.sudo_sql(f""" revoke drop role from {username} """)
    assert acl["row_count"] == 1

    # * TODO: Check another user can't initially interact with previously created new role.
    # * TODO: Grant DROP role privileges to another user.
    # * TODO: Check role drop is available.

    # TODO: ================TABLEs interaction===============
    # ------------------READ---------------------------------
    # * READ is not available.
    with pytest.raises(
        ReturnError,
        match=rf"Read access to space '{table_name}' is denied for user '{username}'",
    ):
        i1.sql(f""" select * from {table_name} """, user=username, password=password)
    # * Grant READ to user.
    acl = i1.sudo_sql(f""" grant read on table {table_name} to {username} """)
    assert acl["row_count"] == 1
    # * Granting already granted privilege do nothing.
    acl = i1.sql(f""" grant read on table {table_name} to {username} """)
    assert acl["row_count"] == 0
    # * After grant READ succeeds.
    i1.sql(f""" select * from {table_name} """, user=username, password=password)
    # * Revoke READ.
    acl = i1.sudo_sql(f""" revoke read on table {table_name} from {username} """)
    assert acl["row_count"] == 1
    # * After revoke READ fails again.
    with pytest.raises(
        ReturnError,
        match=rf"Read access to space '{table_name}' is denied for user '{username}'",
    ):
        i1.sql(f""" select * from {table_name} """, user=username, password=password)
    # ------------------WRITE---------------------------------
    # TODO: remove
    acl = i1.sudo_sql(f""" grant read on table {table_name} to {username} """)
    assert acl["row_count"] == 1
    # * WRITE is not available.
    with pytest.raises(
        ReturnError,
        match=rf"Write access to space '{table_name}' is denied for user '{username}'",
    ):
        i1.sql(
            f""" insert into {table_name} values (1) """,
            user=username,
            password=password,
        )
    # * Grant WRITE to user.
    acl = i1.sudo_sql(f""" grant write on table {table_name} to {username} """)
    assert acl["row_count"] == 1
    # * WRITE succeeds.
    i1.sql(
        f""" insert into {table_name} values (1) """, user=username, password=password
    )
    i1.sql(f""" delete from {table_name} where "a" = 1 """)
    # * Revoke WRITE from role.
    acl = i1.sudo_sql(f""" revoke write on table {table_name} from {username} """)
    assert acl["row_count"] == 1
    # * WRITE fails again.
    with pytest.raises(
        ReturnError,
        match=rf"Write access to space '{table_name}' is denied for user '{username}'",
    ):
        i1.sql(
            f""" insert into {table_name} values (1) """,
            user=username,
            password=password,
        )
    # TODO: remove
    acl = i1.sudo_sql(f""" revoke read on table {table_name} from {username} """)
    assert acl["row_count"] == 1
    # ------------------CREATE---------------------------------
    # * TODO: Unable to create table.
    # * Grant CREATE to user.
    acl = i1.sudo_sql(f""" grant create table to {username} """)
    assert acl["row_count"] == 1
    # * TODO: Creation is available.
    # * TODO: Check user can do everything he wants on a table he created:
    # ** READ.
    # ** WRITE.
    # ** CREATE index.
    # ** ALTER index.
    # ** DROP.
    # * Revoke CREATE from user.
    acl = i1.sudo_sql(f""" revoke create table from {username} """)
    assert acl["row_count"] == 1
    # * TODO: Creation is not available again.
    # ------------------ALTER--------------------------------
    # * TODO: Unable to create new table index.
    # * Grant ALTER to user.
    acl = i1.sudo_sql(f""" grant alter on table {table_name} to {username} """)
    assert acl["row_count"] == 1
    # * TODO: Index creation succeeds.
    # * Revoke ALTER from user.
    acl = i1.sudo_sql(f""" revoke alter on table {table_name} from {username} """)
    assert acl["row_count"] == 1
    # * TODO: Attempt to remove index fails.
    # ------------------DROP---------------------------------
    # * TODO: Unable to drop table previously created by admin.
    # * Grant DROP to user.
    acl = i1.sudo_sql(f""" grant drop on table {table_name} to {username} """)
    assert acl["row_count"] == 1
    # * TODO: Able to drop admin table.
    # * Revoke DROP from user.
    acl = i1.sudo_sql(f""" revoke drop on table {table_name} from {username} """)
    assert acl["row_count"] == 1

    # Grant global tables READ, WRITE, ALTER, DROP.
    acl = i1.sudo_sql(f""" grant read table to {username} """)
    assert acl["row_count"] == 1
    acl = i1.sudo_sql(f""" grant write table to {username} """)
    assert acl["row_count"] == 1
    acl = i1.sudo_sql(f""" grant alter table to {username} """)
    assert acl["row_count"] == 1
    acl = i1.sudo_sql(f""" grant drop table to {username} """)
    assert acl["row_count"] == 1
    # Check all operations available on another_table created by admin.
    i1.sql(
        f""" select * from {another_table_name} """, user=username, password=password
    )
    i1.sql(
        f""" insert into {another_table_name} values (1) """,
        user=username,
        password=password,
    )
    i1.sql(f""" delete from {another_table_name} """, user=username, password=password)
    i1.eval(
        f"""
        box.space.{another_table_name}:create_index('some', {{ parts = {{ 'a' }} }})
    """
    )
    i1.sql(f""" delete from {another_table_name} """, user=username, password=password)
    ddl = i1.sql(
        f""" drop table {another_table_name} """, user=username, password=password
    )
    assert ddl["row_count"] == 1
    # Revoke global privileges
    acl = i1.sudo_sql(f""" revoke read table from {username} """)
    assert acl["row_count"] == 1
    acl = i1.sudo_sql(f""" revoke write table from {username} """)
    assert acl["row_count"] == 1
    acl = i1.sudo_sql(f""" revoke alter table from {username} """)
    assert acl["row_count"] == 1
    acl = i1.sudo_sql(f""" revoke drop table from {username} """)
    assert acl["row_count"] == 1

    # ================ROLE passing================
    # * Check there are no privileges granted to anything initially.
    privs_rows = i1.sql(""" select * from "_pico_privilege" """)["rows"]
    assert len(privs_rows) == default_privileges_number
    # * Read from table is prohibited for user initially.
    with pytest.raises(
        ReturnError,
        match=rf"Read access to space '{table_name}' is denied for user '{username}'",
    ):
        i1.sql(f""" select * from {table_name} """, user=username, password=password)
    # * Grant table READ and WRITE to role.
    acl = i1.sudo_sql(f""" grant read on table {table_name} to {rolename} """)
    assert acl["row_count"] == 1
    acl = i1.sudo_sql(f""" grant write on table {table_name} to {rolename} """)
    assert acl["row_count"] == 1
    # * Grant ROLE to user.
    acl = i1.sudo_sql(f""" grant {rolename} to {username} """)
    assert acl["row_count"] == 1
    # * Check read and write is available for user.
    i1.sql(f""" select * from {table_name} """, user=username, password=password)
    i1.sql(
        f""" insert into {table_name} values (1) """, user=username, password=password
    )
    i1.sql(f""" delete from {table_name} where "a" = 1 """)
    # * Revoke privileges from role.
    acl = i1.sudo_sql(f""" revoke write on table {table_name} from {rolename} """)
    assert acl["row_count"] == 1
    acl = i1.sudo_sql(f""" revoke read on table {table_name} from {rolename} """)
    assert acl["row_count"] == 1
    # * Check privilege revoked from role and user.
    privs_rows = i1.sql(""" select * from "_pico_privilege" """)["rows"]
    assert len(privs_rows) == default_privileges_number + 1  # default + role for user
    # * Check read is prohibited again.
    with pytest.raises(
        ReturnError,
        match=rf"Read access to space '{table_name}' is denied for user '{username}'",
    ):
        i1.sql(f""" select * from {table_name} """, user=username, password=password)


def test_distributed_sql_via_set_language(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    prelude = """
        local console = require('console')
        console.eval([[\\ set language sql]])
        console.eval([[\\ set delimiter ;]])
    """

    i1.eval(
        f"""
        {prelude}
        return console.eval('create table t \
            (a integer not null, b int not null, primary key (a)) \
                using memtx distributed by (b) option (timeout = 3);')
    """
    )

    i1.eval(
        f"""
        {prelude}
        return console.eval('insert into t values (22, 8);')
    """
    )

    select_from_second_instance = i2.eval(
        f"""
        {prelude}
        return console.eval('select * from t where a = 22;')
    """
    )

    assert (
        select_from_second_instance
        == """---
- metadata:
  - {'name': 'A', 'type': 'integer'}
  - {'name': 'B', 'type': 'integer'}
  rows:
  - [22, 8]
...
"""
    )


def test_sql_privileges(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    table_name = "t"
    # Create a test table
    ddl = i1.sql(
        f"""
        create table "{table_name}" ("a" int not null, "b" int, primary key ("a"))
        using memtx
        distributed by ("a")
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    username = "alice"
    alice_pwd = "1234567890"

    # Create user with execute on universe privilege
    acl = i1.sql(
        f"""
        create user "{username}" with password '{alice_pwd}'
        using chap-sha1 option (timeout = 3)
    """
    )
    assert acl["row_count"] == 1
    i1.eval(f""" pico.grant_privilege("{username}", "execute", "universe") """)

    # ------------------------
    # Check SQL read privilege
    # ------------------------
    with pytest.raises(
        ReturnError, match=f"AccessDenied: Read access to space '{table_name}'"
    ):
        i1.sql(f""" select * from "{table_name}" """, user=username, password=alice_pwd)
    # Grant read privilege
    i1.sudo_sql(f""" grant read on table "{table_name}" to "{username}" """)
    dql = i1.sql(
        f""" select * from "{table_name}" """, user=username, password=alice_pwd
    )
    assert dql["rows"] == []

    # Revoke read privilege
    i1.sudo_sql(f""" revoke read on table "{table_name}" from "{username}" """)

    # -------------------------
    # Check SQL write privilege
    # -------------------------
    with pytest.raises(
        ReturnError, match=f"AccessDenied: Write access to space '{table_name}'"
    ):
        i1.sql(
            f""" insert into "{table_name}" values (1, 2) """,
            user=username,
            password=alice_pwd,
        )

    # Grant write privilege
    i1.sudo_sql(f""" grant write on table "{table_name}" to "{username}" """)
    dml = i1.sql(
        f""" insert into "{table_name}" values (1, 2) """,
        user=username,
        password=alice_pwd,
    )
    assert dml["row_count"] == 1

    # Revoke write privilege
    i1.sudo_sql(f""" revoke write on table "{table_name}" from "{username}" """)

    # -----------------------------------
    # Check SQL write and read privileges
    # -----------------------------------
    with pytest.raises(
        ReturnError, match=f"AccessDenied: Read access to space '{table_name}'"
    ):
        i1.sql(
            f""" insert into "{table_name}" select "a" + 1, "b" from "{table_name}"  """,
            user=username,
            password=alice_pwd,
        )
    with pytest.raises(
        ReturnError, match=f"AccessDenied: Read access to space '{table_name}'"
    ):
        i1.sql(
            f""" update "{table_name}" set "b" = 42 """,
            user=username,
            password=alice_pwd,
        )
    with pytest.raises(
        ReturnError, match=f"AccessDenied: Read access to space '{table_name}'"
    ):
        i1.sql(f""" delete from "{table_name}" """, user=username, password=alice_pwd)

    # Grant read privilege
    i1.sudo_sql(f""" grant read on table "{table_name}" to "{username}" """)

    with pytest.raises(
        ReturnError, match=f"AccessDenied: Write access to space '{table_name}'"
    ):
        i1.sql(
            f""" insert into "{table_name}" select "a" + 1, "b" from "{table_name}"  """,
            user=username,
            password=alice_pwd,
        )
    with pytest.raises(
        ReturnError, match=f"AccessDenied: Write access to space '{table_name}'"
    ):
        i1.sql(
            f""" update "{table_name}" set "b" = 42 """,
            user=username,
            password=alice_pwd,
        )
    with pytest.raises(
        ReturnError, match=f"AccessDenied: Write access to space '{table_name}'"
    ):
        i1.sql(f""" delete from "{table_name}" """, user=username, password=alice_pwd)

    # Grant write privilege
    i1.sudo_sql(f""" grant write on table "{table_name}" to "{username}" """)

    dml = i1.sql(
        f""" insert into "{table_name}" select "a" + 1, "b" from "{table_name}"  """,
        user=username,
        password=alice_pwd,
    )
    assert dml["row_count"] == 1
    dml = i1.sql(
        f""" update "{table_name}" set "b" = 42 """, user=username, password=alice_pwd
    )
    assert dml["row_count"] == 2
    dml = i1.sql(f""" delete from "{table_name}" """, user=username, password=alice_pwd)
    assert dml["row_count"] == 2
