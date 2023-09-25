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

    usage_msg = re.escape("Usage: sql(query[, params])")
    with pytest.raises(ReturnError, match=usage_msg):
        i1.call(
            "pico.sql",
        )
    with pytest.raises(ReturnError, match=usage_msg):
        i1.call(
            "pico.sql",
            "select * from t",
            {},
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
        """
        create user {username} with password '{password}'
        using md5 option (timeout = 3)
    """.format(
            username=username, password=password_long
        )
    )
    assert acl["row_count"] == 1
    acl = i1.sql(f"drop user {username}")
    assert acl["row_count"] == 1

    with pytest.raises(ReturnError, match="password is too short"):
        acl = i1.sql(
            """
            create user {username} with password '{password}'
            using md5 option (timeout = 3)
        """.format(
                username=username, password=password_short
            )
        )

    acl = i1.sql(
        """
        create user {username} with password '{password}'
        using ldap option (timeout = 3)
    """.format(
            username=username, password=password_short
        )
    )
    assert acl["row_count"] == 1
    acl = i1.sql(f"drop user {username}")
    assert acl["row_count"] == 1


def test_sql_acl_user(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    username = "User"
    password = "Password"
    upper_username = "USER"
    rolename = "Role"
    upper_rolename = "ROLE"
    acl = i1.sql(
        """
        create user "{username}" with password '{password}'
        using md5 option (timeout = 3)
    """.format(
            username=username, password=password
        )
    )
    assert acl["row_count"] == 1

    # Dropping user that doesn't exist should return 0.
    acl = i1.sql(f"drop user {upper_username}")
    assert acl["row_count"] == 0

    # Dropping user that does exist should return 1.
    acl = i1.sql(f'drop user "{username}"')
    assert acl["row_count"] == 1
    assert i1.call("box.space._pico_user:select") == []

    # All the usernames below should match the same user.
    acl = i1.sql(
        """
        create user "{username}" password '{password}'
        using chap-sha1
    """.format(
            username=upper_username, password=password
        )
    )
    assert acl["row_count"] == 1
    acl = i1.sql(f'drop user "{upper_username}"')
    assert acl["row_count"] == 1

    acl = i1.sql(
        """
        create user "{username}" password '' using ldap
    """.format(
            username=upper_username
        )
    )
    assert acl["row_count"] == 1
    acl = i1.sql(f"drop user {username}")
    assert acl["row_count"] == 1

    acl = i1.sql(
        """
        create user {username} with password '{password}'
        option (timeout = 3)
    """.format(
            username=username, password=password
        )
    )
    acl = i1.sql(f'drop user "{upper_username}"')
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
            """
            create user {username} with password '{password}'
            option (timeout = 0)
        """.format(
                username=username, password=password
            )
        )

    # Username in single quotes is unsupported.
    with pytest.raises(ReturnError, match="rule parsing error"):
        i1.sql(f"drop user '{username}'")
    with pytest.raises(ReturnError, match="rule parsing error"):
        i1.sql(f"create user '{username}' with password '{password}'")
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

    # Attempt to create role with the name of already existed user
    # should lead to an error.
    acl = i1.sql(f""" create user "{username}" with password '123456789' using md5 """)
    assert acl["row_count"] == 1
    with pytest.raises(ReturnError, match="User with the same name already exists"):
        i1.sql(f'create role "{username}"')

    # Dropping role that doesn't exist should return 0.
    acl = i1.sql(f"drop role {rolename}")
    assert acl["row_count"] == 0

    # Successive creation of role.
    acl = i1.sql(f'create role "{rolename}"')
    assert acl["row_count"] == 1

    # Creation of the role that already exists shouldn't do anything.
    acl = i1.sql(f'create role "{rolename}"')
    assert acl["row_count"] == 0

    # Dropping role that does exist should return 1.
    acl = i1.sql(f'drop role "{rolename}"')
    assert acl["row_count"] == 1
    assert i1.call("box.space._pico_role:select") == []

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
