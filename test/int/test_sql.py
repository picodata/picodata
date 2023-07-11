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
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    ddl = i1.sql(
        """
        create table t (a int, primary key (a))
        using memtx
        distributed by (a)
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    data = i1.sql("""insert into t values(1);""")
    assert data["row_count"] == 1
    i2.sql("""insert into t values(2);""")
    i2.sql("""insert into t values(?);""", 2000)
    data = i1.sql("""select * from t where a = ?""", 2)
    assert data["rows"] == [[2]]
    data = i1.sql("""select * from t""")
    assert data["rows"] == [[1], [2], [2000]]
    data = i2.sql(
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
        create table t (a int, primary key (a))
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
    assert data["rows"] == [[lua_hash % bucket_count]]


def test_select_lowercase_name(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    ddl = i1.sql(
        """
        create table "lowercase_name" ("id" int, primary key ("id"))
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
