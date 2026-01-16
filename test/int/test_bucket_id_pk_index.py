import math
import psycopg
import pytest

from conftest import (
    Cluster,
    Instance,
)


@pytest.mark.parametrize(
    "engine",
    ["memtx", "vinyl"],
)
def test_simple_table_having_bucket_id_in_pk(cluster: Cluster, engine: str):
    i1 = cluster.add_instance(replicaset_name="r1")
    cluster.wait_until_instance_has_this_many_active_buckets(i1, 3000)

    ddl = i1.sql(
        f"""
        CREATE TABLE sharded_table
        (a INT NOT NULL, b INT, PRIMARY KEY (bucket_id, a))
        USING {engine}
        DISTRIBUTED BY (a)
        """
    )
    assert ddl["row_count"] == 1
    table_size = 9000
    batch_size = 1000
    for start in range(0, table_size, batch_size):
        response = i1.sql(
            "INSERT INTO sharded_table VALUES " + (", ".join([f"({i},{i})" for i in range(start, start + batch_size)]))
        )
        assert response["row_count"] == batch_size

    i2 = cluster.add_instance(replicaset_name="r2")
    cluster.wait_until_instance_has_this_many_active_buckets(i2, 1500)

    # check vshard rebalancing
    res = i1.eval("return box.space.sharded_table:count()")
    assert math.isclose(res, table_size / 2, abs_tol=20)
    res = i2.eval("return box.space.sharded_table:count()")
    assert math.isclose(res, table_size / 2, abs_tol=20)

    # check schema correctness
    res = i1.sql("SELECT id, format, distribution FROM _pico_table WHERE name = 'sharded_table'")
    assert res[0][1] == [
        {
            "field_type": "unsigned",
            "is_nullable": False,
            "name": "bucket_id",
        },
        {"field_type": "integer", "is_nullable": False, "name": "a"},
        {"field_type": "integer", "is_nullable": True, "name": "b"},
    ]
    assert res[0][2] == {"ShardedImplicitly": [["a"], "murmur3", "default"]}
    table_id = res[0][0]
    res = i1.sql(f"SELECT parts FROM _pico_index WHERE table_id = {table_id}")
    assert res[0][0] == [
        ["bucket_id", "unsigned", None, False, None],
        ["a", "integer", None, False, None],
    ]

    res = i1.eval(f"return box.space._space:select({table_id})")
    assert res[0][6] == [
        {"is_nullable": False, "name": "bucket_id", "type": "unsigned"},
        {"is_nullable": False, "name": "a", "type": "integer"},
        {"is_nullable": True, "name": "b", "type": "integer"},
    ]

    res = i1.eval(f"return box.space._index:select({table_id})")
    assert res[0][5] == [
        {"field": 0, "is_nullable": False, "type": "unsigned"},
        {"field": 1, "is_nullable": False, "type": "integer"},
    ]

    # check DML
    res = i1.sql("SELECT * FROM sharded_table WHERE a = 42")
    assert res == [[42, 42]]
    res = i1.sql("UPDATE sharded_table SET b = 43 WHERE a = 42")
    assert res["row_count"] == 1
    res = i1.sql("UPDATE sharded_table SET b = 42 WHERE b = 43")
    assert res["row_count"] == 2
    res = i1.sql("DELETE FROM sharded_table WHERE a = 42")
    assert res["row_count"] == 1

    # check pgproto
    user, password = "postgres", "Passw0rd"
    host, port = i1.pg_host, i1.pg_port
    i1.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    i1.sql(f'GRANT READ ON TABLE sharded_table TO "{user}"', sudo=True)
    i1.sql(f'GRANT WRITE ON TABLE sharded_table TO "{user}"', sudo=True)
    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
        """
        SELECT * FROM sharded_table WHERE a = 43
        """
    )
    assert cur.fetchall() == [
        (
            43,
            42,
        )
    ]

    # check other table with separate bucket_id index
    ddl = i1.sql(
        f"""
        CREATE TABLE another_table
        (a INT NOT NULL, b INT, PRIMARY KEY (a))
        USING {engine}
        DISTRIBUTED BY (a)
        """
    )
    assert ddl["row_count"] == 1
    table_size = 1000
    response = i1.sql("INSERT INTO another_table VALUES " + (", ".join([f"({i},{i})" for i in range(0, table_size)])))
    assert response["row_count"] == table_size
    # check vshard rebalancing
    cluster.wait_until_instance_has_this_many_active_buckets(i1, 1500)
    cluster.wait_until_instance_has_this_many_active_buckets(i2, 1500)
    res = i1.eval("return box.space.another_table:count()")
    assert math.isclose(res, table_size / 2, abs_tol=200)
    res = i2.eval("return box.space.another_table:count()")
    assert math.isclose(res, table_size / 2, abs_tol=200)
    # check schema correctness
    res = i1.sql("SELECT id, format, distribution FROM _pico_table WHERE name = 'another_table'")
    assert res[0][1] == [
        {"field_type": "integer", "is_nullable": False, "name": "a"},
        {
            "field_type": "unsigned",
            "is_nullable": False,
            "name": "bucket_id",
        },
        {"field_type": "integer", "is_nullable": True, "name": "b"},
    ]
    assert res[0][2] == {"ShardedImplicitly": [["a"], "murmur3", "default"]}
    another_table_id = res[0][0]
    res = i1.sql(f"SELECT parts FROM _pico_index WHERE table_id = {another_table_id}")
    assert res[0][0] == [["a", "integer", None, False, None]]
    res = i1.eval(f"return box.space._space:select({another_table_id})")
    assert res[0][6] == [
        {"is_nullable": False, "name": "a", "type": "integer"},
        {"is_nullable": False, "name": "bucket_id", "type": "unsigned"},
        {"is_nullable": True, "name": "b", "type": "integer"},
    ]
    res = i1.eval(f"return box.space._index:select({another_table_id})")
    assert res[0][5] == [
        {"field": 0, "is_nullable": False, "type": "integer"},
    ]
    assert res[1][5] == [
        {"field": 1, "is_nullable": False, "type": "unsigned"},
    ]

    # check DROP TABLE
    ddl = i1.sql("DROP TABLE sharded_table")
    assert ddl["row_count"] == 1
    res = i1.eval(f"return box.space._space:select({table_id})")
    assert res == []
    res = i1.eval(f"return box.space._index:select({table_id})")


def test_explain_raw(instance: Instance):
    i = instance
    ddl = i.sql(
        """
        CREATE TABLE t (a INT NOT NULL, b INT, PRIMARY KEY (bucket_id, a))
        """
    )
    assert ddl["row_count"] == 1

    statement_list = [
        "SELECT * FROM t WHERE a = 43",
        "UPDATE t SET b = 43 WHERE a = 43",
        "DELETE FROM t WHERE a = 43",
        "SELECT * FROM t WHERE a = 43 LIMIT 1",
        "SELECT * FROM t WHERE a = 43 ORDER BY 1",
        "SELECT * FROM t WHERE a = 43 ORDER BY 1 LIMIT 1",
        "SELECT a FROM t WHERE a = 43 GROUP BY 1",
        "SELECT * FROM t WHERE a = 1 UNION SELECT * FROM t WHERE a = 1 LIMIT 1",
        "SELECT * FROM t WHERE a = 1 UNION SELECT * FROM t WHERE a = 2 ORDER BY 1 LIMIT 1",
        "SELECT MAX(a) FROM t WHERE a = 1",
        "SELECT b, MAX(a) FROM t WHERE a = 1 GROUP BY 1",
        "SELECT a FROM t WHERE a = 1 GROUP BY 1 UNION SELECT a FROM t WHERE a = 1;",
    ]

    for statement in statement_list:
        res = i.sql(f"EXPLAIN(RAW) {statement}")
        assert "SEARCH TABLE t USING PRIMARY KEY (bucket_id=? AND a=?)" in res[5]
