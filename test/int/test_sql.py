import pytest
import re
import uuid

# mypy: disable-error-code="attr-defined"
from tarantool import Datetime as tt_datetime

from conftest import (
    Cluster,
    Instance,
    KeyDef,
    KeyPart,
    ReturnError,
    TarantoolError,
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
            False,
            "query_id",
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

    invalid_meta_msg = re.escape('sbroad: table with name "absent_table" not found')
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
        create table t (a int, primary key (a))
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
    assert data == [[2]]
    data = i1.sql("""select * from t""")
    assert data == [[1], [2], [2000]]
    data = i1.sql(
        """select * from t as t1
           join (select a as a2 from t) as t2
           on t1.a = t2.a2 where t1.a = ?""",
        2,
    )
    assert data == [[2, 2]]


# test is checking create virtual table with type uuid and cast uuid to text#
def test_uuid(
    cluster: Cluster,
):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    t1_id1 = uuid.UUID("7055211c-826d-4da2-921b-7133811239f0")
    t1_id2 = uuid.UUID("5bc3cc1c-1819-4ab7-adfe-ee0fa2c0cde0")
    t2_id1 = uuid.UUID("e4166fc5-e113-46c5-8ae9-970882ca8842")
    t2_id2 = uuid.UUID("6f2ba4c4-0a4c-4d79-86ae-43d4f84b70e1")

    # first table with uuid is creating
    ddl = i1.sql(
        """
        create table t1 (id uuid, primary key (id))
        using memtx
        distributed by (id)
        option (timeout = 3)
    """
    )
    # check the creation of the first table
    assert ddl["row_count"] == 1

    # second table has reference by first table
    ddl = i1.sql(
        """
        create table t2 (id uuid not null,t1_id uuid not null, primary key (id))
        using memtx
        distributed by (id)
        option (timeout = 3)
    """
    )
    # check the creation of the second table
    assert ddl["row_count"] == 1

    # table is filling with uuid
    # start
    data = i1.sql("""insert into t1 values(?);""", t1_id1)
    assert data["row_count"] == 1

    i1.sql("""insert into t1 values(?);""", t1_id2)
    assert data["row_count"] == 1

    data = i1.sql("""insert into t2 values(?,?);""", t2_id1, t1_id1)
    assert data["row_count"] == 1

    data = i1.sql("""insert into t2 values(?,?);""", t2_id2, t1_id2)
    assert data["row_count"] == 1
    # end

    # checking virtual table creation
    data = i1.sql(
        """select * from t1 where id in (select t1_id from t2 where id = (?))""",
        t2_id1,
    )
    assert data == [[t1_id1]]

    # checking cast uuid as text
    data = i1.sql("""select cast(id as Text) from t1""", t1_id1, strip_metadata=False)
    assert data == {
        "metadata": [{"name": "col_1", "type": "string"}],
        "rows": [[str(t1_id2)], [str(t1_id1)]],
    }


def test_select_with_scan(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    cluster.wait_until_instance_has_this_many_active_buckets(i1, 1500)
    cluster.wait_until_instance_has_this_many_active_buckets(i2, 1500)

    ddl = i1.sql(
        """
        create table tmp (value INT primary key)
        using memtx
        distributed by (value)
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    data = i1.sql("""insert into tmp (value) values (123);""")
    assert data["row_count"] == 1

    data = i1.sql("""
                  WITH v(other) AS (VALUES (123))
                    SELECT
                      CASE
                        WHEN v.other IN (SELECT value FROM tmp)
                        THEN 1
                        ELSE 0
                      END
                    FROM v;
                  """)
    assert data == [[1]]

    data = i2.sql("""
                  WITH v(other) AS (VALUES (123))
                    SELECT
                      CASE
                        WHEN v.other IN (SELECT value FROM tmp)
                        THEN 1
                        ELSE 0
                      END
                    FROM v;
                  """)
    assert data == [[1]]


def test_pg_params(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        create table t (a int, b int, primary key (a))
        using memtx
        distributed by (a)
        option (timeout = 3)
        """
    )
    assert ddl["row_count"] == 1

    data = i1.sql("""insert into t values ($1, $1), ($2, $1)""", 1, 2)
    assert data["row_count"] == 2

    data = i1.sql("""select * from t""")
    assert data == [[1, 1], [2, 1]]

    data = i1.sql(
        """
        select b + $1 from t
        group by b + $1
        having sum(a) > $1
    """,
        1,
    )
    assert data == [[2]]

    data = i1.sql(
        """
        select $3, $2, $1, $2, $3 from t
        where a = $1
    """,
        1,
        2,
        3,
    )
    assert data == [[3, 2, 1, 2, 3]]

    with pytest.raises(TarantoolError, match="expected 1 values for parameters, got 0"):
        i1.sql("""SELECT $1::int, 1 AS c1 FROM t ORDER BY c1 DESC""")

    with pytest.raises(TarantoolError, match="expected 1 values for parameters, got 0"):
        i1.sql("""SELECT $1::int as c1 FROM (SELECT 1) ORDER BY 1 DESC""")

    with pytest.raises(TarantoolError, match="expected 1 values for parameters, got 0"):
        i1.sql("""SELECT $1 + 0, 1 as c1 FROM t ORDER BY c1 DESC""")

    with pytest.raises(TarantoolError, match="expected 1 values for parameters, got 0"):
        i1.sql("""SELECT $1::int, 1 as c1 FROM t""")

    with pytest.raises(TarantoolError, match="expected 2 values for parameters, got 0"):
        i1.sql("""select coalesce($1, $2::int) from (values(1)) order by 1""")

    with pytest.raises(TarantoolError, match="expected 1 values for parameters, got 0"):
        i1.sql("""select max($1 + 1) from (select 1)""")

    with pytest.raises(TarantoolError, match="expected 1 values for parameters, got 0"):
        i1.sql("""SELECT MAX($1 * 1.0)""")

    with pytest.raises(TarantoolError, match="expected 1 values for parameters, got 0"):
        i1.sql("""select max(coalesce($1, 1)) from (select 1)""")

    # Test that using a numeric literal as an ordinal reference in GROUP BY
    # which ends up referring to an aggregate expression max(a) is not allowed.
    with pytest.raises(TarantoolError, match="aggregate functions are not allowed in GROUP BY"):
        i1.sql("""select max(a), b from (select 1 as a, 2 as b) group by 1""")

    # Test that a parameter placeholder used in the GROUP BY clause is treated as an expression,
    # not as an ordinal reference to a column in the SELECT list.
    data = i1.sql("""select max(a), b from (select 1 as a, 2 as b) group by $1""", 1)
    assert data == [[1, 2]]

    data = i1.sql("""select max(a), b from (select 1 as a, 2 as b) group by 2""")
    assert data == [[1, 2]]

    data = i1.sql("""select max(coalesce($1, $2 + 1)) from (select 1)""", 1, 2)
    assert data == [[1]]

    data = i1.sql("""select * from (select 1 as a) group by $1""", 1)
    assert data == [[1]]

    data = i1.sql("""with t(a,b,c) as (select 1, 2, 3) select coalesce($1, $2) from t order by 1""", 1, 2)
    assert data == [[1]]

    with pytest.raises(TarantoolError, match="invalid parameters usage"):
        i1.sql(
            """
            select $1, ? from t
            """,
            1,
            2,
        )

    with pytest.raises(TarantoolError, match="cannot be parsed as unsigned param number"):
        i1.sql("select $18446744073709551616", 1)

    with pytest.raises(TarantoolError, match="expected 3333 values for parameters, got 2"):
        i1.sql("select $1::int, $2::int, $3333::int", 1, 2)

    with pytest.raises(TarantoolError, match="expected 5 values for parameters, got 1"):
        i1.sql(
            """
        with
        define_parameter_types as (select $1 = $2 = $3 = $4 = $5),
        cte as (select $1, $2, $2, $3, $4, $5)
        select $1, * from cte
        """,
            True,
        )

    with pytest.raises(TarantoolError, match="expected 5 values for parameters, got 2"):
        i1.sql(
            """
        with
        define_parameter_types as (select $1::bool = $2 = $3 = $4 = $5),
        cte as (select $1, $2, $2, $3, $4, $5)
        select $1, * from cte
        """,
            True,
            True,
        )

    with pytest.raises(TarantoolError, match="expected 3 values for parameters, got 2"):
        i1.sql(
            """
        with
        define_parameter_types as (select $1 = $2 = $3),
        cte as (select $1, $1, $2, $2, $1, $3)
        select $1, (select (select $1)), * from cte
        """,
            True,
            True,
        )

    data = i1.sql("""select $1, *, * from (select $1, (select $1))""", True)
    assert data == [[True] * 5]

    with pytest.raises(TarantoolError, match="expected 2 values for parameters, got 1"):
        i1.sql(
            """
        select $1, *, * from (select $1, (select $2::bool))
        """,
            True,
        )

    data = i1.sql("""select * from (select $1 union select ($1 * 2))""", 1, strip_metadata=False)
    assert data["metadata"] == [{"name": "col_1", "type": "unsigned"}]
    assert data["rows"] == [[1], [2]]

    data = i1.sql("""select (select $1::int) + 1""", 1, strip_metadata=False)
    assert data["metadata"] == [{"name": "col_1", "type": "integer"}]
    assert data["rows"] == [[2]]

    data = i1.sql("""select * from (select * from (select $1))""", "picodata", strip_metadata=False)
    assert data["metadata"] == [{"name": "col_1", "type": "string"}]

    data = i1.sql("""select * from (select * from (select $1))""", 1, strip_metadata=False)
    assert data["metadata"] == [{"name": "col_1", "type": "unsigned"}]

    data = i1.sql("""select * from (select * from (select $1))""", -1, strip_metadata=False)
    assert data["metadata"] == [{"name": "col_1", "type": "integer"}]

    # first table is created
    ddl = i1.sql(
        """
        create table test_params_1 (a unsigned, b unsigned,
        c unsigned, d unsigned, primary key (b))
        using memtx
        distributed by (a, b)
        option (timeout = 3)
    """
    )
    # check the creation of the first table
    assert ddl["row_count"] == 1

    # second table is created
    ddl = i1.sql(
        """
        create table test_params_2 (a string, b integer, primary key (a, b))
        using memtx
        distributed by (a, b)
        option (timeout = 3)
    """
    )
    # check the creation of the second table
    assert ddl["row_count"] == 1

    with pytest.raises(TarantoolError, match="Unable to unify inconsistent types: Unsigned and String"):
        i1.sql(
            """
        SELECT "a", "b" FROM "test_params_1"
        WHERE "a" <= ? and "b" >= ? and "b" <= ?
        AND ("a", "b") NOT IN (
        SELECT "a", "b" FROM "test_params_1"
        WHERE "b" >= ?
        UNION ALL
        SELECT "a", "b" FROM "test_params_2"
        WHERE "a" <= ?)
        """,
            1,
            2,
            3,
            4,
            "5",
        )

    # third table is created
    data = i1.sql(
        """
        CREATE TABLE test_params_3(x INTEGER PRIMARY KEY, y TEXT)
        using memtx
        distributed by (x)
        option (timeout = 3)
    """
    )
    # check the creation of the third table
    assert data["row_count"] == 1

    data = i1.sql("""
                 INSERT INTO test_params_3 VALUES (1, 'aaa'), (2, 'ccc'), (3, 'bbb');
                 """)
    assert data["row_count"] == 3

    data = i1.sql(
        """
        select sum(x) OVER (ROWS BETWEEN 1 preceding and 1 FOLLOWING) from test_params_3
    """,
        strip_metadata=False,
    )
    assert data["metadata"] == [{"name": "col_1", "type": "decimal"}]
    assert data["rows"] == [[3], [6], [5]]

    data = i1.sql(
        """
        select sum(x) OVER (rows $1 PRECEDING) from test_params_3
    """,
        2,
        strip_metadata=False,
    )
    assert data["metadata"] == [{"name": "col_1", "type": "decimal"}]
    assert data["rows"] == [[1], [3], [6]]

    data = i1.sql(
        """
        select avg(b) OVER (ROWS BETWEEN $1 preceding and $1 FOLLOWING) from (select 1::int as b)
    """,
        1,
        strip_metadata=False,
    )
    assert data["metadata"] == [{"name": "col_1", "type": "decimal"}]
    assert data["rows"] == [[1]]

    # Test complex PARTITION BY with expressions
    data = i1.sql(
        """
        SELECT sum(x) OVER (
                   PARTITION BY CASE WHEN y < 'bbb' THEN $1 ELSE $2 END
                   ORDER BY x DESC
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) as running_sum
        FROM test_params_3
        ORDER BY 1
        """,
        1,
        2,
        strip_metadata=False,
    )
    assert data["metadata"] == [{"name": "running_sum", "type": "decimal"}]
    assert data["rows"] == [[1], [3], [5]]

    # Test ORDER BY with complex expressions and FILTER
    data = i1.sql(
        """
        SELECT count(*) FILTER (WHERE x + $1 > $2) OVER
        ( ORDER BY y DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING )
        as filtered_count FROM test_params_3 group by x
        """,
        1,
        2,
        strip_metadata=False,
    )
    assert data["metadata"] == [{"name": "filtered_count", "type": "unsigned"}]
    assert data["rows"] == [[2], [1], [0]]

    # Test nested window functions with complex FILTER and ORDER BY
    data = i1.sql(
        """
        WITH windowed AS
        (SELECT count(y) OVER (ORDER BY x) as rn,
        (sum(x) FILTER (WHERE y != 'bbb')
        OVER ( ORDER BY x ROWS BETWEEN $1 PRECEDING AND $1 FOLLOWING))::int as filtered_sum FROM test_params_3)
        SELECT sum(filtered_sum) OVER ( ORDER BY filtered_sum DESC) as nested_sum FROM windowed
        """,
        1,
        strip_metadata=False,
    )
    assert data["metadata"] == [{"name": "nested_sum", "type": "decimal"}]
    assert data["rows"] == [[6], [6], [8]]

    # Test PARTITION BY with multiple expressions and FILTER
    data = i1.sql(
        """
        SELECT avg(x) FILTER (WHERE x > 1) OVER (
                   PARTITION BY 
                       CASE WHEN x - 2 = 0 THEN 'even' ELSE 'odd' END,
                       y LIKE '%a%'
                   ORDER BY x
                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
               ) as complex_avg
        FROM test_params_3
        ORDER BY 1
        """,
        strip_metadata=False,
    )
    assert data["metadata"] == [{"name": "complex_avg", "type": "decimal"}]
    assert data["rows"] == [[None], [2], [3]]


def test_window_functions(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    # Create test table
    i1.sql("""
        CREATE TABLE t7 (x INTEGER PRIMARY KEY, y DECIMAL)
        using memtx
        distributed by (x)
        option (timeout = 3)
    """)

    # Insert test data
    i1.sql("""INSERT INTO t7 VALUES (1, 1.5), (2, 2.5)""")

    # Test integer + max(integer) -> integer
    data = i1.sql("""SELECT 1 + max(x) OVER (ORDER BY x) FROM t7""", strip_metadata=False)
    assert data["metadata"] == [{"name": "col_1", "type": "integer"}]
    assert sorted(data["rows"]) == [[2], [3]]

    # Test integer + max(decimal) -> decimal
    data = i1.sql("""SELECT 1 + max(y) OVER (ORDER BY x) FROM t7""", strip_metadata=False)
    assert data["metadata"] == [{"name": "col_1", "type": "decimal"}]
    assert sorted(data["rows"]) == [[2.5], [3.5]]

    # Test integer + avg(integer) -> decimal
    data = i1.sql("""SELECT 1 + avg(x) OVER (ORDER BY x) FROM t7""", strip_metadata=False)
    assert data["metadata"] == [{"name": "col_1", "type": "decimal"}]
    assert sorted(data["rows"]) == [[2], [2]]

    # Test row_number() -> integer
    data = i1.sql("""SELECT 2 + row_number() OVER (ORDER BY x) FROM t7""", strip_metadata=False)
    assert data["metadata"] == [{"name": "col_1", "type": "integer"}]
    assert sorted(data["rows"]) == [[3], [4]]

    # Test count() -> unsigned
    data = i1.sql("""SELECT count(*) OVER (ORDER BY x) FROM t7""", strip_metadata=False)
    assert data["metadata"] == [{"name": "col_1", "type": "unsigned"}]
    assert sorted(data["rows"]) == [[1], [2]]

    # Test min(decimal) -> decimal
    data = i1.sql("""SELECT min(y) OVER (ORDER BY x) FROM t7""", strip_metadata=False)
    assert data["metadata"] == [{"name": "col_1", "type": "decimal"}]
    assert sorted(data["rows"]) == [[1.5], [1.5]]

    # Test sum(integer) -> decimal
    data = i1.sql("""SELECT sum(x) OVER (ORDER BY x) FROM t7""", strip_metadata=False)
    assert data["metadata"] == [{"name": "col_1", "type": "decimal"}]
    assert sorted(data["rows"]) == [[1], [3]]

    # Test total(integer) -> double
    data = i1.sql("""SELECT total(x) OVER (ORDER BY x) FROM t7""", strip_metadata=False)
    assert data["metadata"] == [{"name": "col_1", "type": "double"}]
    assert sorted(data["rows"]) == [[1.0], [3.0]]


def test_read_from_global_tables(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    ddl = i1.sql(
        """
        create table "global_t" ("id" unsigned not null, primary key("id"))
        using memtx
        distributed globally
        """
    )
    assert ddl["row_count"] == 1

    dml = i1.sql(
        """
        insert into "global_t" values (1)
        """
    )
    assert dml["row_count"] == 1

    data = i2.sql(
        """
        select * from "global_t"
        """,
    )
    assert data == [[1]]

    data = i1.sql(
        """
        select * from "_pico_table" where "name" = 'global_t'
        """,
    )
    assert len(data) == 1


def test_read_from_system_tables(cluster: Cluster):
    instance_count = 2
    cluster.deploy(instance_count=instance_count)
    i1, _ = cluster.instances
    # Check we can read everything from the table
    data = i1.sql(
        """
        SELECT * FROM "_pico_db_config" ORDER BY "key"
        """,
        strip_metadata=False,
    )
    assert data["metadata"] == [
        {"name": "key", "type": "string"},
        {"name": "scope", "type": "string"},
        {"name": "value", "type": "any"},
    ]
    # Ignore values for the sake of stability
    keys = [row[0] for row in data["rows"]]
    assert keys == [
        "auth_login_attempt_max",
        "auth_password_enforce_digits",
        "auth_password_enforce_lowercase",
        "auth_password_enforce_specialchars",
        "auth_password_enforce_uppercase",
        "auth_password_length_min",
        "governor_auto_offline_timeout",
        "governor_common_rpc_timeout",
        "governor_plugin_rpc_timeout",
        "governor_raft_op_timeout",
        "iproto_net_msg_max",
        "memtx_checkpoint_count",
        "memtx_checkpoint_interval",
        "pg_portal_max",
        "pg_statement_max",
        "raft_snapshot_chunk_size_max",
        "raft_snapshot_read_view_close_timeout",
        "raft_wal_count_max",
        "raft_wal_size_max",
        "shredding",
        "sql_motion_row_max",
        "sql_storage_cache_count_max",
        "sql_storage_cache_size_max",
        "sql_vdbe_opcode_max",
    ]

    data = i1.sql(
        """
        SELECT * FROM "_pico_property" ORDER BY "key"
        """,
        strip_metadata=False,
    )
    assert data["metadata"] == [
        {"name": "key", "type": "string"},
        {"name": "value", "type": "any"},
    ]
    # Ignore values for the sake of stability
    keys = [row[0] for row in data["rows"]]
    assert keys == [
        "cluster_version",
        "global_schema_version",
        "next_schema_version",
        "system_catalog_version",
    ]

    data = i1.sql(
        """
        select * from "_pico_instance"
        """,
        strip_metadata=False,
    )
    assert data["metadata"] == [
        {"name": "name", "type": "string"},
        {"name": "uuid", "type": "string"},
        {"name": "raft_id", "type": "unsigned"},
        {"name": "replicaset_name", "type": "string"},
        {"name": "replicaset_uuid", "type": "string"},
        {"name": "current_state", "type": "array"},
        {"name": "target_state", "type": "array"},
        {"name": "failure_domain", "type": "map"},
        {"name": "tier", "type": "string"},
        {"name": "picodata_version", "type": "string"},
    ]
    assert len(data["rows"]) == instance_count


# FIXME: Flaky test.
#        As one of the possible reasons flakiness is caused by
#        inconsistent bucket reads (read just after update returns
#        extra row).
#        Should be fixed after we move on new vshard methods as a part
#        of the following issue:
#        https://git.picodata.io/picodata/picodata/sbroad/-/issues/531.
#        Picodata issue to resolve:
#        https://git.picodata.io/picodata/picodata/picodata/-/issues/1013#note_106684
@pytest.mark.xfail
def test_dml_on_global_tbls(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    # after inserting/deleting from sharded table
    # wait until all buckets are balanced to reduce
    # flakyness
    def wait_balanced():
        for i in [i1, i2]:
            cluster.wait_until_instance_has_this_many_active_buckets(i, 1500)

    ddl = i1.sql(
        """
        create table t (x int not null, y int not null, primary key (x))
        using memtx
        distributed by (y)
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    ddl = i1.sql(
        """
        create table global_t (id int not null, a int not null, primary key (id))
        using memtx
        distributed globally
        option (timeout = 3)
        """
    )
    assert ddl["row_count"] == 1

    data = i2.sql("insert into t values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
    assert data["row_count"] == 5
    wait_balanced()

    data = i2.sql("insert into global_t values (1, 1), (2, 2)")
    assert data["row_count"] == 2

    # insert into global from global
    data = i2.sql("insert into global_t select id + 2, a from global_t")
    assert data["row_count"] == 2

    # empty insert
    data = i2.sql(
        """
        insert into global_t select id + 2, a from global_t
        where false
        """,
    )
    assert data["row_count"] == 0

    data = i2.sql("select * from global_t")
    assert data == [[1, 1], [2, 2], [3, 1], [4, 2]]
    i1.raft_read_index()
    data = i1.sql("select * from global_t")
    assert data == [[1, 1], [2, 2], [3, 1], [4, 2]]

    # check update
    data = i2.sql("update global_t set a = 1")
    assert data["row_count"] == 4
    data = i2.sql("select * from global_t")
    assert data == [[1, 1], [2, 1], [3, 1], [4, 1]]
    i1.raft_read_index()
    data = i1.sql("select * from global_t")
    assert data == [[1, 1], [2, 1], [3, 1], [4, 1]]

    # check update global from global table
    data = i2.sql(
        """
        update global_t
        set a = u from (select a + 1 as u, id as v from global_t) as s
        where id = v
        """
    )
    assert data["row_count"] == 4
    data = i2.sql("select * from global_t")
    assert data == [[1, 2], [2, 2], [3, 2], [4, 2]]
    i1.raft_read_index()
    data = i1.sql("select * from global_t")
    assert data == [[1, 2], [2, 2], [3, 2], [4, 2]]

    # empty update
    data = i2.sql(
        """
        update global_t
        set a = 20
        where false
        """
    )
    assert data["row_count"] == 0

    # check update global from sharded table
    data = i2.retriable_sql(
        """
        update global_t
        set a = y from t
        where id = x
        """,
        fatal_predicate=r"Duplicate key exists in unique index",
    )
    assert data["row_count"] == 4
    data = i2.sql("select * from global_t")
    assert data == [[1, 1], [2, 2], [3, 3], [4, 4]]
    i1.raft_read_index()
    data = i1.sql("select * from global_t")
    assert data == [[1, 1], [2, 2], [3, 3], [4, 4]]

    # check delete
    data = i2.sql("delete from global_t")
    assert data["row_count"] == 4

    # test reading subtree with motion
    try:
        data = i1.retriable_sql(
            "insert into global_t select count(*), 1 from t",
            fatal_predicate=r"Duplicate key exists in unique index",
        )
        assert data["row_count"] == 1
    except TarantoolError as e:
        assert re.search(r"Duplicate key exists in unique index", str(e))

    data = i1.sql("select * from global_t")
    assert data == [[5, 1]]
    i2.raft_read_index()
    data = i2.sql("select * from global_t")
    assert data == [[5, 1]]

    # test explain
    lines = i1.sql("explain insert into global_t select * from t")
    expected_explain = """insert "global_t" on conflict: fail
    motion [policy: full]
        projection ("t"."x"::integer -> "x", "t"."y"::integer -> "y")
            scan "t"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]"""
    assert "\n".join(lines) == expected_explain

    # empty delete
    data = i2.sql("delete from global_t")
    assert data["row_count"] == 1
    data = i2.sql("delete from global_t where false")
    assert data["row_count"] == 0

    data = i2.sql("select * from global_t")
    assert data == []
    i1.raft_read_index()
    data = i1.sql("select * from global_t")
    assert data == []

    # insert from sharded table
    try:
        data = i2.retriable_sql(
            "insert into global_t select x, y from t",
            fatal_predicate=r"Duplicate key exists in unique index",
        )
        assert data["row_count"] == 5
    except TarantoolError as e:
        assert re.search(r"Duplicate key exists in unique index", str(e))

    data = i2.sql("select * from global_t")
    assert data == [[1, 1], [2, 2], [3, 3], [4, 4], [5, 5]]
    i1.raft_read_index()
    data = i1.sql("select * from global_t")
    assert data == [[1, 1], [2, 2], [3, 3], [4, 4], [5, 5]]

    # insert into sharded table from global table
    data = i2.sql("insert into t select id + 5, a + 5 from global_t where id = 1")
    assert data["row_count"] == 1
    wait_balanced()
    i1.raft_read_index()
    data = i1.retriable_sql("select * from t")
    assert sorted(data) == [[1, 1], [2, 2], [3, 3], [4, 4], [5, 5], [6, 6]]

    # update sharded table from global table
    data = i2.sql("update t set y = a * a from global_t where id = x")
    assert data["row_count"] == 5
    i1.raft_read_index()
    data = i1.retriable_sql("select * from t")
    assert sorted(data) == [[1, 1], [2, 4], [3, 9], [4, 16], [5, 25], [6, 6]]

    # delete sharded table using global table in predicate
    data = i2.sql("delete from t where x in (select id from global_t)")
    assert data["row_count"] == 5
    wait_balanced()
    i1.raft_read_index()
    data = i1.retriable_sql(
        "select * from t",
        retry_timeout=60,
        timeout=8,
    )
    assert sorted(data) == [[6, 6]]

    # test user with write permession can do global dml
    user = "user"
    password = "PaSSW0RD"
    acl = i1.sql(f"create user {user} with password '{password}' using chap-sha1")
    assert acl["row_count"] == 1
    # check we can't write yet
    with pytest.raises(
        TarantoolError,
        match=rf"Write access to space 'global_t' is denied for user '{user}'",
    ):
        i1.sql(
            "insert into global_t values (300, 300)",
            user=user,
            password=password,
        )
    # * Grant WRITE to user.
    acl = i1.sql(f""" grant write on table global_t to {user}""", sudo=True)
    assert acl["row_count"] == 1
    data = i1.sql("insert into global_t values (100, 100)", user=user, password=password)
    assert data["row_count"] == 1


def test_datetime(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        create table t (a int, d datetime not null, primary key (a))
        using memtx
        distributed by (a)
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    data = i1.sql(
        """
        insert into t select cast("COLUMN_5" as int), to_date("COLUMN_6", '%Y %d %m') from (values
            (1, '2010 10 10'),
            (2, '2020 20 02'),
            (3, '2010 10 10')
        )
        """
    )
    assert data["row_count"] == 3

    data = i1.sql("""select d from t""")
    assert data == [
        [tt_datetime(year=2010, month=10, day=10)],
        [tt_datetime(year=2020, month=2, day=20)],
        [tt_datetime(year=2010, month=10, day=10)],
    ]

    # invalid format
    # FIXME: better error message
    with pytest.raises(TarantoolError, match="could not parse"):
        i1.sql("""select to_date('2020/20/20', '%Y/%d/%m') from t where a = 1""")

    # check we can group on datetime column
    data = i1.sql("""select d from t group by d""")
    assert data == [
        [tt_datetime(year=2010, month=10, day=10)],
        [tt_datetime(year=2020, month=2, day=20)],
    ]

    # check we can compare on datetime column
    data = i1.sql("""select d from t where d < to_date('2015/01/01', '%Y/%m/%d')""")
    assert data == [
        [tt_datetime(year=2010, month=10, day=10)],
        [tt_datetime(year=2010, month=10, day=10)],
    ]

    data = i1.sql("""select d from t where d = to_date('2020/02/20', '%Y/%m/%d')""")
    assert data == [
        [tt_datetime(year=2020, month=2, day=20)],
    ]

    data = i1.sql("""select d from t where d > to_date('2010/12/10', '%Y/%m/%d')""")
    assert data == [
        [tt_datetime(year=2020, month=2, day=20)],
    ]

    # without format argument
    # TODO: tarantool does not allow to skip arguments in sql of a stored procedure,
    # but passing '' looks awful, maybe there is a better approach?
    data = i1.sql("""select to_date('1970-01-01T10:10:10 -3', '') from t where a = 1""")
    assert data == [[tt_datetime(year=1970, month=1, day=1, tzoffset=-180)]]

    # check we can use arbitrary expressions returning string inside to_date
    data = i1.sql(
        """select to_date(cast('1970-01-01T10:10:10 -3' as string), '' || '')
                  from t where a = 1"""
    )
    assert data == [[tt_datetime(year=1970, month=1, day=1, tzoffset=-180)]]

    # check we can create table sharded by datetime column
    ddl = i1.sql(
        """
        create table t2 (a int, d datetime not null, primary key (a))
        using memtx
        distributed by (d)
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    # check we can insert min/max date
    data = i1.sql(
        """
        insert into t2 select cast("COLUMN_3" as int), to_date("COLUMN_4", '') from (values
            (1, '-9999-12-31T23:59:59Z'),
            (2, '9999-12-31T23:59:59Z')
        )
        """
    )
    assert data["row_count"] == 2

    # check we can't insert out of limits date
    # FIXME: https://git.picodata.io/picodata/picodata/sbroad/-/issues/639
    with pytest.raises(TarantoolError, match="decode bytes into inner format"):
        i1.sql(
            """
            insert into t2 select cast("COLUMN_1" as int), to_date("COLUMN_2", '') from (values
                (1, '-10000-01-01T00:00:00Z')
            )
            """
        )

    with pytest.raises(TarantoolError, match="decode bytes into inner format"):
        i1.sql(
            """
            insert into t2 select cast("COLUMN_1" as int), to_date("COLUMN_2", '') from (values
                (2, '10000-01-01T00:00:00Z')
            )
            """
        )
    # test to_char builtin function
    data = i1.sql("""select to_char(d, '%Y-%m-%d') from t""")
    assert sorted(data, key=lambda e: e[0]) == [
        ["2010-10-10"],
        ["2010-10-10"],
        ["2020-02-20"],
    ]

    # check to_char with timezone specified
    data = i1.sql(
        """select to_char(to_date("COLUMN_1", ''), '%Y-%m-%d')
                  from (values (('1970-01-01T10:10:10 -3')))"""
    )
    assert sorted(data, key=lambda e: e[0]) == [["1970-01-01"]]

    data = i1.sql(
        """select to_char(to_date("COLUMN_1", ''), '%Y-%Y-%Y')
                  from (values (('1970-01-01T10:10:10 -3')))"""
    )
    assert sorted(data, key=lambda e: e[0]) == [["1970-1970-1970"]]

    data = i1.sql(
        """select to_char(to_date("COLUMN_1", ''), '%Y-%m-%d %z')
                  from (values (('1970-01-01T10:10:10 -3')))"""
    )
    assert sorted(data, key=lambda e: e[0]) == [["1970-01-01 -0300"]]


def test_subqueries_on_global_tbls(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        create table g (a int, b int not null, primary key (a))
        using memtx
        distributed globally
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    dml = i1.sql(
        """
        insert into g values (1, 1), (2, 2),
        (3, 3), (4, 4), (5, 5)
        """
    )
    assert dml["row_count"] == 5

    ddl = i1.sql(
        """
        create table s (c int, primary key (c))
        using memtx
        distributed by (c)
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1
    data = i1.sql("""insert into s values (1), (2), (3), (10);""")
    assert data["row_count"] == 4

    # TODO: remove retries and add more instances when
    # https://git.picodata.io/picodata/picodata/sbroad/-/issues/542
    # is done.
    data = i1.retriable_sql(
        """
        select b from g
        where b in (select c from s where c in (2, 10))
        """
    )
    assert data == [[2]]

    data = i1.retriable_sql(
        """
        select b from g
        where b in (select sum(c) from s)
        """
    )
    assert len(data) == 0

    data = i1.retriable_sql(
        """
        select b from g
        where b in (select c * 5 from s)
        """,
    )
    assert data == [[5]]

    # first subquery selects [1], [2], [3]
    # second subquery must add additional [4] tuple
    data = i1.retriable_sql(
        """
        select b from g
        where b in (select c from s) or a in (select count(*) from s)
        """,
        timeout=2,
    )
    assert data == [[1], [2], [3], [4]]

    data = i1.retriable_sql(
        """
        select b from g
        where b in (select c from s) and a in (select count(*) from s)
        """,
        timeout=2,
    )
    assert len(data) == 0

    data = i1.retriable_sql(
        """
        select c from s inner join
        (select c as c1 from s)
        on c = c1 + 3 and c in (select a from g)
        """,
        timeout=2,
    )
    assert data == []

    # Full join because of 'OR'
    data = i1.retriable_sql(
        """
        select min(c) from s inner join
        (select c as c1 from s)
        on c = c1 + 3 or c in (select a from g)
        """,
        timeout=2,
    )
    assert data == [[1]]

    data = i1.retriable_sql(
        """
        select a from g
        where b in (select c from s where c = 1) or
        b in (select c from s where c = 3)
        """,
        timeout=2,
    )
    assert data == [[1], [3]]

    data = i1.retriable_sql(
        """
        select a from g
        where b in (select c from s where c = 1) or
        b in (select c from s where c = 3) and
        a < (select sum(c) from s)
        """,
        timeout=2,
    )
    assert data == [[1], [3]]


def test_aggregates_on_global_tbl(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        create table g (a int, b int not null, primary key (a))
        using memtx
        distributed globally
        option (timeout = 3)
        """
    )
    assert ddl["row_count"] == 1

    dml = i1.sql(
        """
        insert into g values (1, 1), (2, 2), (3, 1),
        (4, 1), (5, 2)
        """
    )
    assert dml["row_count"] == 5

    data = i1.sql(
        """
        select count(*), min(b), max(b), min(b) + max(b) from g
        """
    )
    assert data == [[5, 1, 2, 3]]

    data = i1.sql(
        """
        select b*b, sum(a + 1) from g
        group by b*b
        """
    )
    assert data == [[1, 11], [4, 9]]

    data = i1.sql(
        """
        select b*b, sum(a + 1) from g
        group by b*b
        having count(a) > 2
        """
    )
    assert data == [[1, 11]]


def test_join_with_global_tbls(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        create table g (a int, b int not null, primary key (a))
        using memtx
        distributed globally
        option (timeout = 3)
        """
    )
    assert ddl["row_count"] == 1

    dml = i1.sql(
        """
        insert into g values (1, 1), (2, 1), (3, 3)
        """
    )
    assert dml["row_count"] == 3

    ddl = i1.sql(
        """
        create table s (c int, primary key (c))
        using memtx
        distributed by (c)
        option (timeout = 3)
        """
    )
    assert ddl["row_count"] == 1
    data = i1.sql("""insert into s values (1), (2), (3), (4), (5);""")
    assert data["row_count"] == 5

    expected_rows = [[1], [1], [3]]

    data = i1.retriable_sql(
        """
        select b from g
        join s on g.a = s.c
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda e: e[0]) == expected_rows

    data = i1.retriable_sql(
        """
        select b from s
        join g on g.a = s.c
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda e: e[0]) == expected_rows

    data = i1.retriable_sql(
        """
        select c from s
        join g on 1 = 1 and
        c in (select a*a from g)
        group by c
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda e: e[0]) == [[1], [4]]

    data = i1.retriable_sql(
        """
        select c, cast(sum(a) as int) from s
        left join g on 1 = 1 and
        c in (select a*a from g)
        where c < 4
        group by c
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda e: e[0]) == [
        [1, 6],
        [2, None],
        [3, None],
    ]

    data = i1.retriable_sql(
        """
        select c, b from
        (select c*c as c from s)
        left join g on c = b
        where c < 5
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda e: e[0]) == [[1, 1], [1, 1], [4, None]]

    data = i1.retriable_sql(
        """
        select c, b from
        (select c*c as c from s)
        inner join g on c = b
        where c < 5
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda e: e[0]) == [[1, 1], [1, 1]]

    data = i1.retriable_sql(
        """
        select c, a from
        (select count(*) as c from s)
        left join g on c = a + 2
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda e: e[0]) == [[5, 3]]

    data = i1.retriable_sql(
        """
        select b, c from (select b + 3 as b from g)
        left join s on b = c
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda e: e[0]) == [[4, 4], [4, 4], [6, None]]

    data = i1.retriable_sql(
        """
        select b, c from g
        left join
        (select c*c as c from s where c > 3)
        on b = c
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda e: e[0]) == [
        [1, None],
        [1, None],
        [3, None],
    ]

    data = i1.retriable_sql(
        """
        select b, c from g
        left join
        (select c*c as c from s where c < 3)
        on b = c
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda e: e[0]) == [[1, 1], [1, 1], [3, None]]

    data = i1.retriable_sql(
        """
        select a, b, c from (
            select a, b from g
            inner join (select a + 2 as u from g)
            on a = u
        )
        left join
        (select c + 1 as c from s where c = 2)
        on b = c
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda e: e[0]) == [[3, 3, 3]]


def test_union_all_on_global_tbls(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        create table g (a int, b int not null, primary key (a))
        using memtx
        distributed globally
        option (timeout = 3)
        """
    )
    assert ddl["row_count"] == 1

    dml = i1.sql(
        """
        insert into g values (1, 1), (2, 2), (3, 2)
        """
    )
    assert dml["row_count"] == 3

    ddl = i1.sql(
        """
        create table s (c int, d int not null, primary key (c))
        using memtx
        distributed by (c)
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1
    data = i1.sql("""insert into s values (1, 2), (2, 2), (3, 2);""")
    assert data["row_count"] == 3

    expected = [[1], [2], [2], [2], [2], [2]]

    data = i1.retriable_sql(
        """
        select b from g
        union all
        select d from s
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == expected

    data = i1.retriable_sql(
        """
        select d from s
        union all
        select b from g
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == expected

    data = i1.retriable_sql(
        """
        select b from g
        union all
        select a from g
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == [
        [1],
        [1],
        [2],
        [2],
        [2],
        [3],
    ]

    expected = [[1], [1], [2], [2], [3], [3]]

    data = i1.retriable_sql(
        """
        select a from g
        union all
        select c from s
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == expected

    data = i1.retriable_sql(
        """
        select c from s
        union all
        select a from g
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == expected

    expected = [[1], [2], [3], [6]]

    data = i1.retriable_sql(
        """
        select sum(c) from s
        union all
        select a from g
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == expected

    data = i1.retriable_sql(
        """
        select a from g
        union all
        select sum(c) from s
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == expected

    # some arbitrary queries

    data = i1.retriable_sql(
        """
        select a from g
        where a = 2
        union all
        select d from s
        group by d
        union all
        select a from g
        where b = 1
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == [[1], [2], [2]]

    data = i1.retriable_sql(
        """
        select a from g
        where a in (select d from s)
        union all
        select c from s
        where c = 3
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == [[2], [3]]

    data = i1.retriable_sql(
        """
        select a, b from g
        where a in (select d from s)
        union all
        select d, sum(u) from s
        inner join (select c as u from s)
        on d = u or u = 1
        group by d
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == [[2, 2], [2, 9]]


def test_union_on_global_tbls(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        create table g (a int, b int not null, primary key (a))
        using memtx
        distributed globally
        option (timeout = 3)
        """
    )
    assert ddl["row_count"] == 1

    dml = i1.sql(
        """
        insert into g values (1, 1), (2, 2), (3, 2)
        """
    )
    assert dml["row_count"] == 3

    ddl = i1.sql(
        """
        create table s (c int, d int not null, primary key (c))
        using memtx
        distributed by (c)
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1
    data = i1.sql("""insert into s values (1, 2), (2, 2), (3, 2);""")
    assert data["row_count"] == 3

    expected = [[1], [2]]

    data = i1.retriable_sql(
        """
        select b from g
        union
        select d from s
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == expected

    data = i1.retriable_sql(
        """
        select d from s
        union
        select b from g
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == expected

    data = i1.retriable_sql(
        """
        select b from g
        union
        select a from g
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == [
        [1],
        [2],
        [3],
    ]

    expected = [[1], [2], [3]]

    data = i1.retriable_sql(
        """
        select a from g
        union
        select c from s
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == expected

    data = i1.retriable_sql(
        """
        select c from s
        union
        select a from g
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == expected

    expected = [[1], [2], [3]]

    data = i1.retriable_sql(
        """
        select sum(c) - 3 from s
        union
        select a from g
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == expected

    data = i1.retriable_sql(
        """
        select a from g
        union
        select sum(c) - 3 from s
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == expected

    data = i1.retriable_sql(
        """
        select a from g
        where a = 2
        union
        select d from s
        group by d
        union
        select a from g
        where b = 1
        except
        select null from g
        where false
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == [[1], [2]]


def test_trim(instance: Instance):
    instance.sql(
        """
        create table t (s string, primary key (s))
        using memtx
        distributed by (s)
        """
    )

    instance.sql(""" insert into t values (' aabb ') """)

    # basic trim test
    data = instance.sql(""" select trim(s) from t """)
    assert data[0] == ["aabb"]

    # trim inside trim
    data = instance.sql(""" select trim(trim(s)) from t """)
    assert data[0] == ["aabb"]

    # trim with removal chars
    data = instance.sql(""" select trim('a' from trim(s)) from t """)
    assert data[0] == ["bb"]

    data = instance.sql(""" select trim(trim(s) from trim(s)) from t """)
    assert data[0] == [""]

    # trim with modifiers
    data = instance.sql(""" select trim(leading 'a' from trim(s)) from t """)
    assert data[0] == ["bb"]

    data = instance.sql(""" select trim(trailing 'b' from trim(s)) from t """)
    assert data[0] == ["aa"]

    data = instance.sql(""" select trim(both 'ab' from trim(s)) from t """)
    assert data[0] == [""]


def test_substr(instance: Instance):
    instance.sql(
        """
        create table t (s string, primary key (s))
        using memtx
        distributed by (s)
        """
    )

    instance.sql(""" insert into t values ('123456789') """)

    # basic substr
    data = instance.sql(""" select substr(s, 1, 5) from t """)
    assert data[0] == ["12345"]

    # substr with 0 count
    data = instance.sql(""" select substr(s, 3) from t """)
    assert data[0] == ["3456789"]

    # nested substr
    data = instance.sql(""" select substr(substr(s, 1, 5), 2, 3) from t """)
    assert data[0] == ["234"]

    # substr with 0 characters
    data = instance.sql(""" select substr(s, 3, 0) from t """)
    assert data[0] == [""]


def test_substring(instance: Instance):
    # negative length
    with pytest.raises(TarantoolError, match="Length parameter in substring cannot be negative"):
        instance.sql("SELECT SUBSTRING('string' FROM 2 FOR -1)")

    with pytest.raises(TarantoolError, match="Length parameter in substring cannot be negative"):
        instance.sql("SELECT SUBSTRING('string' FOR -5)")

    with pytest.raises(TarantoolError, match="Length parameter in substring cannot be negative"):
        instance.sql("SELECT SUBSTRING('string', 2 , -10)")

    # invalid types of parameters
    with pytest.raises(
        TarantoolError, match=r"could not resolve function overload for substring\(text, bool, unsigned\)"
    ):
        instance.sql(r"""SELECT SUBSTRING ('abc' FROM false FOR 1)""")

    # substring expression examples
    data = instance.sql("SELECT SUBSTRING('1234567890' FROM 3)")
    assert data[0] == ["34567890"]

    data = instance.sql("SELECT SUBSTRING('(select 1)', 3)")
    assert data[0] == ["elect 1)"]

    data = instance.sql("SELECT SUBSTRING('1234567890' FROM 4 FOR 3)")
    assert data[0] == ["456"]

    # check for caching similar queries #1
    with pytest.raises(TarantoolError, match=r"could not resolve function overload for substr\(text, unsigned, text\)"):
        instance.sql(r"""SELECT SUBSTRING('12' for ?)""", "12")

    data = instance.sql("SELECT SUBSTRING('12' for ?)", 12)
    assert data[0] == ["12"]

    # check for caching similar queries #2
    with pytest.raises(TarantoolError, match=r"could not resolve function overload for substr\(text, unsigned, text\)"):
        instance.sql(r"""SELECT SUBSTRING('12' for ?)""", "12")

    # overflow cases
    data = instance.sql("SELECT SUBSTRING('string' FROM 2 FOR 2147483646)")
    assert data[0] == ["tring"]

    with pytest.raises(TarantoolError, match="u64 parsing error number too large to fit in target"):
        instance.sql("SELECT SUBSTRING('string' FROM 2 FOR 99999999999999999999)")

    # regular expression substring (with SQL's regex syntax)
    data = instance.sql("""SELECT SUBSTRING('abcdefg' SIMILAR 'a#"(b_d)#"%' ESCAPE '#')""")
    assert data[0] == ["bcd"]

    # double quotes in pattern
    data = instance.sql("""SELECT SUBSTRING('Thomas' SIMILAR '%""o_a""_' ESCAPE '"')""")
    assert data[0] == ["oma"]

    # obsolete SQL99 syntax
    data = instance.sql("""SELECT SUBSTRING('abcdefg' FROM 'a#"(b_d)#"%' FOR '#')""")
    assert data[0] == ["bcd"]

    # NULL as pattern
    data = instance.sql("""SELECT SUBSTRING('abcdefg' SIMILAR NULL ESCAPE '#')""")
    assert data[0] == [None]

    # NULL as escape
    instance.sql("SELECT SUBSTRING('abcdefg' SIMILAR '%' ESCAPE NULL)")
    assert data[0] == [None]

    # NULL as string
    instance.sql("SELECT SUBSTRING(NULL SIMILAR '%' ESCAPE '#')")
    assert data[0] == [None]

    # NULL as all parameters
    instance.sql("SELECT SUBSTRING(NULL SIMILAR NULL ESCAPE NULL)")
    assert data[0] == [None]

    # the first and last parts should act non-greedy
    data = instance.sql("""SELECT SUBSTRING('abcdefg' SIMILAR 'a#"%#"g' ESCAPE '#')""")
    assert data[0] == ["bcdef"]

    data = instance.sql("""SELECT SUBSTRING('abcdefg' SIMILAR 'a*#"%#"g*' ESCAPE '#')""")
    assert data[0] == ["bcdefg"]

    # vertical bar in any part affects only that part
    data = instance.sql("""SELECT SUBSTRING('abcdefg' SIMILAR 'a|b#"%#"g' ESCAPE '#')""")
    assert data[0] == ["bcdef"]

    data = instance.sql("""SELECT SUBSTRING('abcdefg' SIMILAR 'a#"%#"x|g' ESCAPE '#')""")
    assert data[0] == ["bcdef"]

    data = instance.sql("""SELECT SUBSTRING('abcdefg' SIMILAR 'a#"%|ab#"g' ESCAPE '#')""")
    assert data[0] == ["bcdef"]

    # postgres extension cases
    data = instance.sql("""SELECT SUBSTRING('abcdefg' SIMILAR 'a#"%g' ESCAPE '#')""")
    assert data[0] == ["bcdefg"]

    data = instance.sql("""SELECT SUBSTRING('abcdefg' SIMILAR 'a%g' ESCAPE '#')""")
    assert data[0] == ["abcdefg"]

    # with two arguments as POSIX regex
    data = instance.sql("SELECT SUBSTRING('abcdefg' FROM 'c.e')")
    assert data[0] == ["cde"]

    # with parenthesized subexpression
    data = instance.sql("SELECT SUBSTRING('abcdefg' FROM 'b(.*)f')")
    assert data[0] == ["cde"]

    # check case where there's a match but no subexpression
    data = instance.sql("SELECT SUBSTRING('foo' FROM 'foo(bar)?')")
    assert data[0] == [None]

    # invalid escape string (should error)
    with pytest.raises(TarantoolError, match="invalid escape string"):
        instance.sql("""SELECT SUBSTRING('abcdefg' SIMILAR 'abc' ESCAPE 'xy')""")

    # invalid pattern (should error)
    with pytest.raises(TarantoolError, match="Invalid pattern: regex parse error"):
        instance.sql("""SELECT SUBSTRING('abcdefg' SIMILAR '(%' ESCAPE '`')""")

    # invalid pattern (should error)
    with pytest.raises(TarantoolError, match="more than two escape-double-quote separators"):
        instance.sql(r"""SELECT SUBSTRING('abcdefg' SIMILAR '%\"\"\"%' ESCAPE '\')""")

    # invalid parameters number
    # TODO: Get rid of this check so we can get "could not resolve overload for substring(text)" error
    with pytest.raises(
        TarantoolError, match="incorrect SUBSTRING parameters. There is no such overload that takes only 1 argument"
    ):
        instance.sql("SELECT SUBSTRING('abc')")

    # character class handling
    data = instance.sql("""SELECT SUBSTRING('abcdefg' SIMILAR 'a[bcd]%g' ESCAPE '#')""")
    assert data[0] == ["abcdefg"]

    data = instance.sql(r"SELECT SUBSTRING('abcdefg' SIMILAR 'a[b\]c]%g' ESCAPE '\')")
    assert data[0] == ["abcdefg"]

    # escaping special characters
    data = instance.sql("""SELECT SUBSTRING('a.c' SIMILAR '%#.%' ESCAPE '#')""")
    assert data[0] == ["a.c"]

    data = instance.sql("""SELECT SUBSTRING('a^c' SIMILAR '%#^%' ESCAPE '#')""")
    assert data[0] == ["a^c"]

    data = instance.sql("""SELECT SUBSTRING('a$c' SIMILAR '%#$%' ESCAPE '#')""")
    assert data[0] == ["a$c"]

    # error on too many escape-double-quote separators
    with pytest.raises(
        TarantoolError,
        match="may not contain more than two escape-double-quote separators",
    ):
        instance.sql("""SELECT SUBSTRING('abcdefg' SIMILAR 'a#"b#"c#"d' ESCAPE '#')""")

    # backslash outside char class
    data = instance.sql("SELECT SUBSTRING('abc\\def' SIMILAR '%\\%' ESCAPE '#')")
    assert data[0] == ["abc\\def"]

    # backslash inside char class
    data = instance.sql("SELECT SUBSTRING('abc\\def' SIMILAR '%[\\\\]d%' ESCAPE '#')")
    assert data[0] == ["abc\\def"]

    # casted to string expressions as parameters
    data = instance.sql("SELECT SUBSTRING(TRUE::STRING SIMILAR '%%' ESCAPE '#')")
    assert data[0] == ["TRUE"]

    data = instance.sql(r"""SELECT SUBSTRING(TRUE::string SIMILAR 'T3"%|R3"E' ESCAPE (2+1)::string)""")
    assert data[0] == ["RU"]

    data = instance.sql("SELECT SUBSTRING(1234::STRING for 2)")
    assert data[0] == ["12"]

    # casted to integer expressions as parameters
    data = instance.sql("SELECT SUBSTRING('1234567890' FOR 5::integer)")
    assert data[0] == ["12345"]

    # casted arithmetic expression
    data = instance.sql("SELECT SUBSTRING('1234567890',(3 * 1)::int, (3 - 1)::int)")
    assert data[0] == ["34"]

    # with parameters
    data = instance.sql(
        """SELECT SUBSTRING((? || 'ccc') similar ? escape ?), SUBSTRING(? similar ? escape ?)""",
        "abcdefg",
        'a#"(b_d)#"%',
        "#",
        "abcdefg",
        'a#"%|ab#"g',
        "#",
    )
    assert data[0] == ["bcd", "bcdef"]

    data = instance.sql(
        """SELECT SUBSTRING( (select $1 || 'z')::string for  ($2 + 1)::int)""",
        "a234567890",
        2,
    )
    assert data[0] == ["a23"]

    data = instance.sql(
        """SELECT SUBSTRING( (select '1' ||  $1 ) for  ($2 + 1 )::int)""",
        "234567890",
        5,
    )
    assert data[0] == ["123456"]

    data = instance.sql(
        """SELECT SUBSTRING( $1 from  $2)""",
        "a234567890",
        "3.5",
    )
    assert data[0] == ["345"]

    instance.sql(
        """SELECT SUBSTRING( (select $1::text ) from  $2::string)""",
        "a234567890",
        "3.5",
    )
    assert data[0] == ["345"]

    data = instance.sql(
        """SELECT SUBSTRING( (select $1 )::string from  $2)""",
        "a234567890",
        3,
    )
    assert data[0] == ["34567890"]

    data = instance.sql(
        """SELECT SUBSTRING( (select $1 )::string from  $2)""",
        "a234567890",
        "3.5",
    )
    assert data[0] == ["345"]

    # empty ESCAPE
    data = instance.sql("""SELECT SUBSTRING('Thomas' SIMILAR '%o_a_' ESCAPE '')""")
    assert data[0] == ["Thomas"]

    # CTE
    data = instance.sql(
        """with cte(pattern) as (select 'a#"(b_d)#"%') SELECT SUBSTRING('abcdefg' SIMILAR pattern::text ESCAPE '#') from cte"""
    )
    assert data[0] == ["bcd"]

    data = instance.sql(
        """with cte(escape) as (select '#') SELECT SUBSTRING('abcdefg' SIMILAR 'a#"%#"g' ESCAPE escape) from cte"""
    )
    assert data[0] == ["bcdef"]

    data = instance.sql("""with cte(s) as (values ('1'), (NULL)) select substring(s from 'a.c') from cte""")
    assert sorted(data, key=lambda x: (x[0] is not None, x[0])) == [[None], [None]]

    data = instance.sql("""with cte(s) as (values ('1'), (NULL)) select substring(s similar '1' escape '$') from cte""")
    assert sorted(data, key=lambda x: (x[0] is not None, x[0])) == [[None], ["1"]]

    data = instance.sql("""with cte(s) as (values ('1'), (NULL)) select substring(s from '1' for '$') from cte""")
    assert sorted(data, key=lambda x: (x[0] is not None, x[0])) == [[None], ["1"]]

    data = instance.sql("""with cte(s) as (values ('1'), (NULL)) select substring(s from 1 for 2) from cte""")
    assert sorted(data, key=lambda x: (x[0] is not None, x[0])) == [[None], ["1"]]

    data = instance.sql("""with cte(s) as (values ('1'), (NULL)) select substring(s, 1, 2) from cte""")
    assert sorted(data, key=lambda x: (x[0] is not None, x[0])) == [[None], ["1"]]

    data = instance.sql("""with cte(s) as (values ('1'), (NULL)) select substring(s for 1) from cte""")
    assert sorted(data, key=lambda x: (x[0] is not None, x[0])) == [[None], ["1"]]

    data = instance.sql("""with cte(s) as (values ('1'), (NULL)) select substring(s from 1) from cte""")
    assert sorted(data, key=lambda x: (x[0] is not None, x[0])) == [[None], ["1"]]

    data = instance.sql("""with cte(s) as (values ('1'), (NULL)) select substring(s, 1) from cte""")
    assert sorted(data, key=lambda x: (x[0] is not None, x[0])) == [[None], ["1"]]

    data = instance.sql("""with cte(s) as (values ('1'), (NULL)) select substring(s from s) from cte""")
    assert sorted(data, key=lambda x: (x[0] is not None, x[0])) == [[None], ["1"]]


def test_coalesce(instance: Instance):
    instance.sql("create table foo (id int primary key, bar unsigned null);")
    instance.sql("insert into foo values (1, null), (2, 1);")

    # Coalesce returns first non null value from its arguments.
    data = instance.sql("select coalesce(bar, 0) from foo;")
    assert data == [[0], [1]]

    # Type mismatch: all values must have the same type.
    with pytest.raises(TarantoolError, match="COALESCE types unsigned and text cannot be matched"):
        instance.sql("select coalesce(bar, 'none'::text) from foo;")

    # 0 / 0 is not evaluated.
    data = instance.sql("select coalesce(bar, 0, 0 / 0) from foo;")
    assert data == [[0], [1]]


def test_lower_upper(instance: Instance):
    instance.sql(
        """
        create table t (id int primary key, s string)
        using memtx
        """
    )

    instance.sql(""" insert into t values (1, 'AbbA') """)

    data = instance.sql(""" select lower(s) from t """)
    assert data[0] == ["abba"]

    data = instance.sql(""" select upper(s) from t """)
    assert data[0] == ["ABBA"]

    data = instance.sql(""" select lower(upper(s)) from t """)
    assert data[0] == ["abba"]

    data = instance.sql(""" select upper(lower(s)) from t """)
    assert data[0] == ["ABBA"]


def test_except_on_global_tbls(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        create table g (a int, b int not null, primary key (a))
        using memtx
        distributed globally
        option (timeout = 3)
        """
    )
    assert ddl["row_count"] == 1

    dml = i1.sql(
        """
        insert into g values (1, 1), (2, 2),
        (3, 3), (4, 4), (5, 5)
        """
    )
    assert dml["row_count"] == 5

    ddl = i1.sql(
        """
        create table s (c int, d int not null, primary key (c))
        using memtx
        distributed by (c)
        option (timeout = 3)
        """
    )
    assert ddl["row_count"] == 1

    try:
        data = i1.retriable_sql(
            """insert into s values (3, 2), (4, 3), (5, 4), (6, 5), (7, 6);""",
            fatal_predicate=r"Duplicate key exists in unique index",
        )
        assert data["row_count"] == 5
    except TarantoolError as e:
        assert re.search(r"Duplicate key exists in unique index", str(e))

    data = i1.retriable_sql(
        """
        select a from g
        except
        select a - 1 from g
        """,
        timeout=2,
    )
    assert data == [[5]]

    data = i1.retriable_sql(
        """
        select a from g
        except
        select c from s
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x) == [[1], [2]]

    data = i1.retriable_sql(
        """
        select b from g
        except
        select d from s
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == [[1]]

    data = i1.retriable_sql(
        """
        select b from g
        where b = 1 or b = 2
        except
        select sum(d) from s
        where d = 3
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == [[1], [2]]

    data = i1.retriable_sql(
        """
        select sum(d) from s
        where d = 3 or d = 2
        except
        select b from g
        where b = 1 or b = 2
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == [[5]]

    data = i1.retriable_sql(
        """
        select c from s
        except
        select a from g
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == [[6], [7]]

    data = i1.retriable_sql(
        """
        select d from s
        except
        select b from g
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == [[6]]

    data = i1.retriable_sql(
        """
        select a + 5 from g
        where a = 1 or a = 2
        except select * from (
        select d from s
        except
        select b from g
        )
        """,
        timeout=2,
    )
    assert sorted(data, key=lambda x: x[0]) == [[7]]


def test_except_parsing_panic_gl_1339(cluster: Cluster):
    """
    https://git.picodata.io/core/picodata/-/issues/1339
    """

    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    with pytest.raises(
        TarantoolError, match="children tuples have mismatching amount of columns in except node: left 1, right 2"
    ):
        i1.sql("select 1 except select 1, 2")

    with pytest.raises(TarantoolError, match=r"could not resolve operator overload for \+\(unsigned, bool\)"):
        i1.sql("select 1 + false except select 1")

    ddl = i1.sql("create table lol(a int primary key, b int, c int);")
    assert ddl["row_count"] == 1

    ddl = i1.sql("create table kek (a int primary key, b int, c int);")
    assert ddl["row_count"] == 1

    # query from the issue
    with pytest.raises(
        TarantoolError, match="children tuples have mismatching amount of columns in except node: left 2, right 1"
    ):
        i1.sql(
            """
            WITH cte0 AS
              (SELECT t1.c,
                      t3.b
               FROM kek t0
               JOIN lol t1 ON t0.a = t1.b
               JOIN lol t3 ON
                 (SELECT 86
                  FROM lol
                  JOIN kek t1 ON t1.b =
                    (SELECT a
                     FROM lol
                     WHERE c IN (
                                 VALUES (30101762462282911)))) <= 0
               UNION SELECT 0,
                            0
               EXCEPT SELECT t3.a
               FROM kek t0
               JOIN lol t1 ON t0.a = t1.b
               JOIN lol t2 ON a = a
               JOIN kek t3 ON a BETWEEN t2.b AND t3.c),
                 cte1 AS
              (SELECT t1.c,
                      t0.c,
                      59 c3
               FROM lol t0
               JOIN lol t1 ON t0.a = t1.c
               ORDER BY c3)
            SELECT 0
            FROM cte0;
            """
        )


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
    assert data == [[lua_hash % bucket_count + 1]]


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
    assert data == [[420]]


def test_select_string_field(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    ddl = i1.sql(
        """
        create table "STUFF" ("id" integer not null, "str" string null, primary key ("id"))
        distributed by ("id")
    """
    )
    assert ddl["row_count"] == 1

    data = i1.sql("""insert into "STUFF" values(1337, 'foo');""")
    assert data["row_count"] == 1
    data = i1.sql("""select * from "STUFF" """)
    assert data == [[1337, "foo"]]


def test_create_drop_table(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    ddl = i1.sql(
        """
        create table "t" ("a" integer, "b" int, primary key ("b", "a"))
        using memtx
        distributed by ("a", "b")
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1
    cluster.raft_wait_index(i1.raft_get_index())

    # Already exists error.
    with pytest.raises(TarantoolError, match="table t already exists"):
        ddl = i1.sql(
            """
            create table "t" ("a" integer, "b" int, primary key ("b", "a"))
            using memtx
            distributed by ("a", "b")
            option (timeout = 3)
        """
        )

    ddl = i2.sql(
        """
        drop table "t"
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1
    cluster.raft_wait_index(i2.raft_get_index())

    # already dropped -> error, no such table
    with pytest.raises(TarantoolError, match="table t does not exist"):
        i2.sql(
            """
            drop table "t"
            option (timeout = 3)
        """
        )

    ddl = i2.sql(
        """
        create table "t" ("a" integer, "b" int, primary key ("b", "a"))
        using memtx
        distributed by ("a", "b")
    """
    )
    assert ddl["row_count"] == 1
    cluster.raft_wait_index(i2.raft_get_index())

    ddl = i1.sql(
        """
        drop table "t"
    """
    )
    assert ddl["row_count"] == 1
    cluster.raft_wait_index(i1.raft_get_index())

    # Check vinyl space
    ddl = i1.sql(
        """
        create table "t" ("key" string, "value" string not null, primary key ("key"))
        using vinyl
        distributed by ("key")
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1
    cluster.raft_wait_index(i1.raft_get_index())

    ddl = i2.sql(
        """
        drop table "t"
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1
    cluster.raft_wait_index(i2.raft_get_index())

    # Check NOT NULL inferred on PRIMARY KEY
    ddl = i1.sql(
        """
        create table "t" (a int primary key)
        distributed by (a)
        """
    )
    assert ddl["row_count"] == 1
    cluster.raft_wait_index(i1.raft_get_index())
    with pytest.raises(
        TarantoolError,
        match=("Tuple field 1 \\(a\\) type does not match one required by operation: expected integer, got nil"),
    ):
        i1.sql('insert into "t" values (null)')

    ddl = i2.sql(
        """
        drop table "t"
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1
    cluster.raft_wait_index(i2.raft_get_index())

    # Check global space
    ddl = i1.sql(
        """
        create table "global_t" ("key" string, "value" string not null,
        primary key ("key"))
        using memtx
        distributed globally
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1
    cluster.raft_wait_index(i1.raft_get_index())

    # check distribution can be skipped and sharding key
    # will be inferred from primary key
    ddl = i1.sql(
        """
        create table "infer_sk" ("a" string, "b" string,
        primary key ("b", "a"))
    """
    )
    assert ddl["row_count"] == 1
    data = i1.sql(
        """
        select "distribution" from "_pico_table"
        where "name" = 'infer_sk'
    """
    )
    assert data == [[{"ShardedImplicitly": [["b", "a"], "murmur3", "default"]}]]

    with pytest.raises(TarantoolError, match="global spaces can use only memtx engine"):
        i1.sql(
            """
            create table "t" ("key" string, "value" string not null,
            primary key ("key"))
            using vinyl
            distributed globally
            option (timeout = 3)
            """
        )

    # Check table creation with different forms of primary key declaration.
    with pytest.raises(TarantoolError, match="Primary key has been already declared"):
        i1.sql(
            """
            create table "primary_t" (a int not null primary key, b int not null primary key)
            distributed by (a)
            """
        )
    with pytest.raises(TarantoolError, match="Primary key has been already declared"):
        i1.sql(
            """
            create table "primary_t" (a int primary key, b int, primary key (a))
            distributed by (a)
            """
        )
    with pytest.raises(TarantoolError, match="Primary key has been already declared"):
        i1.sql(
            """
            create table "primary_t" (a int not null primary key, b int, primary key (b))
            distributed by (a)
            """
        )
    with pytest.raises(TarantoolError, match="Primary key must be declared"):
        i1.sql(
            """
            create table "primary_t" (a int)
            distributed by (a)
            """
        )

    ddl = i1.sql(
        """
        create table "primary_t" (a int not null primary key)
        distributed by (a)
        """
    )
    assert ddl["row_count"] == 1


def test_check_format(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    # Primary key missing.
    with pytest.raises(TarantoolError, match="Primary key column b not found"):
        i1.sql(
            """
        create table "error" ("a" integer, primary key ("b"))
        using memtx
        distributed by ("a")
    """
        )
    # Sharding key missing.
    with pytest.raises(TarantoolError, match="Sharding key column b not found"):
        i1.sql(
            """
        create table "error" ("a" integer not null, primary key ("a"))
        using memtx
        distributed by ("b")
    """
        )
    # Nullable primary key.
    with pytest.raises(TarantoolError, match="Primary key mustn't contain nullable columns"):
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


@pytest.mark.flaky(reruns=3)
def test_values(cluster: Cluster):
    """
    flaky: https://git.picodata.io/core/picodata/-/issues/1354
    """
    # Initially test scenarios (presented below) were described in the issue:
    # https://git.picodata.io/core/picodata/-/issues/1278.
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    cluster.wait_until_instance_has_this_many_active_buckets(i1, 1500)
    cluster.wait_until_instance_has_this_many_active_buckets(i2, 1500)

    ddl = i1.sql(""" create table t (a int primary key, b text) """)
    assert ddl["row_count"] == 1

    dml = i1.sql(""" insert into t values (1, ?), (2, 'hi') """, None)
    assert dml["row_count"] == 2

    data = i1.sql("""select * from t """)
    assert sorted(data) == [[1, None], [2, "hi"]]

    ddl = i1.sql(""" create table gt (a unsigned primary key, b integer) distributed globally """)
    assert ddl["row_count"] == 1

    dml = i1.sql(""" insert into gt values (0, -9), (1, 9) """)
    assert dml["row_count"] == 2

    data = i1.sql("""select * from gt """)
    assert data == [[0, -9], [1, 9]]


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
    with pytest.raises(TarantoolError, match="expected 1 values for parameters, got 0"):
        i1.sql(
            """
        insert into "t" values (?::int)
        """
        )
    with pytest.raises(TarantoolError, match="expected 2 values for parameters, got 1"):
        i1.sql(
            """
        insert into "t" values (?::int), (?::int)
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
    assert data == [[1, 1]]

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
    assert data == [[1, 2]]

    i1.sql("""
        drop table if exists t
           """)

    ddl = i1.sql(
        """
        create table "t" ("a" integer not null, "b" int not null, primary key ("a")) 
        distributed globally
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
    assert data == [[1, 1]]

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
    assert data == [[1, 2]]


def test_truncate_simple_flow(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql("CREATE TABLE t(a INT PRIMARY KEY)")
    assert ddl["row_count"] == 1

    dml = i1.sql("INSERT INTO t VALUES (1), (2), (3)")
    assert dml["row_count"] == 3

    # TRUNCATE deletes all rows from sharded table.
    ddl = i1.sql("TRUNCATE TABLE t")
    assert ddl["row_count"] == 1
    dql = i1.sql("SELECT * FROM t")
    assert dql == []

    # Refill table.
    dml = i1.sql("INSERT INTO t VALUES (1), (2), (3)")
    assert dml["row_count"] == 3

    username = "vasya"
    password = "Passw0rd"
    acl = i1.sql(f"""create user "{username}" with password '{password}' using chap-sha1""")
    assert acl["row_count"] == 1

    # TRUNCATE is denied without WRITE privilege.
    with pytest.raises(
        TarantoolError,
        match=rf"Write access to space 't' is denied for user '{username}'",
    ):
        i1.sql("TRUNCATE t", user=username, password=password)

    acl = i1.sql(f"GRANT WRITE ON TABLE t TO {username}")
    assert acl["row_count"] == 1

    # Now TRUNCATE is allowed.
    ddl = i1.sql("TRUNCATE TABLE t", user=username, password=password)
    assert ddl["row_count"] == 1
    dql = i1.sql("SELECT * FROM t")
    assert dql == []

    ddl = i1.sql("CREATE TABLE gt(a INT PRIMARY KEY) DISTRIBUTED GLOBALLY")
    assert ddl["row_count"] == 1

    dml = i1.sql("INSERT INTO gt VALUES (1), (2), (3)")
    assert dml["row_count"] == 3

    # TRUNCATE deletes all rows from global table.
    ddl = i1.sql("TRUNCATE TABLE gt")
    assert ddl["row_count"] == 1
    dql = i1.sql("SELECT * FROM gt")
    assert dql == []


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

    with pytest.raises(TarantoolError, match="Reached a limit on max executed vdbe opcodes. Limit: 5"):
        i1.sql(
            """
        select * from "t" where "a" = 1 option(sql_vdbe_opcode_max=5)
    """
        )

    dql = i1.sql(
        """
        select * from "t" where "a" = 1 option(sql_vdbe_opcode_max=50)
    """
    )
    assert dql == [[1, 1]]

    with pytest.raises(
        TarantoolError,
        match=r"Exceeded maximum number of rows \(1\) in virtual table: 2",
    ):
        i1.sql(
            """
        select * from "t" option(sql_motion_row_max=1, sql_vdbe_opcode_max=50)
    """
        )


def test_sql_acl_password_length(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    username = "USER"
    password_empty = ""
    password_long = "Passw0rd"

    acl = i1.sql(
        f"""
        create user {username} with password '{password_long}'
        using md5 option (timeout = 3)
        """
    )
    assert acl["row_count"] == 1
    acl = i1.sql(f"drop user {username}")
    assert acl["row_count"] == 1

    acl = i1.sql(
        f"""
        create user {username}
        using ldap option (timeout = 3)
    """
    )
    assert acl["row_count"] == 1
    acl = i1.sql(f"drop user {username}")
    assert acl["row_count"] == 1

    with pytest.raises(
        TarantoolError,
        match="expected ChapSha1 or Md5",
    ):
        i1.sql(
            f"""
            create user {username} with password '{password_empty}'
            using ldap option (timeout = 3)
        """
        )
        i1.sql(
            f"""
            create user {username} with password '{password_long}'
            using ldap option (timeout = 3)
        """
        )


def test_sql_acl_users_roles(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    username = "User"
    password = "Passw0rd"
    upper_username = "USER"
    lower_username = "user"
    rolename = "Role"
    upper_rolename = "ROLE"
    lower_rolename = "role"
    acl = i1.sql(
        f"""
        create user "{username}" with password '{password}'
        using md5 option (timeout = 3)
    """
    )
    assert acl["row_count"] == 1
    assert i1.call("box.space._pico_user.index._pico_user_name:get", username) is not None

    # Dropping user that doesn't exist should return does not exist error.
    with pytest.raises(TarantoolError, match="user x does not exist"):
        i1.sql("drop user x")

    # Dropping user that does exist should return 1.
    acl = i1.sql(f'drop user "{username}"')
    assert acl["row_count"] == 1
    assert i1.call("box.space._pico_user.index._pico_user_name:get", username) is None

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
        create user {username} using ldap
    """
    )
    assert acl["row_count"] == 1
    acl = i1.sql(f"drop user {username}")
    assert acl["row_count"] == 1
    # * Username without parentheses should be downcast.
    acl = i1.sql(
        f"""
        create user {upper_username} with password '{password}'
        option (timeout = 3)
    """
    )
    assert acl["row_count"] == 1
    acl = i1.sql(f'drop user "{lower_username}"')
    assert acl["row_count"] == 1

    # Check user creation with LDAP works well with non-empty password specification
    # (it must be ignored).
    acl = i1.sql(
        f"""
        create user "{upper_username}" using ldap
    """
    )
    assert acl["row_count"] == 1

    acl = i1.sql(f'drop user "{upper_username}"')
    assert acl["row_count"] == 1

    acl = i1.sql(f"create user {username} with password '{password}' using md5")
    assert acl["row_count"] == 1
    with pytest.raises(TarantoolError, match="user .* already exists"):
        i1.sql(f"create user {username} with password '{password}' using md5")

    acl = i1.sql(f"drop user {username}")
    assert acl["row_count"] == 1
    with pytest.raises(TarantoolError, match="user .* does not exist"):
        i1.sql(f"drop user {username}")

    # Zero timeout should return timeout error.
    with pytest.raises(TarantoolError, match="timeout"):
        i1.sql(f"drop user {username} option (timeout = 0)")
    with pytest.raises(TarantoolError, match="timeout"):
        i1.sql(f"drop role {username} option (timeout = 0)")
    with pytest.raises(TarantoolError, match="timeout"):
        i1.sql(
            f"""
            create user {username} with password '{password}'
            option (timeout = 0)
        """
        )
    with pytest.raises(TarantoolError, match="timeout"):
        i1.sql(
            f"""
            alter user {username} with password '{password}'
            option (timeout = 0)
        """
        )

    # Username in single quotes is unsupported.
    with pytest.raises(TarantoolError, match="rule parsing error"):
        i1.sql(f"drop user '{username}'")
    with pytest.raises(TarantoolError, match="rule parsing error"):
        i1.sql(f"create user '{username}' with password '{password}'")
    with pytest.raises(TarantoolError, match="rule parsing error"):
        i1.sql(f"alter user '{username}' with password '{password}'")
    # Role name in single quotes is unsupported.
    with pytest.raises(TarantoolError, match="rule parsing error"):
        i1.sql(f"drop role '{username}'")

    # Can't create same user with different auth methods.
    with pytest.raises(TarantoolError, match="user .* already exists"):
        i1.sql(f"create user {username} with password '{password}' using md5")
        i1.sql(f"create user {username} with password '{password}' using chap-sha1")

    # Can't create same user with different password.
    with pytest.raises(TarantoolError, match="user .* already exists"):
        i1.sql(f"create user {username} with password 'Badpa5SS' using md5")

    acl = i1.sql(f"drop user {username}")
    assert acl["row_count"] == 1

    # Attempt to drop role with name of user should return error.
    with pytest.raises(TarantoolError, match=f"User {username} exists. Unable to drop user."):
        i1.sql(f""" create user "{username}" with password '{password}' """)
        i1.sql(f""" drop role "{username}" """)
    acl = i1.sql(f""" drop user "{username}" """)
    assert acl["row_count"] == 1

    another_password = "Qwerty123"
    # Alter of non-existent user should raise an error.
    with pytest.raises(TarantoolError, match="user .* does not exist"):
        i1.sql(f"alter user nobody with password '{another_password}'")

    # Check altering works.
    acl = i1.sql(f"create user {username} with password '{password}' using md5")
    assert acl["row_count"] == 1
    user_def = i1.call("box.space._pico_user.index._pico_user_name:get", lower_username)
    users_auth_was = user_def[3]
    # * Password and method aren't changed -> update nothing.
    acl = i1.sql(f"alter user {username} with password '{password}' using md5")
    assert acl["row_count"] == 0
    user_def = i1.call("box.space._pico_user.index._pico_user_name:get", lower_username)
    users_auth_became = user_def[3]
    assert users_auth_was == users_auth_became

    # * Password is changed -> update hash.
    acl = i1.sql(f"alter user {username} with password '{another_password}' using md5")
    assert acl["row_count"] == 1
    user_def = i1.call("box.space._pico_user.index._pico_user_name:get", lower_username)
    users_auth_became = user_def[3]
    assert users_auth_was[0] == users_auth_became[0]
    assert users_auth_was[1] != users_auth_became[1]

    # * Password and method are changed -> update method and hash.
    acl = i1.sql(f"alter user {username} with password '{another_password}' using chap-sha1")
    assert acl["row_count"] == 1
    user_def = i1.call("box.space._pico_user.index._pico_user_name:get", lower_username)
    users_auth_became = user_def[3]
    assert users_auth_was[0] != users_auth_became[0]
    assert users_auth_was[1] != users_auth_became[1]
    # * LDAP does not need a password -> update method and hash.
    acl = i1.sql(f"alter user {username} with password using ldap")
    assert acl["row_count"] == 1
    user_def = i1.call("box.space._pico_user.index._pico_user_name:get", lower_username)
    users_auth_became = user_def[3]
    assert users_auth_was[0] != users_auth_became[0]
    assert users_auth_became[1] == ""
    acl = i1.sql(f"drop user {username}")
    assert acl["row_count"] == 1

    # Attempt to create role with the name of already existed user
    # should lead to an error.
    acl = i1.sql(f""" create user "{username}" with password 'Validpassw0rd' using md5 """)
    assert acl["row_count"] == 1

    with pytest.raises(TarantoolError, match="user .* already exists"):
        i1.sql(f'create role "{username}"')
    acl = i1.sql(f'drop user "{username}"')
    assert acl["row_count"] == 1

    # Dropping role that doesn't exist should return does not exist error.
    with pytest.raises(TarantoolError, match="role .* does not exist"):
        i1.sql(f"drop role {rolename}")

    # Successive creation of role.
    acl = i1.sql(f'create role "{rolename}"')
    assert acl["row_count"] == 1
    # Unable to alter role.
    with pytest.raises(TarantoolError, match=f"Role {rolename} exists. Unable to alter role."):
        i1.sql(f"alter user \"{rolename}\" with password '{password}'")

    # Attempt to drop user with name of role should return error.
    with pytest.raises(TarantoolError, match=f"Role {rolename} exists. Unable to drop role."):
        i1.sql(f""" drop user "{rolename}" """)

    # Creation of the role that already exists shouldn't do anything.
    with pytest.raises(TarantoolError, match="role .* already exists"):
        i1.sql(f'create role "{rolename}"')
    assert i1.call("box.space._pico_user.index._pico_user_name:get", rolename) is not None

    # Dropping role that does exist should return 1.
    acl = i1.sql(f'drop role "{rolename}"')
    assert acl["row_count"] == 1
    assert i1.call("box.space._pico_user.index._pico_user_name:get", rolename) is None

    # All the rolenames below should match the same role.
    acl = i1.sql(f'create role "{upper_rolename}"')
    assert acl["row_count"] == 1
    acl = i1.sql(f'drop role "{upper_rolename}"')
    assert acl["row_count"] == 1
    acl = i1.sql(f"create role {upper_rolename}")
    assert acl["row_count"] == 1
    acl = i1.sql(f"drop role {lower_rolename}")
    assert acl["row_count"] == 1
    acl = i1.sql(f"create role {rolename}")
    assert acl["row_count"] == 1
    acl = i1.sql(f'drop role "{lower_rolename}"')
    assert acl["row_count"] == 1

    # Create user with auth method in lowercase
    acl = i1.sql("CREATE USER andy WITH PASSWORD 'Passw0rd' USING md5")
    assert acl["row_count"] == 1

    # Create user with auth method in uppercase
    acl = i1.sql("CREATE USER randy WITH PASSWORD 'Passw0rd' USING MD5")
    assert acl["row_count"] == 1

    # Create user with auth method in mixed case
    acl = i1.sql("CREATE USER wendy WITH PASSWORD 'Passw0rd' USING CHAP-sha1")
    assert acl["row_count"] == 1

    # Create the same user with the same password, but auth method is in different case
    with pytest.raises(TarantoolError, match="user .* already exists"):
        i1.sql("CREATE USER wendy WITH PASSWORD 'Passw0rd' USING CHAP-SHA1")

    # Alter user with auth method in lowercase
    acl = i1.sql("ALTER USER wendy WITH PASSWORD 'Passw0rd2' USING md5")
    assert acl["row_count"] == 1

    # Alter user with auth method in uppercase
    acl = i1.sql("ALTER USER wendy WITH PASSWORD 'Passw0rd2' USING CHAP-SHA1")
    assert acl["row_count"] == 1

    # Alter user with auth method in mixed case
    acl = i1.sql("ALTER USER wendy WITH PASSWORD 'Passw0rd2' USING Md5")
    assert acl["row_count"] == 1

    # Alter the same user with the same password, but auth method is in different case
    acl = i1.sql("ALTER USER wendy WITH PASSWORD 'Passw0rd2' USING md5")
    assert acl["row_count"] == 0


def test_sql_alter_login(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    # Create the owner of `USER`
    owner_username = "owner_user"
    owner_password = "PA5sWORD"

    acl = i1.sql(
        f"create user {owner_username} with password '{owner_password}' using chap-sha1",
        sudo=True,
    )
    assert acl["row_count"] == 1

    acl = i1.sql(f"grant create user to {owner_username}", sudo=True)
    assert acl["row_count"] == 1

    username = "user"
    password = "PA5sWORD"
    # Create user.
    acl = i1.sql(
        f"create user {username} with password '{password}' using chap-sha1",
        user=owner_username,
        password=owner_password,
    )
    assert acl["row_count"] == 1

    # Alter user with LOGIN option - opertaion is idempotent.
    acl = i1.sql(f"alter user {username} with login", sudo=True)
    assert acl["row_count"] == 1
    # Alter user with NOLOGIN option.
    acl = i1.sql(f"alter user {username} with nologin", sudo=True)
    assert acl["row_count"] == 1
    # Alter user with NOLOGIN again - operation is idempotent.
    acl = i1.sql(f"alter user {username} with nologin", sudo=True)
    assert acl["row_count"] == 1
    # Login privilege is removed
    with pytest.raises(
        Exception,
        match="User does not have login privilege",
    ):
        i1.sql("insert into t values(2);", user=username, password=password)

    # Alter user with LOGIN option again - `USER` owner can also modify LOGIN privilege.
    acl = i1.sql(
        f"alter user {username} with login",
        user=owner_username,
        password=owner_password,
    )
    assert acl["row_count"] == 1

    # Alter user with NOLOGIN option again - `USER` owner can also modify LOGIN privilege.
    acl = i1.sql(
        f"alter user {username} with nologin",
        user=owner_username,
        password=owner_password,
    )
    assert acl["row_count"] == 1

    # Login privilege cannot be granted or removed by any user
    # other than admin or user account owner
    other_username = "other_user"
    other_password = "PA5sWORD"
    acl = i1.sql(
        f"create user {other_username} with password '{other_password}' using chap-sha1",
        sudo=True,
    )
    assert acl["row_count"] == 1
    with pytest.raises(
        Exception,
        match=f"Grant Login from '{username}' is denied for '{other_username}'",
    ):
        acl = i1.sql(
            f"alter user {username} with login",
            user=other_username,
            password=other_password,
        )
    with pytest.raises(
        Exception,
        match=f"Revoke Login from '{username}' is denied for '{other_username}'",
    ):
        acl = i1.sql(
            f"alter user {username} with nologin",
            user=other_username,
            password=other_password,
        )

    # Login privilege cannot be removed from pico_service even by admin.
    with pytest.raises(
        Exception,
        match="Revoke 'login' from 'pico_service' is denied for all users",
    ):
        i1.sql('alter user "pico_service" with nologin', sudo=True)


def test_sql_acl_privileges(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    username = "user"
    another_username = "ANOTHER_USER"
    password = "PaSSW0RD"
    rolename = "role"
    another_rolename = "another_role"

    # Create users.
    acl = i1.sql(f"create user {username} with password '{password}' using chap-sha1")
    assert acl["row_count"] == 1
    acl = i1.sql(f"create user {another_username} with password '{password}' using chap-sha1 ")
    assert acl["row_count"] == 1
    # Create roles.
    acl = i1.sql(f"create role {rolename}")
    assert acl["row_count"] == 1
    acl = i1.sql(f"create role {another_rolename}")
    assert acl["row_count"] == 1
    # Create tables.
    table_name = "t1"
    another_table_name = "t2"
    ddl = i1.sql(
        f"""
        create table {table_name} ("a" int not null, primary key ("a"))
        distributed globally
    """
    )
    assert ddl["row_count"] == 1

    ddl = i1.sql(
        f"""
        create table {another_table_name} ("a" int not null, primary key ("a"))
        distributed globally
    """
    )
    assert ddl["row_count"] == 1

    # Remember number of default privileges.
    default_privileges_number = len(i1.sql(""" select * from "_pico_privilege" """))

    # =========================ERRORs======================
    # Attempt to grant unsupported privileges.
    with pytest.raises(TarantoolError, match=r"Supported privileges are: \[Read, Write, Alter, Drop\]"):
        i1.sql(f""" grant create on table {table_name} to {username} """)
    with pytest.raises(TarantoolError, match=r"Supported privileges are: \[Create, Alter, Drop\]"):
        i1.sql(f""" grant read user to {username} """)
    with pytest.raises(TarantoolError, match=r"Supported privileges are: \[Alter, Drop\]"):
        i1.sql(f""" grant create on user {username} to {rolename} """)
    with pytest.raises(TarantoolError, match=r"Supported privileges are: \[Create, Drop\]"):
        i1.sql(f""" grant alter role to {username} """)
    with pytest.raises(TarantoolError, match=r"Supported privileges are: \[Drop\]"):
        i1.sql(f""" grant create on role {rolename} to {username} """)

    # Attempt to grant unexisted role.
    with pytest.raises(TarantoolError, match="There is no role with name SUPER"):
        i1.sql(f""" grant "SUPER" to {username} """)
    # Attempt to grant TO unexisted role.
    with pytest.raises(TarantoolError, match="Nor user, neither role with name SUPER exists"):
        i1.sql(f""" grant {rolename} to "SUPER" """)
    # Attempt to revoke unexisted role.
    with pytest.raises(TarantoolError, match="There is no role with name SUPER"):
        i1.sql(f""" revoke "SUPER" from {username} """)
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
    acl = i1.sql(f""" grant create user to {username} """, sudo=True)
    assert acl["row_count"] == 1
    # * Check privileges table is updated.
    privs_rows = i1.sql(""" select * from "_pico_privilege" """)
    assert len(privs_rows) == default_privileges_number + 1
    # * TODO: User creation is available.
    # * Revoke CREATE from user.
    acl = i1.sql(f""" revoke create user from {username} """, sudo=True)
    assert acl["row_count"] == 1
    # * Check privileges table is updated.
    privs_rows = i1.sql(""" select * from "_pico_privilege" """)
    assert len(privs_rows) == default_privileges_number
    # * TODO: Check that user with granted privileges can ALTER and DROP created user
    #         as it's the owner.
    # * TODO: Revoke automatically granted privileges.
    # * TODO: Check ALTER and DROP are prohibited.
    # * Grant global ALTER on users.
    acl = i1.sql(f""" grant alter user to {username} """, sudo=True)
    assert acl["row_count"] == 1
    # * Check privileges table is updated.
    privs_rows = i1.sql(""" select * from "_pico_privilege" """)
    assert len(privs_rows) == default_privileges_number + 1
    # * TODO: Check ALTER is available.
    # * Revoke global ALTER.
    acl = i1.sql(f""" revoke alter user from {username} """, sudo=True)
    assert acl["row_count"] == 1

    privs_rows = i1.sql(""" select * from "_pico_privilege" """)
    assert len(privs_rows) == default_privileges_number

    # * TODO: Check another user can't initially interact with previously created new user.
    # * TODO: Grant ALTER and DROP user privileges to another user.
    # * TODO: Check user alternation is available.
    # * TODO: Check user drop is available.

    # TODO: ================ROLEs interaction================
    # * TODO: Role creation is prohibited.
    # * Grant CREATE to user.
    acl = i1.sql(f""" grant create role to {username} """, sudo=True)
    assert acl["row_count"] == 1
    # * Check privileges table is updated.
    privs_rows = i1.sql(""" select * from "_pico_privilege" """)
    assert len(privs_rows) == default_privileges_number + 1
    # * TODO: Role creation is available.
    # * Revoke CREATE from user.
    acl = i1.sql(f""" revoke create role from {username} """, sudo=True)
    assert acl["row_count"] == 1
    # * Check privileges table is updated.
    privs_rows = i1.sql(""" select * from "_pico_privilege" """)
    assert len(privs_rows) == default_privileges_number
    # * TODO: Check that user with granted privileges can DROP created role as it's the owner.
    # * TODO: Revoke automatically granted privileges.
    # * TODO: Check DROP are prohibited.
    # * Grant global drop on role.
    acl = i1.sql(f""" grant drop role to {username} """, sudo=True)
    assert acl["row_count"] == 1
    # * Check privileges table is updated.
    privs_rows = i1.sql(""" select * from "_pico_privilege" """)
    assert len(privs_rows) == default_privileges_number + 1
    # * TODO: Check DROP is available.
    # * Revoke global DROP.
    acl = i1.sql(f""" revoke drop role from {username} """, sudo=True)
    assert acl["row_count"] == 1

    # * TODO: Check another user can't initially interact with previously created new role.
    # * TODO: Grant DROP role privileges to another user.
    # * TODO: Check role drop is available.

    # TODO: ================TABLEs interaction===============
    # ------------------READ---------------------------------
    # * READ is not available.
    with pytest.raises(
        TarantoolError,
        match=rf"Read access to space '{table_name}' is denied for user '{username}'",
    ):
        i1.sql(f""" select * from {table_name} """, user=username, password=password)
    # * Grant READ to user.
    acl = i1.sql(f""" grant read on table {table_name} to {username} """, sudo=True)
    assert acl["row_count"] == 1
    # * Granting already granted privilege do nothing.
    acl = i1.sql(f""" grant read on table {table_name} to {username} """)
    assert acl["row_count"] == 0
    # * After grant READ succeeds.
    i1.sql(f""" select * from {table_name} """, user=username, password=password)
    # * Revoke READ.
    acl = i1.sql(f""" revoke read on table {table_name} from {username} """, sudo=True)
    assert acl["row_count"] == 1
    # * After revoke READ fails again.
    with pytest.raises(
        TarantoolError,
        match=rf"Read access to space '{table_name}' is denied for user '{username}'",
    ):
        i1.sql(f""" select * from {table_name} """, user=username, password=password)
    # ------------------WRITE---------------------------------
    # TODO: remove
    acl = i1.sql(f""" grant read on table {table_name} to {username} """, sudo=True)
    assert acl["row_count"] == 1
    # * WRITE is not available.
    with pytest.raises(
        TarantoolError,
        match=rf"Write access to space '{table_name}' is denied for user '{username}'",
    ):
        i1.sql(
            f""" insert into {table_name} values (1) """,
            user=username,
            password=password,
        )
    # * Grant WRITE to user.
    acl = i1.sql(f""" grant write on table {table_name} to {username} """, sudo=True)
    assert acl["row_count"] == 1
    # * WRITE succeeds.
    i1.sql(f""" insert into {table_name} values (1) """, user=username, password=password)
    i1.sql(f""" delete from {table_name} where "a" = 1 """)
    # * Revoke WRITE from role.
    acl = i1.sql(f""" revoke write on table {table_name} from {username} """, sudo=True)
    assert acl["row_count"] == 1
    # * WRITE fails again.
    with pytest.raises(
        TarantoolError,
        match=rf"Write access to space '{table_name}' is denied for user '{username}'",
    ):
        i1.sql(
            f""" insert into {table_name} values (1) """,
            user=username,
            password=password,
        )
    # TODO: remove
    acl = i1.sql(f""" revoke read on table {table_name} from {username} """, sudo=True)
    assert acl["row_count"] == 1
    # ------------------CREATE---------------------------------
    # * TODO: Unable to create table.
    # * Grant CREATE to user.
    acl = i1.sql(f""" grant create table to {username} """, sudo=True)
    assert acl["row_count"] == 1
    # * TODO: Creation is available.
    # * TODO: Check user can do everything he wants on a table he created:
    # ** READ.
    # ** WRITE.
    # ** CREATE index.
    # ** ALTER index.
    # ** DROP.
    # * Revoke CREATE from user.
    acl = i1.sql(f""" revoke create table from {username} """, sudo=True)
    assert acl["row_count"] == 1
    # * TODO: Creation is not available again.
    # ------------------ALTER--------------------------------
    # * TODO: Unable to create new table index.
    # * Grant ALTER to user.
    acl = i1.sql(f""" grant alter on table {table_name} to {username} """, sudo=True)
    assert acl["row_count"] == 1
    # * TODO: Index creation succeeds.
    # * Revoke ALTER from user.
    acl = i1.sql(f""" revoke alter on table {table_name} from {username} """, sudo=True)
    assert acl["row_count"] == 1
    # * TODO: Attempt to remove index fails.
    # ------------------DROP---------------------------------
    # * TODO: Unable to drop table previously created by admin.
    # * Grant DROP to user.
    acl = i1.sql(f""" grant drop on table {table_name} to {username} """, sudo=True)
    assert acl["row_count"] == 1
    # * TODO: Able to drop admin table.
    # * Revoke DROP from user.
    acl = i1.sql(f""" revoke drop on table {table_name} from {username} """, sudo=True)
    assert acl["row_count"] == 1

    # Grant global tables READ, WRITE, ALTER, DROP.
    acl = i1.sql(f""" grant read table to {username} """, sudo=True)
    assert acl["row_count"] == 1
    acl = i1.sql(f""" grant write table to {username} """, sudo=True)
    assert acl["row_count"] == 1
    acl = i1.sql(f""" grant alter table to {username} """, sudo=True)
    assert acl["row_count"] == 1
    acl = i1.sql(f""" grant drop table to {username} """, sudo=True)
    assert acl["row_count"] == 1
    # Check all operations available on another_table created by admin.
    i1.sql(f""" select * from {another_table_name} """, user=username, password=password)
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
    ddl = i1.sql(f""" drop table {another_table_name} """, user=username, password=password)
    assert ddl["row_count"] == 1
    # Revoke global privileges
    acl = i1.sql(f""" revoke read table from {username} """, sudo=True)
    assert acl["row_count"] == 1
    acl = i1.sql(f""" revoke write table from {username} """, sudo=True)
    assert acl["row_count"] == 1
    acl = i1.sql(f""" revoke alter table from {username} """, sudo=True)
    assert acl["row_count"] == 1
    acl = i1.sql(f""" revoke drop table from {username} """, sudo=True)
    assert acl["row_count"] == 1

    # ================ROLE passing================
    # * Check there are no privileges granted to anything initially.
    privs_rows = i1.sql(""" select * from "_pico_privilege" """)
    assert len(privs_rows) == default_privileges_number
    # * Read from table is prohibited for user initially.
    with pytest.raises(
        TarantoolError,
        match=rf"Read access to space '{table_name}' is denied for user '{username}'",
    ):
        i1.sql(f""" select * from {table_name} """, user=username, password=password)
    # * Grant table READ and WRITE to role.
    acl = i1.sql(f""" grant read on table {table_name} to {rolename} """, sudo=True)
    assert acl["row_count"] == 1
    acl = i1.sql(f""" grant write on table {table_name} to {rolename} """, sudo=True)
    assert acl["row_count"] == 1
    # * Grant ROLE to user.
    acl = i1.sql(f""" grant {rolename} to {username} """, sudo=True)
    assert acl["row_count"] == 1
    # * Check read and write is available for user.
    i1.sql(f""" select * from {table_name} """, user=username, password=password)
    i1.sql(f""" insert into {table_name} values (1) """, user=username, password=password)
    i1.sql(f""" delete from {table_name} where "a" = 1 """)
    # * Revoke privileges from role.
    acl = i1.sql(f""" revoke write on table {table_name} from {rolename} """, sudo=True)
    assert acl["row_count"] == 1
    acl = i1.sql(f""" revoke read on table {table_name} from {rolename} """, sudo=True)
    assert acl["row_count"] == 1
    # * Check privilege revoked from role and user.
    privs_rows = i1.sql(""" select * from "_pico_privilege" """)
    assert len(privs_rows) == default_privileges_number + 1  # default + role for user
    # * Check read is prohibited again.
    with pytest.raises(
        TarantoolError,
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
                using memtx distributed globally option (timeout = 3);')
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
  - {'name': 'a', 'type': 'integer'}
  - {'name': 'b', 'type': 'integer'}
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
    alice_pwd = "Pa55sword"

    # Create user
    acl = i1.sql(
        f"""
        create user "{username}" with password '{alice_pwd}'
        using chap-sha1 option (timeout = 3)
    """
    )
    assert acl["row_count"] == 1

    # ------------------------
    # Check SQL read privilege
    # ------------------------
    with pytest.raises(TarantoolError, match=f"Read access to space '{table_name}'"):
        i1.sql(f""" select * from "{table_name}" """, user=username, password=alice_pwd)
    # Grant read privilege
    i1.sql(f""" grant read on table "{table_name}" to "{username}" """, sudo=True)
    dql = i1.sql(f""" select * from "{table_name}" """, user=username, password=alice_pwd)
    assert dql == []

    # Revoke read privilege
    i1.sql(f""" revoke read on table "{table_name}" from "{username}" """, sudo=True)

    # -------------------------
    # Check SQL write privilege
    # -------------------------
    with pytest.raises(TarantoolError, match=f"Write access to space '{table_name}'"):
        i1.sql(
            f""" insert into "{table_name}" values (1, 2) """,
            user=username,
            password=alice_pwd,
        )

    # Grant write privilege
    i1.sql(f""" grant write on table "{table_name}" to "{username}" """, sudo=True)
    dml = i1.sql(
        f""" insert into "{table_name}" values (1, 2) """,
        user=username,
        password=alice_pwd,
    )
    assert dml["row_count"] == 1

    # Revoke write privilege
    i1.sql(f""" revoke write on table "{table_name}" from "{username}" """, sudo=True)

    # -----------------------------------
    # Check SQL write and read privileges
    # -----------------------------------
    with pytest.raises(TarantoolError, match=f"Read access to space '{table_name}'"):
        i1.sql(
            f""" insert into "{table_name}" select "a" + 1, "b" from "{table_name}"  """,
            user=username,
            password=alice_pwd,
        )
    with pytest.raises(TarantoolError, match=f"Read access to space '{table_name}'"):
        i1.sql(
            f""" update "{table_name}" set "b" = 42 """,
            user=username,
            password=alice_pwd,
        )
    with pytest.raises(TarantoolError, match=f"Read access to space '{table_name}'"):
        i1.sql(f""" delete from "{table_name}" """, user=username, password=alice_pwd)

    # Grant read privilege
    i1.sql(f""" grant read on table "{table_name}" to "{username}" """, sudo=True)

    with pytest.raises(TarantoolError, match=f"Write access to space '{table_name}'"):
        i1.sql(
            f""" insert into "{table_name}" select "a" + 1, "b" from "{table_name}"  """,
            user=username,
            password=alice_pwd,
        )
    with pytest.raises(TarantoolError, match=f"Write access to space '{table_name}'"):
        i1.sql(
            f""" update "{table_name}" set "b" = 42 """,
            user=username,
            password=alice_pwd,
        )
    with pytest.raises(TarantoolError, match=f"Write access to space '{table_name}'"):
        i1.sql(f""" delete from "{table_name}" """, user=username, password=alice_pwd)

    # Grant write privilege
    i1.sql(f""" grant write on table "{table_name}" to "{username}" """, sudo=True)

    dml = i1.sql(
        f""" insert into "{table_name}" select "a" + 1, "b" from "{table_name}"  """,
        user=username,
        password=alice_pwd,
    )
    assert dml["row_count"] == 1
    dml = i1.sql(f""" update "{table_name}" set "b" = 42 """, user=username, password=alice_pwd)
    assert dml["row_count"] == 2
    dml = i1.sql(f""" delete from "{table_name}" """, user=username, password=alice_pwd)
    assert dml["row_count"] == 2


def test_sql_privileges_vtables(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    username = "borat"
    pwd = "Password1"

    # Create user.
    acl = i1.sql(
        f"""
        create user "{username}" with password '{pwd}'
        using chap-sha1 option (timeout = 3)
    """
    )
    assert acl["row_count"] == 1

    # Grant create table privilege.
    i1.sql(f""" grant create table to "{username}" """, sudo=True)

    table_name = "foo"

    # Create a table by user.
    ddl = i1.sql(
        f"""
        create table "{table_name}" ("i" int not null, primary key ("i"))
        using memtx
        distributed by ("i")
    """,
        user=username,
        password=pwd,
    )
    assert ddl["row_count"] == 1

    # Check no-motion (without need to read vtables) query work.
    dql = i1.sql(f""" select * from "{table_name}" """, user=username, password=pwd)
    assert dql == []

    # Check with-motion (with need to read vtables) query work.
    # See https://git.picodata.io/picodata/picodata/picodata/-/issues/620
    dql = i1.sql(f""" select count(*) from "{table_name}" """, user=username, password=pwd)
    assert dql == [[0]]

    # Check DML query work.
    dml = i1.sql(f""" insert into "{table_name}" select count(*) from "{table_name}" """)
    assert dml["row_count"] == 1


def test_user_changes_password(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)
    user_name = "U"
    old_password = "Passw0rd"
    new_password = "Pa55word"

    i1.create_user(with_name=user_name, with_password=old_password, with_auth="chap-sha1")
    i1.sql(
        f"""
        ALTER USER "{user_name}" PASSWORD '{new_password}' USING chap-sha1
        """,
        user=user_name,
        password=old_password,
    )
    # ensure we can authenticate with new password
    i1.sql("SELECT * FROM (VALUES (1))", user=user_name, password=new_password)


def test_create_drop_procedure(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    data = i1.sql(
        """
        create table t (a int not null, b int, primary key (a))
        using memtx
        distributed globally
        """
    )
    assert data["row_count"] == 1
    cluster.raft_wait_index(i1.raft_get_index())

    # Check that the procedure would be created with the expected id.
    next_func_id = i1.eval("return box.internal.generate_func_id(true)")
    data = i2.sql(
        """
        create procedure proc1(int)
        language SQL
        as $$insert into t values($1, $1)$$
        """
    )
    assert data["row_count"] == 1
    cluster.raft_wait_index(i2.raft_get_index())

    data = i1.sql(
        """
        select "id" from "_pico_routine" where "name" = 'proc1'
        """,
    )
    assert data == [[next_func_id]]

    with pytest.raises(TarantoolError, match="procedure proc1 already exists"):
        i2.sql(
            """
            create procedure proc1(int, int)
            language SQL
            as $$insert into t values($1, $2)$$
            """
        )

    # Check that we can't create a procedure with a occupied name.
    with pytest.raises(TarantoolError, match="procedure proc1 already exists"):
        i2.sql(
            """
            create procedure proc1(int, int)
            language SQL
            as $$insert into t values($1, $2)$$
            option(timeout=3)
            """
        )
    with pytest.raises(TarantoolError, match="procedure proc1 already exists"):
        i2.sql(
            """
            create procedure proc1(int)
            language SQL
            as $$insert into t values(1, 2)$$
            """
        )

    # Check the routine creation abortion on _func space id conflict.
    i2.eval(
        """
        box.schema.func.create(
            'sum1',
            {
                body = [[function(a, b) return a + b end]],
                id = box.internal.generate_func_id(true)
            }
        )
        """
    )
    with pytest.raises(TarantoolError, match="ddl operation was aborted"):
        i1.sql(
            """
            create procedure proc2()
            as $$insert into t values(1, 2)$$
            """
        )
    data = i1.sql(""" select * from "_pico_routine" where "name" = 'proc2' """)
    assert data == []
    data = i2.sql(""" select * from "_pico_routine" where "name" = 'proc2' """)
    assert data == []

    # Check that proc1 is actually dropped
    i1.sql(""" drop procedure proc1 """)
    cluster.raft_wait_index(i1.raft_get_index())

    data = i1.sql(""" select * from "_pico_routine" where "name" = 'proc1' """)
    assert data == []
    data = i2.sql(""" select * from "_pico_routine" where "name" = 'proc1' """)
    assert data == []

    with pytest.raises(TarantoolError, match="procedure proc1 does not exist"):
        i2.sql(""" drop procedure proc1 """)
    cluster.raft_wait_index(i1.raft_get_index())

    # Create proc for dropping.
    data = i2.sql(
        """
        create procedure proc1(int)
        language SQL
        as $$insert into t values($1, $1)$$
        """
    )
    assert data["row_count"] == 1
    cluster.raft_wait_index(i2.raft_get_index())

    data = i1.sql(
        """
        select "id" from "_pico_routine" where "name" = 'proc1'
        """,
    )
    assert data != []
    routine_id = data[0][0]

    # Check that dropping raises an error in case of parameters mismatch.
    with pytest.raises(
        TarantoolError,
        match=r"routine exists but with a different signature: proc1\(integer\)",
    ):
        data = i2.sql(""" drop procedure proc1(decimal) """)

    # Check that dropping raises an error in case of parameters mismatch.
    with pytest.raises(
        TarantoolError,
        match=r"routine exists but with a different signature: proc1\(integer\)",
    ):
        data = i2.sql(""" drop procedure proc1(integer, integer) """)

    # Routine mustn't be dropped at the moment.
    data = i1.sql(""" select * from "_pico_routine" where "name" = 'proc1' """)
    assert data != []
    data = i2.sql(""" select * from "_pico_routine" where "name" = 'proc1' """)
    assert data != []

    # Check drop with matching parameters.
    i2.sql(""" drop procedure proc1(integer) """)
    cluster.raft_wait_index(i2.raft_get_index())

    data = i1.sql(""" select * from "_pico_routine" where "name" = 'proc1' """)
    assert data == []
    data = i2.sql(""" select * from "_pico_routine" where "name" = 'proc1' """)
    assert data == []

    # Check that recreated routine has the same id with the recently dropped one.
    i2.sql(
        """
        create procedure proc1(int)
        language SQL
        as $$insert into t values($1, $1)$$
        """
    )
    cluster.raft_wait_index(i2.raft_get_index())

    data = i1.sql(
        """
        select "id" from "_pico_routine" where "name" = 'proc1'
        """,
    )
    assert data != []
    assert routine_id == data[0][0]

    # Check that procedures from "_pico_routine" and "_func" have the same owner.
    # https://git.picodata.io/picodata/picodata/picodata/-/issues/607

    # Create a procedure using sudo, as in the issue.
    i2.sql(
        """
        create procedure FOO(int)
        language SQL
        as $$insert into t values($1, $1)$$
        """,
        sudo=True,
    )
    pico_owner = i1.sql(""" select "owner" from "_pico_routine" where "name" = 'FOO' """)
    tnt_owner = i1.eval(
        """
        return box.execute([[ select "owner" from "_vfunc" where "name" = 'FOO']]).rows
        """
    )
    assert pico_owner == tnt_owner


def test_sql_user_password_checks(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, _ = cluster.instances

    with pytest.raises(
        TarantoolError,
        match="invalid password: password should contains at least one uppercase letter",
    ):
        i1.sql(
            """
            create user noname with password 'withoutdigitsanduppercase'
            using md5 option (timeout = 3)
            """
        )

    with pytest.raises(
        TarantoolError,
        match="invalid password: password should contains at least one lowercase letter",
    ):
        i1.sql(
            """
            create user noname with password 'PASSWORD3'
            using md5 option (timeout = 3)
            """
        )

    with pytest.raises(
        TarantoolError,
        match="invalid password: password should contains at least one digit",
    ):
        i1.sql(
            """
            create user noname with password 'Withoutdigits'
            using md5 option (timeout = 3)
            """
        )

    acl = i1.sql(
        """
        create user success with password 'Withdigit1'
        using md5 option (timeout = 3)
        """
    )
    assert acl["row_count"] == 1

    # let's turn off uppercase check and turn on special characters check
    dml = i1.sql(
        """
        ALTER SYSTEM SET auth_password_enforce_uppercase=false
        """
    )
    assert dml["row_count"] == 1

    dml = i1.sql(
        """
        ALTER SYSTEM SET auth_password_enforce_specialchars=true
        """
    )
    assert dml["row_count"] == 1

    with pytest.raises(
        TarantoolError,
        match="invalid password: password should contains at least one special character",
    ):
        i1.sql(
            """
            create user noname with password 'withoutspecialchar14'
            using md5 option (timeout = 3)
            """
        )

    # now it's ok to create password without uppercase letter, but with special symbol
    acl = i1.sql(
        """
        create user noname with password '!withdigit1@'
        using md5 option (timeout = 3)
        """
    )
    assert acl["row_count"] == 1


def test_call_procedure(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    data = i1.sql(
        """
        create table t (a int not null, b int, primary key (a))
        using memtx
        distributed by (b)
        """
    )
    assert data["row_count"] == 1

    # Check that the procedure is called successfully.
    data = i2.sql(
        """
        create procedure "proc1"(int, int)
        language SQL
        as $$insert into t values(?::int, ?::int) on conflict do nothing$$
        """
    )
    assert data["row_count"] == 1
    data = i2.retriable_sql(""" call "proc1"(1, 1) """)
    assert data["row_count"] == 1
    data = i2.retriable_sql(""" call "proc1"(1, 1) """)
    assert data["row_count"] == 0
    data = i2.retriable_sql(""" call "proc1"(2, 2) """)
    assert data["row_count"] == 1

    data = i1.sql(
        """
        create procedure "proc2"(int)
        language SQL
        as $$insert into t values(?::int, 42) on conflict do fail$$
        """
    )
    assert data["row_count"] == 1

    # Check that procedure returns an error when called with wrong number of arguments.
    with pytest.raises(TarantoolError, match=r"""expected 1 parameter\(s\), got 0"""):
        i2.sql(""" call "proc2"() """)
    with pytest.raises(TarantoolError, match=r"""expected 1 parameter\(s\), got 2"""):
        i2.sql(""" call "proc2"(3, 3) """)
    # Check that procedure returns an error when called with the wrong argument type.
    error_msg = "expected integer for parameter on position 0, got string"
    with pytest.raises(TarantoolError, match=error_msg):
        i2.sql(""" call "proc2"('hello') """)

    # Check internal statement errors are propagated.
    with pytest.raises(TarantoolError, match="Duplicate key exists in unique index"):
        i2.sql(""" call "proc2"(1) """)

    # Check parameters are passed correctly.
    data = i1.retriable_sql(
        """ call "proc2"(?) """,
        4,
        fatal_predicate=r"Duplicate key exists in unique index",
    )
    assert data["row_count"] == 1
    data = i1.retriable_sql(
        """ call "proc2"($1) option(sql_vdbe_opcode_max = $1, sql_motion_row_max = $1)""",
        5,
        fatal_predicate=r"Duplicate key exists in unique index",
    )
    assert data["row_count"] == 1

    # Check call permissions.
    username = "alice"
    alice_pwd = "Passw0rd"
    acl = i1.sql(
        f"""
        create user "{username}" with password '{alice_pwd}'
        using chap-sha1 option (timeout = 3)
    """
    )
    assert acl["row_count"] == 1
    with pytest.raises(TarantoolError, match="Execute access to function 'proc1' is denied"):
        i1.retriable_sql(
            """ call "proc1"(3, 3) """,
            user=username,
            password=alice_pwd,
            fatal=TarantoolError,
        )


def test_rename_procedure(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    data = i1.sql(
        """
        create table t (a int not null, b int, primary key (a))
        using memtx
        distributed by (b)
        """
    )
    assert data["row_count"] == 1

    data = i2.sql(
        """
        create procedure foo(int)
        language SQL
        as $$insert into t values($1::int, $1::int)$$
        """
    )
    assert data["row_count"] == 1

    data = i1.sql(
        """
        alter procedure foo
        rename to BAR
        option ( timeout = 4 )
        """
    )
    assert data["row_count"] == 1

    data = i2.sql(
        """
        select "name" from "_pico_routine"
        where "name" = 'bar' or "name" = 'foo'
        """
    )
    assert data == [["bar"]]

    with pytest.raises(TarantoolError, match="procedure foo does not exist"):
        i1.sql(
            """
            alter procedure foo
            rename to bar
            """
        )

    # rename back, use syntax with parameters
    data = i1.sql(
        """
        alter procedure "bar"(int)
        rename to "FOO"
        """
    )
    assert data["row_count"] == 1

    data = i2.sql(
        """
        select "name" from "_pico_routine"
        where "name" = 'bar' or "name" = 'FOO'
        """
    )
    assert data == [["FOO"]]

    data = i2.sql(
        """
        create procedure bar()
        language SQL
        as $$insert into t values(200, 200)$$
        """
    )
    assert data["row_count"] == 1

    with pytest.raises(TarantoolError, match="Name 'FOO' is already taken"):
        data = i1.sql(
            """
            alter procedure bar()
            rename to "FOO"
            """
        )

    with pytest.raises(TarantoolError, match="routine exists but with a different signature: bar()"):
        data = i1.sql(
            """
            alter procedure bar(int)
            rename to buzz
            """
        )

    username = "alice"
    alice_pwd = "Passw0rd"
    acl = i1.sql(
        f"""
        create user "{username}" with password '{alice_pwd}'
        using chap-sha1 option (timeout = 3)
    """
    )
    assert acl["row_count"] == 1
    with pytest.raises(
        TarantoolError,
        match="Alter access to function 'bar' is denied for user 'alice'",
    ):
        i1.sql("""alter procedure bar rename to buzz""", user=username, password=alice_pwd)

    data = i2.sql(
        """
        create procedure sudo()
        language SQL
        as $$insert into t values(200, 200)$$
        """,
        sudo=True,
    )
    assert data["row_count"] == 1

    with pytest.raises(
        TarantoolError,
        match="Alter access to function 'sudo' is denied for user 'alice'",
    ):
        i1.sql("""alter procedure sudo rename to dudo""", user=username, password=alice_pwd)

    i1.sql("""alter procedure sudo rename to dudo""", sudo=True)
    data = i2.sql(
        """
        select "name" from "_pico_routine"
        where "name" = 'sudo' or "name" = 'dudo'
        """
    )
    assert data == [["dudo"]]

    i2.eval(
        """
        box.schema.func.create(
            'foobar',
            {
                body = [[function(question) return 42 end]],
                id = box.internal.generate_func_id(true)
            }
        )
        """
    )
    with pytest.raises(TarantoolError, match="ddl operation was aborted"):
        i1.sql(
            """
            alter procEdurE "FOO" rENaME tO "foobar"
            """
        )

    data = i1.sql(""" select * from "_pico_routine" where "name" = 'foobar' """)
    assert data == []
    data = i2.sql(""" select * from "_pico_routine" where "name" = 'foobar' """)
    assert data == []


def test_procedure_privileges(cluster: Cluster):
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

    alice = "alice"
    alice_pwd = "Passw0rd"

    bob = "bob"
    bob_pwd = "Passw0rd"

    for user, pwd in [(alice, alice_pwd), (bob, bob_pwd)]:
        acl = i1.sql(
            f"""
            create user "{user}" with password '{pwd}'
            using chap-sha1 option (timeout = 3)
            """
        )
        assert acl["row_count"] == 1
        # grant write permission for procedure calls to work
        acl = i1.sql(
            f"""
            grant write on table "{table_name}" to "{user}"
            """,
            sudo=True,
        )
        assert acl["row_count"] == 1

    # ---------------
    # HELPERS
    # ---------------

    def create_procedure(name: str, arg_cnt: int, as_user=None, as_pwd=None):
        assert arg_cnt < 3
        query = f"""
        create procedure {name}()
        language SQL
        as $$insert into "{table_name}" values(42, 8) on conflict do replace$$
        """
        if arg_cnt == 1:
            query = f"""
            create procedure {name}(int)
            language SQL
            as $$insert into "{table_name}" values($1, $1) on conflict do replace$$
            """
        if arg_cnt == 2:
            query = f"""
            create procedure {name}(int, int)
            language SQL
            as $$insert into "{table_name}" values($2, $1) on conflict do replace$$
            """
        ddl = None
        if not as_user:
            ddl = i1.sql(query, sudo=True)
        else:
            ddl = i1.sql(query, user=as_user, password=as_pwd)
        assert ddl["row_count"] == 1

    def drop_procedure(name: str, as_user=None, as_pwd=None):
        query = f"drop procedure {name}"
        ddl = None
        if not as_user:
            ddl = i1.sql(query, sudo=True)
        else:
            ddl = i1.sql(query, user=as_user, password=as_pwd)
        assert ddl["row_count"] == 1

    def rename_procedure(old_name: str, new_name: str, as_user=None, as_pwd=None):
        query = f"alter procedure {old_name} rename to {new_name}"
        ddl = None
        if not as_user:
            ddl = i1.sql(query, sudo=True)
        else:
            ddl = i1.sql(query, user=as_user, password=as_pwd)
        assert ddl["row_count"] == 1

    # TODO: remove this function, the tests are impossible to understand because of these
    def grant_procedure(priv: str, user: str, fun=None, as_user=None, as_pwd=None):
        query = f"grant {priv}"
        if fun:
            query += f' on procedure "{fun}"'
        else:
            query += " procedure"
        query += f' to "{user}"'
        acl = i1.sql(query, user=as_user, password=as_pwd) if as_user else i1.sql(query, sudo=True)

        assert acl["row_count"] == 1

    def revoke_procedure(priv: str, user: str, fun=None, as_user=None, as_pwd=None):
        query = f"revoke {priv}"
        if fun:
            query += f' on procedure "{fun}"'
        else:
            query += " procedure"
        query += f' from "{user}"'
        acl = i1.sql(query, user=as_user, password=as_pwd) if as_user else i1.sql(query, sudo=True)

        assert acl["row_count"] == 1

    def call_procedure(proc, *args, as_user=None, as_pwd=None):
        args_str = ",".join(str(x) for x in args)
        data = i1.sql(
            f"""
            call {proc}({args_str})
            """,
            user=as_user,
            password=as_pwd,
        )
        assert data["row_count"] == 1

    def check_execute_access_denied(fun, username, pwd, *args):
        with pytest.raises(
            TarantoolError,
            match=f"Execute access to function '{fun}' " + f"is denied for user '{username}'",
        ):
            call_procedure(fun, *args, as_user=username, as_pwd=pwd)

    def check_create_access_denied(fun, username, pwd):
        with pytest.raises(
            TarantoolError,
            match=f"Create access to function '{fun}' " + f"is denied for user '{username}'",
        ):
            create_procedure(fun, 0, as_user=username, as_pwd=pwd)

    def check_drop_access_denied(fun, username, pwd):
        with pytest.raises(
            TarantoolError,
            match=f"Drop access to function '{fun}' " + f"is denied for user '{username}'",
        ):
            drop_procedure(fun, as_user=username, as_pwd=pwd)

    def check_rename_access_denied(old_name, new_name, username, pwd):
        with pytest.raises(
            TarantoolError,
            match=f"Alter access to function '{old_name}' " + f"is denied for user '{username}'",
        ):
            rename_procedure(old_name, new_name, as_user=username, as_pwd=pwd)

    # ----------------- Default privliges -----------------

    # Check that a user can't create a procedure without permition.
    check_create_access_denied("foobazspam", alice, alice_pwd)

    # Check that a non-owner user without drop privilege can't drop proc
    create_procedure("foo", 0)
    check_drop_access_denied("foo", bob, bob_pwd)

    # Check that a non-owner user can't rename proc
    check_rename_access_denied("foo", "bar", bob, bob_pwd)

    # Check that a user without permession can't call proc
    check_execute_access_denied("foo", bob, bob_pwd)

    # Check that owner can call proc
    call_procedure("foo")

    # Check that owner can rename proc
    rename_procedure("foo", "bar")

    # Check that owner can drop proc
    drop_procedure("bar")

    # ----------------- Default privliges -----------------

    # ----------------- grant-revoke privilege -----------------

    # ALL PROCEDURES

    # Check admin can grant create procedure to user
    grant_procedure("create", alice)
    create_procedure("foo", 0, alice, alice_pwd)
    drop_procedure("foo", alice, alice_pwd)
    create_procedure("foo", 0)
    check_drop_access_denied("foo", alice, alice_pwd)
    check_rename_access_denied("foo", "bar", alice, alice_pwd)
    drop_procedure("foo")

    # Check admin can revoke create procedure from user
    revoke_procedure("create", alice)
    check_create_access_denied("foo", alice, alice_pwd)

    # check grant execute to all procedures
    grant_procedure("create", bob)
    create_procedure("foo", 0, bob, bob_pwd)
    grant_procedure("execute", alice)
    call_procedure("foo", as_user=alice, as_pwd=alice_pwd)

    # check revoke execute from all procedures
    revoke_procedure("execute", alice)
    check_execute_access_denied("foo", alice, alice_pwd)
    revoke_procedure("create", bob)

    # check grant drop for all procedures
    drop_procedure("foo")
    create_procedure("foo", 0)
    check_drop_access_denied("foo", bob, bob_pwd)
    grant_procedure("drop", bob)
    drop_procedure("foo", bob, bob_pwd)

    # check revoke drop for all procedures
    create_procedure("foo", 0)
    revoke_procedure("drop", bob)
    check_drop_access_denied("foo", bob, bob_pwd)

    drop_procedure("foo")

    # Check that user can't grant create procedure (only admin can)
    with pytest.raises(
        TarantoolError,
        # WTF is "grant to routine"?
        match=f"Grant to routine is denied for user '{alice}'",
    ):
        grant_procedure("create", bob, as_user=alice, as_pwd=alice_pwd)

    # Check that user can't grant execute procedure (only admin can)
    with pytest.raises(
        TarantoolError,
        match=f"Grant to routine is denied for user '{alice}'",
    ):
        grant_procedure("execute", bob, as_user=alice, as_pwd=alice_pwd)

    # SPECIFIC PROCEDURE

    # check grant execute specific procedure
    grant_procedure("create", alice)
    create_procedure("foo", 0, alice, alice_pwd)
    check_execute_access_denied("foo", bob, bob_pwd)
    grant_procedure("execute", bob, "foo", as_user=alice, as_pwd=alice_pwd)
    call_procedure("foo", as_user=bob, as_pwd=bob_pwd)

    # check admin can revoke execute from user
    revoke_procedure("execute", bob, "foo", alice, alice_pwd)
    check_execute_access_denied("foo", bob, bob_pwd)

    # check owner of procedure can grant drop to other user
    check_drop_access_denied("foo", bob, bob_pwd)
    grant_procedure("drop", bob, "foo", as_user=alice, as_pwd=alice_pwd)
    check_rename_access_denied("foo", "bar", bob, bob_pwd)
    drop_procedure("foo", bob, bob_pwd)

    # check owner of procedure can revoke drop to other user
    create_procedure("foo", 0, alice, alice_pwd)
    check_drop_access_denied("foo", bob, bob_pwd)
    grant_procedure("drop", bob, "foo", as_user=alice, as_pwd=alice_pwd)
    revoke_procedure("drop", bob, "foo", alice, alice_pwd)
    check_drop_access_denied("foo", bob, bob_pwd)

    # check we can't grant create specific procedure
    with pytest.raises(
        TarantoolError,
        match="sbroad: invalid privilege",
    ):
        grant_procedure("create", bob, "foo")

    # check we can't grant to non-existing user
    with pytest.raises(
        TarantoolError,
        match="Nor user, neither role with name pasha exists",
    ):
        grant_procedure("drop", "pasha", "foo")

    # check we can't revoke from non-existing user
    with pytest.raises(
        TarantoolError,
        match="Nor user, neither role with name pasha exists",
    ):
        revoke_procedure("execute", "pasha", "foo")


def test_rename_user(cluster: Cluster):
    i1, _ = cluster.deploy(instance_count=2)

    # Two users biba and boba
    biba = "biba"
    boba = "boba"
    password = "Passw0rd"

    i1.create_user(with_name=biba, with_password=password, with_auth="chap-sha1")
    i1.create_user(with_name=boba, with_password=password, with_auth="chap-sha1")

    with pytest.raises(TarantoolError, match=f"user {boba} does not exist"):
        data = i1.sql(
            f"""
            ALTER USER "{boba}"
            RENAME TO "{boba}"
            """,
            user=boba,
            password=password,
        )

    # Existed name
    with pytest.raises(
        TarantoolError,
        match=f"user {biba} already exists",
    ):
        data = i1.sql(
            f"""
            ALTER USER "{boba}"
            RENAME TO "{biba}"
            """,
            sudo=True,
        )

    # Without privileges they cannot rename each other
    with pytest.raises(
        TarantoolError,
        match="""\
Alter access to user 'boba' is denied for user 'biba'\
""",
    ):
        data = i1.sql(
            f"""
            ALTER USER "{boba}"
            RENAME TO "skibidi"
            """,
            user=biba,
            password=password,
        )

    # Admin can rename anyone: boba -> skibidi
    data = i1.sql(
        f"""
        ALTER USER "{boba}"
        RENAME TO "skibidi"
        """,
        sudo=True,
    )
    assert data["row_count"] == 1

    def names_from_pico_user_table():
        data = i1.sql(
            """
            SELECT "name" FROM "_pico_user"
            """,
            sudo=True,
        )

        return [row[0] for row in data]

    # Check that rename works fine
    assert "skibidi" in names_from_pico_user_table()
    assert boba not in names_from_pico_user_table()

    # Check that we can create a new user "boba" after rename him
    i1.create_user(with_name=boba, with_password=password)
    assert boba in names_from_pico_user_table()


def test_drop_user(cluster: Cluster):
    i1, _ = cluster.deploy(instance_count=2)
    user = "user"
    password = "Passw0rd"

    # Create and drop the created user
    i1.sql(f""" create user "{user}" with password '{password}' using chap-sha1""")
    i1.sql(f""" drop user "{user}" """)

    # Create user with privileges
    i1.sql(f""" create user "{user}" with password '{password}' using chap-sha1""")
    i1.sql(f""" grant create table to "{user}" """, sudo=True)
    i1.sql(f""" grant create procedure to "{user}" """, sudo=True)

    # Drop user shouldn't fail despite the fact that the user has privileges
    i1.sql(f""" drop user "{user}" """)

    # Create user with privileges
    i1.sql(f""" create user "{user}" with password '{password}' using chap-sha1""")
    i1.sql(f""" grant create table to "{user}" """, sudo=True)
    i1.sql(f""" grant create procedure to "{user}" """, sudo=True)

    # User creates a table
    ddl = i1.sql(
        """
        create table t (a text not null, b text not null, c text, primary key (a))
        using memtx
        distributed by (a)
        option (timeout = 3)
        """,
        user=user,
        password=password,
    )
    assert ddl["row_count"] == 1

    # User creates a procedure
    data = i1.sql(
        """
        create procedure proc1(text, text, text)
        language SQL
        as $$insert into t values(?::text, ?::text, ?::text)$$
        """,
        user=user,
        password=password,
    )
    assert data["row_count"] == 1

    # Drop user should fail as the user owns tables and routines
    with pytest.raises(
        TarantoolError,
        match=r"user cannot be dropped because some objects depend on it.*"
        r"owner of tables t.*"
        r"owner of procedures proc1.*",
    ):
        i1.sql(f""" drop user "{user}" """)

    # Drop user objects
    data = i1.sql("drop table t")
    assert data["row_count"] == 1

    data = i1.sql("drop procedure proc1")
    assert data["row_count"] == 1

    # Grant create user and create role
    data = i1.sql(f'grant create user to "{user}"', sudo=True)
    assert data["row_count"] == 1

    data = i1.sql(f'grant create role to "{user}"', sudo=True)
    assert data["row_count"] == 1

    # User creates user
    data = i1.sql(
        "create user lol with password 'Passw0rd'",
        user=user,
        password=password,
    )
    assert data["row_count"] == 1

    # User creates role
    data = i1.sql(
        "create role kek",
        user=user,
        password=password,
    )
    assert data["row_count"] == 1

    # Drop user should fail as the user owns another user and role
    with pytest.raises(
        TarantoolError,
        match=r"user cannot be dropped because some objects depend on it.*"
        r"owner of users lol.*"
        r"owner of roles kek.*",
    ):
        i1.sql(f""" drop user "{user}" """)

    # Drop user objects
    data = i1.sql("drop user lol")
    assert data["row_count"] == 1

    data = i1.sql("drop role kek")
    assert data["row_count"] == 1

    # Drop user with no objects
    data = i1.sql(f'drop user "{user}"')
    assert data["row_count"] == 1


def test_index(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    # Sharded memtx table
    ddl = i1.sql(
        """
        create table t (a text not null, b text not null, c text, primary key (a))
        using memtx
        distributed by (a)
        option (timeout = 3)
        """
    )
    assert ddl["row_count"] == 1

    # Global table
    ddl = i1.sql(
        """
        create table g (a int not null, b text not null, c text, primary key (a))
        distributed globally
        option (timeout = 3)
        """
    )
    assert ddl["row_count"] == 1

    # Sharded vinyl table
    ddl = i1.sql(
        """
        create table v (a int not null, b text not null, c text, primary key (a))
        using vinyl
        distributed by (a)
        option (timeout = 3)
        """
    )

    # Check that created index appears in _pico_index table.
    ddl = i1.sql(""" create index i0 on t (a) option (timeout = 3) """)
    assert ddl["row_count"] == 1
    data = i1.sql(""" select * from "_pico_index" where "name" = 'i0' """)
    assert data != []

    # Successful tree index creation with default options
    ddl = i1.sql(""" create index i1 on t (a, b) """)
    assert ddl["row_count"] == 1

    # Unique index can be created only on the sharding key for sharded tables.
    invalid_unique = "unique index for the sharded table must duplicate its sharding key columns"
    with pytest.raises(TarantoolError, match=invalid_unique):
        i1.sql(""" create unique index i2 on t using tree (b) """)

    # Successful unique tree index creation on the sharding key.
    ddl = i1.sql(""" create unique index i2 on t using tree (a) """)
    assert ddl["row_count"] == 1

    # No restrictions on unique index for globally distributed tables.
    ddl = i1.sql(""" create unique index i3 on g using tree (b) """)

    # Successful create a tree index with corresponding options.
    ddl = i1.sql(""" create index i4 on t using tree (c) with (HiNT = TrUe) """)
    assert ddl["row_count"] == 1

    # Fail to create a tree index with wrong options.
    invalid_tree_option = "index type tree does not support option"
    with pytest.raises(TarantoolError, match=invalid_tree_option):
        i1.sql(""" create index i5 on t using tree (c) with (distance = euclid) """)
    with pytest.raises(TarantoolError, match=invalid_tree_option):
        i1.sql(""" create index i6 on t using tree (c) with (dimension = 42) """)

    # RTree indexes can't be created via SQL at the moment as they require array columns
    # that are not supported yet.
    non_array_rtree = "index type rtree does not support column type"
    with pytest.raises(TarantoolError, match=non_array_rtree):
        i1.sql(""" create index i11 on t using rtree (b) """)

    # Fail to create an rtree index from nullable columns.
    nullable_rtree = "index type rtree does not support nullable columns"
    with pytest.raises(TarantoolError, match=nullable_rtree):
        i1.sql(""" create index i12 on t using rtree (c) """)

    # Fail to create an rtree index with wrong options.
    invalid_rtree_option = "index type rtree does not support option"
    with pytest.raises(TarantoolError, match=invalid_rtree_option):
        i1.sql(""" create index i13 on t using rtree (b) with (hint = true) """)

    # Successful bitset index creation.
    ddl = i1.sql(""" create index i14 on t using bitset (b) """)

    # Fail to create a bitset index from nullable columns.
    nullable_bitset = "index type bitset does not support nullable columns"
    with pytest.raises(TarantoolError, match=nullable_bitset):
        i1.sql(""" create index i15 on t using bitset (c) """)

    # Fail to create unique bitset index.
    unique_bitset = "index type bitset does not support unique indexes"
    with pytest.raises(TarantoolError, match=unique_bitset):
        i1.sql(""" create unique index i16 on t using bitset (a) """)

    # Fail to create bitset index with column types other then string, number or varbinary.
    invalid_bitset = "index type bitset does not support column type"
    with pytest.raises(TarantoolError, match=invalid_bitset):
        i1.sql(""" create index i17 on v using bitset (a) """)

    # Successful hash index creation.
    ddl = i1.sql(""" create unique index i17 on t using hash (a) """)
    assert ddl["row_count"] == 1

    # Fail to create a non-unique hash index.
    non_unique_hash = "index type hash does not support non-unique indexes"
    with pytest.raises(TarantoolError, match=non_unique_hash):
        i1.sql(""" create index i18 on t using hash (c) """)

    # Fail to create an index on memtex table with vinyl options.
    invalid_memtx = "table engine memtx does not support option"
    with pytest.raises(TarantoolError, match=invalid_memtx):
        i1.sql(""" create index i7 on t (b) with (page_size = 42) """)
    with pytest.raises(TarantoolError, match=invalid_memtx):
        i1.sql(""" create index i8 on t (b) with (range_size = 42) """)
    with pytest.raises(TarantoolError, match=invalid_memtx):
        i1.sql(""" create index i9 on t (b) with (run_count_per_level = 42) """)
    with pytest.raises(TarantoolError, match=invalid_memtx):
        i1.sql(""" create index i10 on t (b) with (run_size_ratio = 0.1) """)

    # Successful index drop.
    ddl = i1.sql(""" drop index i0 """)
    assert ddl["row_count"] == 1

    # Check that the index is actually dropped.
    data = i1.sql(""" select * from "_pico_index" where "name" = 'i0' """)
    assert data == []

    # Drop non-existing index.
    with pytest.raises(TarantoolError, match="index i0 does not exist"):
        ddl = i1.sql(""" drop index i0 option (timeout = 3) """)

    ddl = i1.sql(""" create index i19 on t (b)""")
    assert ddl["row_count"] == 1
    with pytest.raises(TarantoolError, match="index i19 already exists"):
        i1.sql(""" create index i19 on v (b)""")


def test_order_by(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        create table "null_t" ("na" integer not null, "nb" int, "nc" int, primary key ("na"))
        distributed by ("na")
    """
    )
    assert ddl["row_count"] == 1

    dml = i1.sql(
        """
        insert into "null_t" values
            (1, 2, 1),
            (2, NULL, 3),
            (3, 2, 3),
            (4, 3, 1),
            (5, 1, 5),
            (6, -1, 3),
            (7, 1, 1),
            (8, NULL, -1)
    """
    )
    assert dml["row_count"] == 8

    expected_ordering_by_1 = [
        [1, 2, 1],
        [2, None, 3],
        [3, 2, 3],
        [4, 3, 1],
        [5, 1, 5],
        [6, -1, 3],
        [7, 1, 1],
        [8, None, -1],
    ]
    data = i1.sql(""" select * from "null_t" order by "na" """)
    assert data == expected_ordering_by_1
    data = i1.sql(""" select * from "null_t" order by 1 """)
    assert data == expected_ordering_by_1
    data = i1.sql(""" select * from "null_t" order by 1 asc """)
    assert data == expected_ordering_by_1
    data = i1.sql(""" select * from "null_t" order by "na" asc """)
    assert data == expected_ordering_by_1
    data = i1.sql(""" select * from "null_t" order by 1, 2 """)
    assert data == expected_ordering_by_1

    expected_ordering_by_2 = [
        [2, None, 3],
        [8, None, -1],
        [6, -1, 3],
        [5, 1, 5],
        [7, 1, 1],
        [1, 2, 1],
        [3, 2, 3],
        [4, 3, 1],
    ]
    data = i1.sql(""" select * from "null_t" order by "nb" """)
    assert data == expected_ordering_by_2
    data = i1.sql(""" select * from "null_t" order by "nb" asc """)
    assert data == expected_ordering_by_2
    data = i1.sql(""" select * from "null_t" order by 2 """)
    assert data == expected_ordering_by_2

    data = i1.sql(""" select * from "null_t" order by 1 desc """)
    assert data == [
        [8, None, -1],
        [7, 1, 1],
        [6, -1, 3],
        [5, 1, 5],
        [4, 3, 1],
        [3, 2, 3],
        [2, None, 3],
        [1, 2, 1],
    ]

    expected_ordering_by_2_desc = [
        [4, 3, 1],
        [1, 2, 1],
        [3, 2, 3],
        [5, 1, 5],
        [7, 1, 1],
        [6, -1, 3],
        [2, None, 3],
        [8, None, -1],
    ]
    data = i1.sql(""" select * from "null_t" order by "nb" desc """)
    assert data == expected_ordering_by_2_desc
    data = i1.sql(""" select * from "null_t" order by "nb" * 2 + 42 * "nb" desc """)
    assert data == expected_ordering_by_2_desc

    data = i1.sql(""" select * from "null_t" order by "nb" desc, "na" desc """)
    assert data == [
        [4, 3, 1],
        [3, 2, 3],
        [1, 2, 1],
        [7, 1, 1],
        [5, 1, 5],
        [6, -1, 3],
        [8, None, -1],
        [2, None, 3],
    ]

    data = i1.sql(""" select * from "null_t" order by 2 asc, 1 desc, 2 desc, 1 asc """)
    assert data == [
        [8, None, -1],
        [2, None, 3],
        [6, -1, 3],
        [7, 1, 1],
        [5, 1, 5],
        [3, 2, 3],
        [1, 2, 1],
        [4, 3, 1],
    ]

    data = i1.sql(""" select name as relname from _pico_instance order by relname """)
    assert data == [
        ["default_1_1"],
    ]

    data = i1.sql(""" select name as relname from _pico_instance order by 1 """)
    assert data == [
        ["default_1_1"],
    ]

    data = i1.sql(""" select nb as new_nb from null_t order by new_nb """)
    assert data == [
        [None],
        [None],
        [-1],
        [1],
        [1],
        [2],
        [2],
        [3],
    ]

    data = i1.sql(""" select nb as new_nb from null_t order by 1 """)
    assert data == [
        [None],
        [None],
        [-1],
        [1],
        [1],
        [2],
        [2],
        [3],
    ]

    data = i1.sql(
        """ SELECT PI.name instance_name,
                            PI.current_state,
                            PI.target_state,
                            PI.tier
                        FROM _pico_peer_address PA
                        JOIN _pico_instance PI ON PA.raft_id = PI.raft_id
                        WHERE connection_type = 'iproto'
                        ORDER BY instance_name
 """
    )
    assert data == [
        [
            "default_1_1",
            ["Online", 1],
            ["Online", 1],
            "default",
        ],
    ]

    with pytest.raises(
        TarantoolError,
        match="Ordering index \\(4\\) is bigger than child projection output length \\(3\\)",
    ):
        i1.sql(""" select * from "null_t" order by 4 """)
    with pytest.raises(
        TarantoolError,
        match="Using parameter as a standalone ORDER BY expression doesn't influence sorting",
    ):
        i1.sql(
            """
            with define_parameter_types as (select $1::int)
            select * from "null_t" order by $1
            """
        )


def test_cte(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    # Initialize sharded table
    ddl = i1.sql(
        """
        create table t (a int not null, b int, primary key (a))
        distributed by (b)
        option (timeout = 3)
        """
    )
    assert ddl["row_count"] == 1
    data = i1.sql(
        """
        insert into t values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)
        """
    )
    assert data["row_count"] == 5

    # Initialize global table
    ddl = i1.sql(
        """
        create table g (a int not null, b int, primary key (a))
        distributed globally
        option (timeout = 3)
        """
    )
    assert ddl["row_count"] == 1

    dml = i1.sql(
        """
        insert into g values (1, 1), (2, 2), (3, 3), (4, 4)
        """
    )
    assert dml["row_count"] == 4

    # basic CTE
    data = i1.sql(
        """
        with cte (b) as (select a from t where a > 2)
        select * from cte
        """
    )
    assert data == [[3], [4], [5]]

    # nested CTE
    data = i1.sql(
        """
        with cte1 (b) as (select a from t where a > 2),
             cte2 as (select b from cte1)
        select * from cte2
        """
    )
    assert data == [[3], [4], [5]]

    # reuse CTE
    data = i1.sql(
        """
        with cte (b) as (select a from t where a = 2)
        select b from cte
        union all
        select b + b from cte
        """
    )
    assert data == [[2], [4]]

    # global CTE
    data = i1.sql(
        """
        with cte (b) as (select a from g where a < 4)
        select * from cte
        """
    )
    assert data == [[1], [2], [3]]

    # CTE with parameters
    data = i1.sql(
        """
        with cte (b) as (select a from t where a > ?)
        select * from cte
        """,
        3,
    )
    assert data == [[4], [5]]

    with pytest.raises(TarantoolError, match="expected 2 values for parameters, got 1"):
        i1.sql(
            """
        with
        define_parameter_types as (select $1 = $2),
        cte (t, kek) as (select $1, $2)
        select $1, * from cte
        """,
            True,
        )

    # join sharded table with CTE
    data = i1.sql(
        """
        with cte (b) as (select a from t where a > 2)
        select t.a, cte.b from t join cte on t.a = cte.b
        """
    )
    assert data == [[3, 3], [4, 4], [5, 5]]

    # join global table with CTE
    data = i1.sql(
        """
        with cte (b) as (select a from g where a < 4)
        select g.a, cte.b from g join cte on g.a = cte.b
        """
    )
    assert data == [[1, 1], [2, 2], [3, 3]]

    # CTE in aggregate
    data = i1.sql(
        """
        with cte (b) as (select a from t where a between 2 and 4)
        select count(b) from cte
        """
    )
    assert data == [[3]]

    # CTE in subquery
    data = i1.sql(
        """
        with cte (b) as (select a from t where a in (2, 5))
        select * from t where a in (select b from cte)
        """
    )
    assert data == [[2, 2], [5, 5]]

    # values in CTE
    data = i1.sql(
        """
        with cte (b) as (values (1), (2), (3))
        select * from cte order by 1
        """
    )
    assert data == [[1], [2], [3]]

    # union all in CTE
    data = i1.sql(
        """
        with cte1 (b) as (values (1), (2), (3)),
        cte2 (b) as (select a from t where a = 1 union all select * from cte1)
        select * from cte2 order by 1
        """
    )
    assert data == [[1], [1], [2], [3]]

    # join in CTE
    data = i1.sql(
        """
        with cte (c) as (
            select t.a from t join g on t.a = g.a
            join t as t2 on t2.a = t.a
            where t.a = 1
        )
        select * from cte
        """
    )
    assert data == [[1]]

    # order by in CTE
    data = i1.sql(
        """
        with cte (b) as (select a from t order by a desc)
        select * from cte
        """
    )
    assert data == [[5], [4], [3], [2], [1]]

    # randomly distributed CTE used multiple times
    data = i1.sql(
        """
        with cte (b) as (select a from t where a > 3)
        select t.c from (select count(*) as c from cte c1 join cte c2 on true) t
        join cte on true
        """
    )
    assert data == [[4], [4]]

    # CTE with segment distribution used multiple times
    data = i1.sql(
        """
        with cte (b) as (select a from t where a = 1)
        select t.c from (select count(*) as c from cte c1 join cte c2 on true) t
        join cte on true
        """
    )
    assert data == [[1]]

    # CTE with global distribution used multiple times
    data = i1.sql(
        """
        with cte (b) as (select a from g where a = 1)
        select t.c from (select count(*) as c from cte c1 join cte c2 on true) t
        join cte on true
        """
    )
    assert data == [[1]]

    # CTE with values used multiple times
    data = i1.sql(
        """
        with cte (b) as (values (1))
        select t.c from (select count(*) as c from cte c1 join cte c2 on true) t
        join cte on true
        """
    )
    assert data == [[1]]

    # Ensure typing errors in cte do not result in panic.
    # https://git.picodata.io/core/picodata/-/issues/1441

    with pytest.raises(
        TarantoolError,
        match="COALESCE types text and unsigned cannot be matched",
    ):
        i1.sql(""" with cte as (select coalesce('kek'::text,1487)) select * from cte; """)

    with pytest.raises(
        TarantoolError,
        match="CASE/THEN types unsigned and bool cannot be matched",
    ):
        i1.sql("with cte as (select case when true then 1 else false end) select * from cte;")

    with pytest.raises(
        TarantoolError,
        match=r"could not resolve operator overload for \+\(unsigned, bool\)",
    ):
        i1.sql(""" with cte as (select 1 + true) select * from cte; """)


def test_unique_index_name_for_sharded_table(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]
    table_names = ["t", "t2"]

    # Initialize two sharded table
    for table_name in table_names:
        ddl = i1.sql(
            f"""
            create table {table_name} (a int not null, b int, primary key (a))
            distributed by (b)
            option (timeout = 3)
            """
        )
        assert ddl["row_count"] == 1

    for table_name, other_table_name in zip(table_names, reversed(table_names)):
        with pytest.raises(
            TarantoolError,
            match="index bucket_id already exists",
        ):
            # try to create existing index
            i1.sql(
                f""" create index "bucket_id"
                on "{table_name}" (a) option (timeout = 3) """
            )

        with pytest.raises(
            TarantoolError,
            match="index bucket_id already exists",
        ):
            # try to create non existing index with existing name
            i1.sql(
                f""" create index "bucket_id"
                on "{other_table_name}" (a) option (timeout = 3) """
            )

        # ensure that index on field bucket_id of sharded table exists in space _index
        assert i1.eval(f"""return box.space.{table_name}.index.bucket_id""") is not None
        assert i1.eval(f"""return box.space.{other_table_name}.index.bucket_id""") is not None


def test_metadata(instance: Instance):
    ddl = instance.sql(
        """
        create table t (a int not null, primary key (a))
        distributed by (a)
        option (timeout = 3)
        """
    )
    assert ddl["row_count"] == 1

    data = instance.sql(""" select * from t """, strip_metadata=False)
    assert data["metadata"] == [{"name": "a", "type": "integer"}]

    # Previously, we returned metadata from tarantool, not from the plan.
    # Sometimes it led to disinformation, for example (pay attention to the types):
    #
    # picodata> select 1 from g
    # ---
    # - metadata:
    #   - {'name': 'col_1', 'type': 'integer'}
    #   rows: []
    # ...
    #
    # picodata> explain select 1 from g
    # ---
    # - - projection (1::unsigned -> "col_1")
    #   - '    scan "G"'
    #   - 'execution options:'
    #   -     sql_vdbe_opcode_max = 45000
    #   -     sql_motion_row_max = 5000
    # ...
    data = instance.sql(""" select 1 from t """, strip_metadata=False)
    assert data["metadata"] == [{"name": "col_1", "type": "unsigned"}]

    data = instance.sql(""" select -2 + 1 from t """, strip_metadata=False)
    assert data["metadata"] == [{"name": "col_1", "type": "integer"}]

    # Test that we can infer the actual type of min/max functions, which depends on the argument.
    data = instance.sql(""" select min(a) from t """, strip_metadata=False)
    assert data["metadata"] == [{"name": "col_1", "type": "integer"}]

    data = instance.sql(""" select min(a) + max(a) from t """, strip_metadata=False)
    assert data["metadata"] == [{"name": "col_1", "type": "integer"}]

    # verify that we've fixed the problem from
    # https://git.picodata.io/picodata/picodata/sbroad/-/issues/632
    ddl = instance.sql(
        """
        CREATE TABLE "testing_space" ( "id" INTEGER NOT NULL, PRIMARY KEY ("id") )
        DISTRIBUTED BY ("id")
        """
    )
    assert ddl["row_count"] == 1

    ddl = instance.sql(
        """
        CREATE TABLE "space_simple_shard_key" ( "id" INTEGER NOT NULL, PRIMARY KEY ("id") )
        DISTRIBUTED BY ("id")
        """
    )
    assert ddl["row_count"] == 1

    ddl = instance.sql(
        """
        CREATE TABLE "space_simple_shard_key_hist" ( "id" INTEGER NOT NULL, PRIMARY KEY ("id") )
        DISTRIBUTED BY ("id")
        """
    )
    assert ddl["row_count"] == 1

    data = instance.sql(
        """
        SELECT t1."id" as "id" FROM "testing_space" as t1
        JOIN "space_simple_shard_key" as t2
        ON t1."id" = t2."id"
        JOIN "space_simple_shard_key_hist" as t3
        ON t2."id" = t3."id"
        WHERE t1."id" = 1
        """,
        strip_metadata=False,
    )

    # It used to return "T1.id" column name in metadata,
    # though it should return "id" (because of an alias).
    assert data["metadata"] == [{"name": "id", "type": "integer"}]


def test_create_role_and_user_with_empty_name(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    with pytest.raises(
        TarantoolError,
        match="expected non empty name",
    ):
        i1.sql('CREATE ROLE ""')

    with pytest.raises(
        TarantoolError,
        match="expected non empty name",
    ):
        i1.sql("""CREATE USER "" WITH PASSWORD 'P@ssw0rd' USING chap-sha1""")

    i1.sql("""CREATE USER "andy" WITH PASSWORD 'P@ssw0rd' USING chap-sha1""")

    # rename existing user to empty name
    with pytest.raises(
        TarantoolError,
        match="expected non empty name",
    ):
        i1.sql("""ALTER USER "andy" RENAME TO "" """)


def test_limit(cluster: Cluster):
    cluster.deploy(instance_count=3)
    [i1, i2, i3] = cluster.instances

    # Make sure buckets are balanced before routing via bucket_id to eliminate
    # flakiness due to bucket rebalancing
    def wait_balanced():
        for i in cluster.instances:
            cluster.wait_until_instance_has_this_many_active_buckets(i, 1000)

    wait_balanced()

    ###########################
    # Tests with sharded tables
    ###########################

    i1.sql(
        """
        CREATE TABLE "t" ("id" INTEGER, "n" INTEGER, PRIMARY KEY("id"))
        DISTRIBUTED BY ("id")
        """
    )

    i1.sql(
        """
        INSERT INTO "t"
            VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7)
        """
    )
    wait_balanced()
    data = i1.retriable_sql(""" SELECT * FROM "t" """)
    assert len(data) == 7

    # Test that LIMIT affects the number of rows returned.
    data = i1.retriable_sql(""" SELECT * FROM "t" LIMIT 0 """)
    assert len(data) == 0

    data = i2.retriable_sql(""" SELECT * FROM "t" LIMIT 1 """)
    assert len(data) == 1

    data = i3.retriable_sql(""" SELECT * FROM "t" LIMIT 2 """)
    assert len(data) == 2

    # Limit exceeding the number of rows returned has no effect.
    data = i1.retriable_sql(""" SELECT * FROM "t" LIMIT 100 """)
    assert len(data) == 7

    # Limit all has no effect.
    data = i2.retriable_sql(""" SELECT * FROM "t" LIMIT ALL """)
    assert len(data) == 7

    # Limit null is the same as limit all.
    data = i3.retriable_sql(""" SELECT * FROM "t" LIMIT NULL """)
    assert len(data) == 7

    # LIMIT + UNION ALL.
    data = i1.retriable_sql(
        """
        SELECT * FROM "t"
        UNION ALL
        SELECT * FROM "t"
        LIMIT 1
        """
    )
    assert len(data) == 1

    # LIMIT + ORDER BY.
    data = i2.retriable_sql(
        """
        SELECT "id" FROM "t"
        ORDER BY "id"
        LIMIT 5
        """
    )
    # Verify the order.
    assert data == [[1], [2], [3], [4], [5]]

    # LIMIT + COUNT.
    data = i3.retriable_sql(
        """
        SELECT count(*) FROM "t"
        LIMIT 2
        """
    )
    assert len(data) == 1

    # LIMIT + COUNT + GROUP BY.
    data = i2.retriable_sql(
        """
        SELECT "id", count(*) FROM "t"
        GROUP BY "id"
        LIMIT 5
        """
    )
    assert len(data) == 5

    # LIMIT + COUNT + GROUP BY + HAVING.
    data = i2.retriable_sql(
        """
        SELECT "id", count(*) FROM "t"
        GROUP BY "id"
        HAVING "id" > 2
        LIMIT 3
        """
    )
    assert len(data) == 3

    # Create a table for join.
    i1.sql(
        """
        CREATE TABLE "w" ("id" INTEGER, "n" INTEGER, PRIMARY KEY("id"))
        DISTRIBUTED BY ("id")
        """
    )
    i1.sql(""" INSERT INTO "w" VALUES (-1, 1), (-2, 2), (-3, 3), (-4, 4) """)
    wait_balanced()

    # LIMIT + JOIN.
    data = i2.retriable_sql(
        """
        SELECT "w"."n" FROM "t" JOIN "w" ON "t"."n" = "w"."n"
        LIMIT 3
        """
    )
    assert len(data) == 3

    # LIMIT in a subqeury.
    data = i2.retriable_sql(
        """
        SELECT * FROM (SELECT * FROM "t" LIMIT 1)
        """
    )
    assert len(data) == 1

    # LIMIT + LIMIT in a subquery
    data = i2.retriable_sql(
        """
        SELECT * FROM (SELECT * FROM "t" LIMIT 2) LIMIT 1
        """
    )
    assert len(data) == 1

    # Create a huge distributed table.
    i1.retriable_sql(
        """
        CREATE TABLE "huge" ("id" INTEGER, PRIMARY KEY("id"))
        DISTRIBUTED BY ("id")
        """
    )
    # Fill in the table with values.
    for n in range(0, 5100, 4):
        i1.sql(
            """
            INSERT INTO "huge" VALUES
                ($1),
                (CAST($1 AS INT) + 1),
                (CAST($1 AS INT) + 2),
                (CAST($1 AS INT) + 3)
            """,
            n,
        )
    wait_balanced()

    # Read without LIMIT should fail.
    with pytest.raises(TarantoolError, match="Exceeded maximum number of rows"):
        i1.retriable_sql(""" SELECT * FROM "huge" """)

    # Test read with LIMIT.
    data = i1.retriable_sql(""" SELECT * FROM "huge" LIMIT 1000 """)
    assert len(data) == 1000

    data = i1.retriable_sql(""" SELECT * FROM (SELECT * FROM "huge" LIMIT 1000) """)
    assert len(data) == 1000

    data = i1.retriable_sql(""" SELECT * FROM "huge" UNION ALL SELECT * FROM "huge" LIMIT 1000 """)
    assert len(data) == 1000

    ##########################
    # Tests with global tables
    ##########################

    i2.sql(
        """
        CREATE TABLE "g" ("id" INTEGER, "n" INTEGER, PRIMARY KEY("id"))
        DISTRIBUTED GLOBALLY
        """
    )

    i1.sql(
        """
        INSERT INTO "g"
            VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7)
        """
    )
    data = i1.retriable_sql(""" SELECT * FROM "g" """)
    assert len(data) == 7

    data = i3.retriable_sql(""" SELECT * FROM "g" LIMIT 2 """)
    assert len(data) == 2

    # LIMIT + ORDER BY.
    data = i2.retriable_sql(
        """
        SELECT "id" FROM "g"
        ORDER BY "id"
        LIMIT 5
        """
    )
    # Verify the order.
    assert data == [[1], [2], [3], [4], [5]]

    # LIMIT + COUNT.
    data = i3.retriable_sql(
        """
        SELECT count(*) FROM "t"
        LIMIT 2
        """
    )
    assert len(data) == 1

    # LIMIT + COUNT + GROUP BY + HAVING.
    data = i2.retriable_sql(
        """
        SELECT "id", count(*) FROM "g"
        GROUP BY "id"
        HAVING "id" > 2
        LIMIT 3
        """
    )
    assert len(data) == 3

    ######################################
    # Tests with global and sharded tables
    ######################################

    # LIMIT + UNION ALL
    data = i1.retriable_sql(
        """
        SELECT * FROM "t"
        UNION ALL
        SELECT * FROM "g"
        LIMIT 10
        """
    )
    assert len(data) == 10

    # # LIMIT + JOIN.
    data = i3.retriable_sql(
        """
        SELECT "t"."n" FROM "t" JOIN "g" ON "t"."n" = "g"."n"
        LIMIT 4
        """
    )
    assert len(data) == 4

    # LIMIT + NOT IN.
    data = i3.retriable_sql(
        """
        SELECT "n" FROM "t" WHERE "n" NOT IN (SELECT "n" FROM "g" ORDER BY "n" LIMIT 3)
        LIMIT 3
        """
    )
    assert len(data) == 3

    ##################
    # Tests with CTEs
    ##################

    # Initialize sharded table
    ddl = i1.sql(
        """
        create table t1 (a int not null, b int, primary key (a))
        distributed by (b)
        option (timeout = 3)
        """
    )
    assert ddl["row_count"] == 1
    data = i1.sql(
        """
        insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)
        """
    )
    assert data["row_count"] == 5
    wait_balanced()

    # LIMIT + basic CTE
    data = i1.retriable_sql(
        """
        with cte (b) as (select a from t1 where a > 1 limit 3)
        select * from cte
        """
    )
    assert len(data) == 3

    # LIMIT + basic CTE
    data = i1.retriable_sql(
        """
        with cte (b) as (select a from t1 where a > 1)
        select * from cte limit 3
        """
    )
    assert len(data) == 3

    # LIMIT + nested CTE
    data = i1.retriable_sql(
        """
        with cte1 (b) as (select a from t1 where a > 2 limit 2),
             cte2 as (select b from cte1)
        select * from cte2
        """
    )
    assert len(data) == 2

    # LIMIT + reuse CTE
    data = i1.retriable_sql(
        """
        with cte (b) as (select a from t1 where a > 2 limit 2)
        select b from cte
        union all
        select b + b from cte
        """
    )
    assert len(data) == 4

    # LIMIT + global CTE
    data = i1.sql(
        """
        with cte as (select "n" from "g" where "n" < 5 order by "n" limit 3)
        select * from cte
        """
    )
    assert data == [[1], [2], [3]]

    # LIMIT + global CTE
    data = i2.sql(
        """
        with cte as (select "n" from "g" where "n" < 5 order by "n" limit 3)
        select * from cte
        """
    )
    assert data == [[1], [2], [3]]


def test_alter_system_property(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    non_default_prop = [
        ("auth_password_length_min", "", 10),
        ("auth_password_enforce_digits", "", True),
        ("auth_password_enforce_uppercase", "", True),
        ("auth_password_enforce_lowercase", "", True),
        ("auth_password_enforce_specialchars", "", False),
        ("governor_auto_offline_timeout", "", 12),
        ("auth_login_attempt_max", "", 4),
        ("pg_statement_max", "", 1024),
        ("pg_portal_max", "", 1024),
        ("raft_snapshot_chunk_size_max", "", 1500),
        ("raft_snapshot_read_view_close_timeout", "", 12312.4),
        ("auth_password_enforce_uppercase", "", False),
        ("auth_password_enforce_lowercase", "", False),
        ("auth_password_enforce_specialchars", "", True),
        ("auth_login_attempt_max", "", 8),
        ("pg_statement_max", "", 4096),
        ("pg_portal_max", "", 2048),
        ("governor_raft_op_timeout", "", 10),
        ("governor_common_rpc_timeout", "", 10),
        ("governor_plugin_rpc_timeout", "", 20),
    ]

    default_prop = []
    for index, (prop, scope, value) in enumerate(non_default_prop):
        data = i1.sql(f""" select * from "_pico_db_config" where "key" = '{prop}' """)
        default_prop.append(data[0][2])

        # check simple setting
        data = i1.sql(f""" alter system set "{prop}" to {value} """)
        assert data["row_count"] == 1
        data = i1.sql(""" select * from "_pico_db_config" where "key" = ? """, prop)
        assert data[0][2] == value

        # change back to non default value for check of reset
        data = i1.sql(f""" alter system set "{prop}" to default """)
        assert data["row_count"] == 1
        data = i1.sql(""" select * from "_pico_db_config" where "key" = ? """, prop)
        assert data[0][2] == default_prop[index]

        # change back to
        data = i1.sql(f""" alter system set "{prop}" to {value} """)
        assert data["row_count"] == 1
        data = i1.sql(""" select * from "_pico_db_config" where "key" = ? """, prop)
        assert data[0][2] == value

        # check reset to default
        data = i1.sql(f""" alter system reset "{prop}" """)
        assert data["row_count"] == 1
        data = i1.sql(""" select * from "_pico_db_config" where "key" = ? """, prop)
        assert data[0][2] == default_prop[-1]

        # change back to non default value for later check of reset all
        data = i1.sql(f""" alter system set "{prop}" to {value} """)
        assert data["row_count"] == 1
        data = i1.sql(""" select * from "_pico_db_config" where "key" = ? """, prop)
        assert data[0][2] == value

    # check reset all
    data = i1.sql(""" alter system reset all """)
    assert data["row_count"] == 1
    for (prop, _, _), default in zip(non_default_prop, default_prop):
        data = i1.sql(""" select * from "_pico_db_config" where "key" = ? """, prop)
        assert data[0][2] == default


def test_alter_system_property_errors(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    # check valid insertion (int)
    data = i1.sql(""" select * from "_pico_db_config" where "key" = 'governor_auto_offline_timeout' """)
    assert data == [["governor_auto_offline_timeout", "", 30]]
    dml = i1.sql(
        """
        alter system set "governor_auto_offline_timeout" to 3 for all tiers
        """
    )
    assert dml["row_count"] == 1
    data = i1.sql(""" select * from "_pico_db_config" where "key" = 'governor_auto_offline_timeout' """)
    assert data == [["governor_auto_offline_timeout", "", 3]]

    # check valid insertion (bool)
    data = i1.sql(""" select * from "_pico_db_config" where "key" = 'auth_password_enforce_digits' """)
    assert data == [["auth_password_enforce_digits", "", True]]
    dml = i1.sql(
        """
        alter system set "auth_password_enforce_digits" to false for all tiers
        """
    )
    assert dml["row_count"] == 1
    data = i1.sql(""" select * from "_pico_db_config" where "key" = 'auth_password_enforce_digits' """)
    assert data == [["auth_password_enforce_digits", "", False]]

    # such property does not exist
    with pytest.raises(TarantoolError, match="unknown parameter: 'invalid_parameter_name'"):
        dml = i1.sql(
            """
            alter system set "invalid_parameter_name" to 3 for all tiers
            """
        )

    # property expects different value type
    with pytest.raises(
        TarantoolError,
        match="invalid value for 'auth_password_enforce_digits' expected boolean, got unsigned.",
    ):
        dml = i1.sql(
            """
            alter system set "auth_password_enforce_digits" to 3 for all tiers
            """
        )

    # such property exists but must not be allowed to be changed through alter system
    with pytest.raises(TarantoolError, match="unknown parameter: 'next_schema_version'"):
        dml = i1.sql(
            """
            alter system set "next_schema_version" to 3 for all tiers
            """
        )

    # timeout values cannot be negative
    for param in ["governor_auto_offline_timeout", "governor_raft_op_timeout"]:
        with pytest.raises(TarantoolError) as e:
            dml = i1.sql(
                f"""
                ALTER SYSTEM SET {param} TO -1
                """
            )
        assert e.value.args[1] == "timeout value cannot be negative"


def test_global_dml_cas_conflict(cluster: Cluster):
    # Number of update operations per worker
    N = 100
    # Number of parallel workers running update operations
    K = 4
    # Add one for raft leader (not going to be a worker)
    instance_count = K + 1
    [i1, *_] = cluster.deploy(instance_count=instance_count)
    workers = [i for i in cluster.instances if i != i1]

    i1.sql(
        """
        CREATE TABLE test_table (id UNSIGNED PRIMARY KEY, counter UNSIGNED) DISTRIBUTED GLOBALLY
        """
    )
    i1.sql(""" INSERT INTO test_table VALUES (0, 0) """)

    test_sql = """ UPDATE test_table SET counter = counter + 1 WHERE id = 0 """
    prepare = """
        local N, test_sql = ...
        local fiber = require 'fiber'
        local log = require 'log'
        function test_body()
            while not box.space.test_table do
                fiber.sleep(.1)
            end

            local i = 0
            local stats = { n_retries = 0 }
            while i < N do
                log.info("UPDATE #%d running...", i)
                while true do
                    local ok, err = pico.sql(test_sql)
                    if err == nil then break end
                    log.error("UPDATE #%d failed: %s, retry", i, err)
                    stats.n_retries = stats.n_retries + 1
                    pico.raft_wait_index(pico.raft_get_index() + 1, 3)
                end
                log.info("UPDATE #%d OK", i)
                i = i + 1
            end

            log.info("DONE: n_retries = %d", stats.n_retries)
            return stats
        end

        function wait_result()
            while true do
                local result = rawget(_G, 'result')
                if result ~= nil then
                    if not result[1] then
                        error(result[2])
                    end
                    return result[2]
                end
                fiber.sleep(.1)
            end
        end

        function start_test()
            fiber.create(function()
                rawset(_G, 'result', { pcall(test_body) })
            end)
        end
    """  # noqa: E501
    for i in workers:
        i.eval(prepare, N, test_sql)

    #
    # Run parallel updates to same table row from several instances simultaniously
    #
    for i in workers:
        i.call("start_test")

    #
    # Wait for the test results
    #
    for i in workers:
        stats = i.call("wait_result", timeout=20)
        # There were conflicts
        assert stats["n_retries"] > 0

    #
    # All operations were successfull
    #
    rows = i1.sql(""" SELECT * FROM test_table """)
    assert rows == [[0, N * K]]


def test_already_exists_error(instance: Instance):
    def do_sql_twice(sql):
        data = instance.sql(sql)
        assert data["row_count"] == 1

        # Should raise an error
        data = instance.sql(sql)

    with pytest.raises(TarantoolError, match="table my_table already exists"):
        do_sql_twice("CREATE TABLE my_table (id INT PRIMARY KEY)")

    ddl = instance.sql("CREATE TABLE IF NOT EXISTS my_table (id INT PRIMARY KEY)")
    assert ddl["row_count"] == 0

    with pytest.raises(TarantoolError, match="procedure my_proc already exists"):
        do_sql_twice(
            """
        CREATE PROCEDURE my_proc(INT)
        LANGUAGE SQL
        AS $$INSERT INTO my_table VALUES(?::int)$$
    """
        )

    ddl = instance.sql(
        """
    CREATE PROCEDURE IF NOT EXISTS my_proc(INT)
    LANGUAGE SQL
    AS $$INSERT INTO my_table VALUES(?::int)$$
    """
    )
    assert ddl["row_count"] == 0

    # With IF NOT EXISTS we can provide a different signature.
    instance.sql(
        """
    CREATE PROCEDURE IF NOT EXISTS my_proc(INT, INT, INT, INT)
    LANGUAGE SQL
    AS $$INSERT INTO my_table VALUES(?::int)$$
    """
    )

    with pytest.raises(TarantoolError, match="index my_index already exists"):
        do_sql_twice("CREATE INDEX my_index ON my_table (id)")

    ddl = instance.sql("CREATE INDEX IF NOT EXISTS my_index ON my_table (id)")
    assert ddl["row_count"] == 0

    with pytest.raises(TarantoolError, match="user test_user already exists"):
        do_sql_twice("CREATE USER test_user WITH PASSWORD 'Passw0rd'")

    acl = instance.sql("CREATE USER IF NOT EXISTS test_user WITH PASSWORD 'Passw0rd'")
    assert acl["row_count"] == 0

    # With IF NOT EXISTS we can provide a different password.
    acl = instance.sql("CREATE USER IF NOT EXISTS test_user WITH PASSWORD 'AnotherPassw0rd'")
    assert acl["row_count"] == 0

    with pytest.raises(TarantoolError, match="role my_role already exists"):
        do_sql_twice("CREATE ROLE my_role")

    acl = instance.sql("CREATE ROLE IF NOT EXISTS my_role")
    assert acl["row_count"] == 0

    instance.sql("CREATE USER foo WITH PASSWORD 'Passw0rd'")
    instance.sql("CREATE USER bar WITH PASSWORD 'Passw0rd'")

    with pytest.raises(TarantoolError, match="user bar already exists"):
        instance.sql("ALTER USER foo RENAME TO bar")


def test_does_not_exist_error(instance: Instance):
    with pytest.raises(TarantoolError, match="table my_table does not exist"):
        instance.sql("DROP TABLE my_table")

    ddl = instance.sql("DROP TABLE IF EXISTS t")
    assert ddl["row_count"] == 0

    with pytest.raises(TarantoolError, match="procedure my_proc does not exist"):
        instance.sql("DROP PROCEDURE my_proc")

    ddl = instance.sql("DROP PROCEDURE IF EXISTS my_proc")
    assert ddl["row_count"] == 0

    with pytest.raises(TarantoolError, match="index my_index does not exist"):
        instance.sql("DROP INDEX my_index")

    ddl = instance.sql("DROP INDEX IF EXISTS my_index")
    assert ddl["row_count"] == 0

    with pytest.raises(TarantoolError, match="user test_user does not exist"):
        instance.sql("DROP USER test_user")

    ddl = instance.sql("DROP USER IF EXISTS test_user")
    assert ddl["row_count"] == 0

    with pytest.raises(TarantoolError, match="role my_role does not exist"):
        instance.sql("DROP ROLE my_role")

    ddl = instance.sql("DROP ROLE IF EXISTS my_role")
    assert ddl["row_count"] == 0

    with pytest.raises(TarantoolError, match="user foo does not exist"):
        instance.sql("ALTER USER foo RENAME TO bar")


def test_if_exists_no_early_return(instance: Instance):
    # DROP TABLE IF EXISTS
    ddl = instance.sql("CREATE TABLE t (id INT PRIMARY KEY)")
    assert ddl["row_count"] == 1
    ddl = instance.sql("DROP TABLE t")
    assert ddl["row_count"] == 1

    # table for indexes and routines
    ddl = instance.sql("CREATE TABLE t (id INT PRIMARY KEY)")
    assert ddl["row_count"] == 1

    # DROP INDEX IF EXISTS
    ddl = instance.sql("CREATE INDEX i ON t (id)")
    assert ddl["row_count"] == 1
    ddl = instance.sql("DROP INDEX IF EXISTS i")
    assert ddl["row_count"] == 1

    # DROP PROCEDURE IF EXISTS
    ddl = instance.sql("CREATE PROCEDURE proc(INT) LANGUAGE SQL AS $$INSERT INTO t VALUES(?::int)$$")
    assert ddl["row_count"] == 1
    ddl = instance.sql("DROP PROCEDURE IF EXISTS proc")
    assert ddl["row_count"] == 1

    # DROP USER IF EXISTS
    acl = instance.sql("CREATE USER u WITH PASSWORD 'Passw0rd'")
    assert acl["row_count"] == 1
    acl = instance.sql("DROP USER IF EXISTS u")
    assert acl["row_count"] == 1

    # DROP ROLE IF EXISTS
    acl = instance.sql("CREATE ROLE r")
    assert acl["row_count"] == 1
    acl = instance.sql("DROP ROLE IF EXISTS r")
    assert acl["row_count"] == 1


def test_like(instance: Instance):
    instance.sql(
        """
        create table t (id int primary key, s string)
        using memtx
        """
    )

    instance.sql(""" insert into t values (1, 'abacaba'), (2, 'AbaC'), (3, '%__%')""")
    # test LIKE operator

    data = instance.sql(r" select '_' like '\_' and '%' like '\%' from (values (1))")
    assert data == [[True]]

    data = instance.sql(r" select '_pico_table' like '\_%' from (values (1))")
    assert data == [[True]]

    data = instance.sql(""" select s like '%a%' from t """)
    assert data == [[True], [True], [False]]

    data = instance.sql(r""" select s from t where s like '%\_\__' escape '\' """)
    assert data[0] == ["%__%"]

    data = instance.sql(r""" select s like 'AbaC%', count(*) from t group by s like 'AbaC%'""")
    assert sorted(data) == [[False, 2], [True, 1]]

    data = instance.sql(r"""select s like '%' from t""")
    assert sorted(data) == [[True], [True], [True]]

    data = instance.sql(r"""select 'a' || 'a' like 'a' || 'a' from (values (1))""")
    assert data == [[True]]

    data = instance.sql(r"""select true and 'a' like 'a' and true from (values (1))""")
    assert data == [[True]]

    data = instance.sql(r"""select false or 'a' like 'a' or false from (values (1))""")
    assert data == [[True]]

    data = instance.sql(r"""select ? like ? escape ? from (values (1))""", "%%", "x%x%", "x")
    assert data == [[True]]

    data = instance.sql(r"""select ? like ? escape 'x' from (values (1))""", "%%", "x%x%")
    assert data == [[True]]

    data = instance.sql(r"""select ? like 'x%x%' escape ? from (values (1))""", "%%", "x")
    assert data == [[True]]

    data = instance.sql(r"""select '%%' like 'x%x%' escape ? from (values (1))""", "x")
    assert data == [[True]]

    data = instance.sql(
        r"""
        select "COLUMN_1" from (values (1))
        where (values ('a_')) like (values ('%\_')) escape (values ('\'))
    """
    )
    assert data == [[1]]

    with pytest.raises(
        TarantoolError,
        match="ESCAPE expression must be a single character",
    ):
        instance.sql(r"""select s like '%' escape 'a' || 'a' from t""")

    # test ILIKE operator
    data = instance.sql("select 'AbA' ilike 'aba' from (values (1))")
    assert data[0] == [True]

    data = instance.sql("select 'aba' ilike 'aBa' from (values (1))")
    assert data[0] == [True]

    data = instance.sql("select 'ABA' ilike '%b%' from (values (1))")
    assert data[0] == [True]

    data = instance.sql("select 'ABA' ilike '_b%' from (values (1))")
    assert data[0] == [True]

    data = instance.sql(r"""select '%UU_' ilike '\%uu\_' escape '\' from (values (1))""")
    assert data[0] == [True]

    instance.sql("""create table str (n string primary key)""")
    instance.sql("insert into str values ('PRODUCT'), ('Product'), ('prod_1')")
    data = instance.sql("select n from str where n iLiKe 'prod%'")
    assert data == [["PRODUCT"], ["Product"], ["prod_1"]]


def test_select_without_scan(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    cluster.wait_until_instance_has_this_many_active_buckets(i1, 1500)
    cluster.wait_until_instance_has_this_many_active_buckets(i2, 1500)

    ddl = i1.sql("create table t (a int primary key)")
    assert ddl["row_count"] == 1

    data = i1.sql("select 1", strip_metadata=False)
    assert data["metadata"] == [
        {"name": "col_1", "type": "unsigned"},
    ]
    assert data["rows"] == [[1]]

    data = i1.sql("select 1 + 3 as foo", strip_metadata=False)
    assert data["metadata"] == [
        {"name": "foo", "type": "unsigned"},
    ]
    assert data["rows"] == [[4]]

    # check subquery with global table
    data = i1.sql("select (select name from _pico_table where name = 't') as foo")
    assert data == [["t"]]

    # check subquery with sharded table
    dml = i2.sql("insert into t values (1), (2), (3), (4)")
    assert dml["row_count"] == 4

    data = i1.sql("select (select * from t where a = 3) as bar", strip_metadata=False)
    assert data["metadata"] == [
        {"name": "bar", "type": "integer"},
    ]
    assert data["rows"] == [[3]]

    # check values
    data = i1.sql("select (values (1))")
    assert data == [[1]]

    # check recursive
    data = i1.sql("select (select 1)")
    assert data == [[1]]

    # check usage as a subquery
    data = i1.sql("select a from t where a = (select 1)")
    assert data == [[1]]

    data = i1.sql("select (select 1 as foo) from t where a = (select 1)", strip_metadata=False)
    assert data["metadata"] == [
        {"name": "col_1", "type": "unsigned"},
    ]
    assert data["rows"] == [[1]]

    # check usage with union/except
    data = i1.sql("select 1 union all select 1")
    assert data == [[1], [1]]
    data = i1.sql("select 1 union select 1")
    assert data == [[1]]
    data = i1.sql("select a from t where a = 1 union select 1")
    assert data == [[1]]
    data = i1.sql("select 1 except select 2")
    assert data == [[1]]
    data = i1.sql("select a from t where a = 1 except select 1")
    assert data == []

    # check usage with join
    data = i1.sql("select * from t join (select 1) on a = col_1")
    assert data == [[1, 1]]

    data = i1.sql("select * from (select 1) join t on a = col_1")
    assert data == [[1, 1]]

    # check usage with limit
    data = i1.sql("select 1 limit 1")
    assert data == [[1]]


def test_explain(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    cluster.wait_until_instance_has_this_many_active_buckets(i1, 1500)
    cluster.wait_until_instance_has_this_many_active_buckets(i2, 1500)

    ddl = i1.sql("create table t (a int primary key, b int)")
    assert ddl["row_count"] == 1

    # ---------------------- DQL ----------------------
    # Reading from all buckets
    lines = i1.sql("explain select a from t")
    expected_explain = """projection ("t"."a"::integer -> "a")
    scan "t"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]"""
    assert "\n".join(lines) == expected_explain

    # Reading from a single bucket => single node
    lines = i1.sql("explain select a from t where a = 1")
    expected_explain = """projection ("t"."a"::integer -> "a")
    selection "t"."a"::integer = 1::unsigned
        scan "t"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1934]"""
    assert "\n".join(lines) == expected_explain

    lines = i1.sql("explain select a from t where a = 1 and a = 2")
    expected_explain = """projection ("t"."a"::integer -> "a")
    selection ("t"."a"::integer = 1::unsigned) and ("t"."a"::integer = 2::unsigned)
        scan "t"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = []"""
    assert "\n".join(lines) == expected_explain

    # When query has motions, we estimate buckets for whole
    # plan by buckets of leaf subtrees
    lines = i1.sql("explain select t.a from t join t as t2 on t.a = t2.b")
    expected_explain = """projection ("t"."a"::integer -> "a")
    join on "t"."a"::integer = "t2"."b"::integer
        scan "t"
            projection ("t"."a"::integer -> "a", "t"."b"::integer -> "b")
                scan "t"
        motion [policy: segment([ref("b")])]
            scan "t2"
                projection ("t2"."a"::integer -> "a", "t2"."b"::integer -> "b")
                    scan "t" -> "t2"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = unknown"""
    assert "\n".join(lines) == expected_explain

    # Reading from global table
    lines = i1.sql("explain select id from _pico_table")
    expected_explain = """projection ("_pico_table"."id"::unsigned -> "id")
    scan "_pico_table"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = any"""
    assert "\n".join(lines) == expected_explain

    # Special case: motion node at the top
    lines = i1.sql("explain select id from _pico_table union select a from t")
    expected_explain = """motion [policy: full]
    union
        motion [policy: local]
            projection ("_pico_table"."id"::unsigned -> "id")
                scan "_pico_table"
        projection ("t"."a"::integer -> "a")
            scan "t"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]"""
    assert "\n".join(lines) == expected_explain

    # ---------------------- DML ----------------------
    # For non-local motion child of DML we can't estimate
    # buckets.
    lines = i1.sql("explain insert into t values (1, 2)")
    expected_explain = """insert "t" on conflict: fail
    motion [policy: segment([ref("COLUMN_1")])]
        values
            value row (data=ROW(1::unsigned, 2::unsigned))
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = unknown"""
    assert "\n".join(lines) == expected_explain

    # For local motion: we can
    lines = i1.sql("explain insert into t select a, b from t")
    expected_explain = """insert "t" on conflict: fail
    motion [policy: local segment([ref("a")])]
        projection ("t"."a"::integer -> "a", "t"."b"::integer -> "b")
            scan "t"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]"""
    assert "\n".join(lines) == expected_explain

    # Update: update non-sharding column
    lines = i1.sql("explain update t set b = 1 where b = 3")
    expected_explain = """update "t"
"b" = "col_0"
    motion [policy: local]
        projection (1::unsigned -> "col_0", "t"."a"::integer -> "col_1")
            selection "t"."b"::integer = 3::unsigned
                scan "t"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]"""
    assert "\n".join(lines) == expected_explain

    # Update sharding column
    ddl = i1.sql("create table t2 (c int primary key, d int) distributed by (d)")
    assert ddl["row_count"] == 1
    lines = i1.sql("explain update t2 set d = 1 where d = 2 or d = 2002")
    print("\n".join(lines))
    expected_explain = """update "t2"
"c" = "col_0"
"d" = "col_1"
    motion [policy: segment([])]
        projection ("t2"."c"::integer -> "col_0", 1::unsigned -> "col_1", "t2"."d"::integer -> "col_2")
            selection ("t2"."d"::integer = 2::unsigned) or ("t2"."d"::integer = 2002::unsigned)
                scan "t2"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = unknown"""  # noqa: E501
    assert "\n".join(lines) == expected_explain

    # Delete
    lines = i1.sql("explain delete from t")
    expected_explain = """delete "t"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]"""
    assert "\n".join(lines) == expected_explain

    # Dml on global table
    ddl = i1.sql("create table g (u int primary key, v int) distributed globally")
    assert ddl["row_count"] == 1
    lines = i1.sql("explain insert into g select a, b from t")
    expected_explain = """insert "g" on conflict: fail
    motion [policy: full]
        projection ("t"."a"::integer -> "a", "t"."b"::integer -> "b")
            scan "t"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]"""
    assert "\n".join(lines) == expected_explain

    lines = i1.sql("explain insert into g select u, v from g")
    expected_explain = """insert "g" on conflict: fail
    motion [policy: full]
        projection ("g"."u"::integer -> "u", "g"."v"::integer -> "v")
            scan "g"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = any"""
    assert "\n".join(lines) == expected_explain


def test_extreme_integer_values(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    cluster.wait_until_instance_has_this_many_active_buckets(i1, 3000)

    U64_MIN = 0
    U64_MAX = 18446744073709551615
    I64_MIN = -9223372036854775808
    I64_MAX = 9223372036854775807

    data = i1.sql(f"SELECT * FROM (VALUES ({I64_MAX}, {U64_MAX}), ({U64_MAX}, {I64_MAX}))")
    assert sorted(data) == [[I64_MAX, U64_MAX], [U64_MAX, I64_MAX]]

    data = i1.sql(f"SELECT {I64_MAX}, {U64_MAX} UNION SELECT {U64_MAX}, {I64_MAX}")
    assert sorted(data) == [[I64_MAX, U64_MAX], [U64_MAX, I64_MAX]]

    data = i1.sql(f"SELECT {U64_MAX} UNION ALL SELECT {U64_MAX}")
    assert sorted(data) == [[U64_MAX], [U64_MAX]]

    data = i1.sql(f"SELECT {U64_MAX} UNION SELECT {U64_MAX}")
    assert sorted(data) == [[U64_MAX]]

    data = i1.sql(f"SELECT {U64_MAX} UNION ALL SELECT {U64_MAX} LIMIT 1")
    assert sorted(data) == [[U64_MAX]]

    ddl = i1.sql(
        """
        CREATE TABLE T (uid UNSIGNED PRIMARY KEY, iid INTEGER)
        DISTRIBUTED GLOBALLY
        OPTION (TIMEOUT = 3)
    """
    )
    assert ddl["row_count"] == 1

    dml = i1.sql(f"INSERT INTO T VALUES ({U64_MIN}, {I64_MIN}), ({U64_MAX}, {I64_MAX})")
    assert dml["row_count"] == 2

    data = i1.sql("SELECT * FROM T")
    assert sorted(data) == [[U64_MIN, I64_MIN], [U64_MAX, I64_MAX]]

    data = i1.sql(f"SELECT uid FROM T WHERE uid = {U64_MIN}")
    assert data == [[U64_MIN]]

    data = i1.sql(f"SELECT uid FROM T WHERE uid = {U64_MAX}")
    assert data == [[U64_MAX]]

    data = i1.sql(f"SELECT iid FROM T WHERE iid = {I64_MIN}")
    assert data == [[I64_MIN]]

    data = i1.sql(f"SELECT iid FROM T WHERE iid = {I64_MAX}")
    assert data == [[I64_MAX]]

    data = i1.sql(f"SELECT * FROM T WHERE uid = {U64_MIN} LIMIT 1")
    assert sorted(data) == [[U64_MIN, I64_MIN]]

    data = i1.sql(f"SELECT * FROM T WHERE iid = {I64_MIN} LIMIT 1")
    assert sorted(data) == [[U64_MIN, I64_MIN]]

    data = i1.sql(f"SELECT * FROM T WHERE uid = {U64_MAX} LIMIT 1")
    assert sorted(data) == [[U64_MAX, I64_MAX]]

    data = i1.sql(f"SELECT * FROM T WHERE iid = {I64_MAX} LIMIT 1")
    assert sorted(data) == [[U64_MAX, I64_MAX]]

    with pytest.raises(TarantoolError, match="integer is overflowed"):
        i1.sql(f"SELECT uid + 1 FROM T WHERE uid = {U64_MAX} LIMIT 1")


def test_vdbe_steps_and_vtable_rows(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    cluster.wait_until_instance_has_this_many_active_buckets(i1, 1500)
    cluster.wait_until_instance_has_this_many_active_buckets(i2, 1500)

    ddl = i1.sql("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
    assert ddl["row_count"] == 1

    # Default values
    lines = i1.sql("EXPLAIN SELECT a FROM t")
    expected_explain = """projection ("t"."a"::integer -> "a")
    scan "t"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]"""
    assert "\n".join(lines) == expected_explain

    new_sql_vdbe_opcode_max = 50000
    ddl = i1.sql(f"ALTER SYSTEM SET sql_vdbe_opcode_max = {new_sql_vdbe_opcode_max}")
    assert ddl["row_count"] == 1

    # Default value for sql_vdbe_opcode_max changed
    lines = i1.sql("EXPLAIN SELECT a FROM t")
    expected_explain = f"""projection ("t"."a"::integer -> "a")
    scan "t"
execution options:
    sql_vdbe_opcode_max = {new_sql_vdbe_opcode_max}
    sql_motion_row_max = 5000
buckets = [1-3000]"""
    assert "\n".join(lines) == expected_explain

    new_sql_motion_row_max = 6000
    ddl = i1.sql(f"ALTER SYSTEM SET sql_motion_row_max = {new_sql_motion_row_max}")
    assert ddl["row_count"] == 1

    # Default value for new_sql_motion_row_max changed and
    # value for new_sql_vdbe_opcode_max hasn't changed
    lines = i1.sql("EXPLAIN SELECT a FROM t")
    expected_explain = f"""projection ("t"."a"::integer -> "a")
    scan "t"
execution options:
    sql_vdbe_opcode_max = {new_sql_vdbe_opcode_max}
    sql_motion_row_max = {new_sql_motion_row_max}
buckets = [1-3000]"""
    assert "\n".join(lines) == expected_explain

    # `option` clause has highest priority
    # also old value set with `ALTER SYSTEM`
    # still used
    # TODO: rewrite with explain like above
    new_sql_motion_row_max = 1
    error_message = """sbroad: unexpected number of values: \
Exceeded maximum number of rows (1) in virtual table: 2"""

    error_message = re.escape(error_message)

    with pytest.raises(
        TarantoolError,
        match=error_message,
    ):
        i1.sql(
            f"SELECT * FROM (VALUES (1), (2)) \
OPTION (sql_motion_row_max = {new_sql_motion_row_max})"
        )


def test_varchar_cast(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        create table t (a varchar not null, primary key (a))
        using memtx
        distributed by (a)
        """
    )
    assert ddl["row_count"] == 1

    dml = i1.sql("""insert into t values ('test_string')""")
    assert dml["row_count"] == 1

    data = i1.sql("""select cast(a as varchar(20)) from t""")
    assert data == [["test_string"]]

    # VARCHAR cast without length
    data = i1.sql("""select cast(a as varchar) from t""")
    assert data == [["test_string"]]

    # VARCHAR cast with string literal
    data = i1.sql("""select cast('direct_string' as varchar)""")
    assert data == [["direct_string"]]

    data = i1.sql(
        """select cast(a as varchar) from t where a = 'test_string'""",
        strip_metadata=False,
    )
    assert data["metadata"] == [{"name": "col_1", "type": "string"}]


def test_forbid_order_by_with_array(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1 = cluster.instances[0]

    error_message = "sbroad: invalid expression: " + "Array is not supported as a sort type for ORDER BY"
    error_message = re.escape(error_message)

    with pytest.raises(
        TarantoolError,
        match=error_message,
    ):
        i1.sql(
            """SELECT * FROM
                            (SELECT PI.name instance_name,
                                PI.current_state,
                                PI.target_state,
                                PI."uuid" instance_uuid,
                                PI.replicaset_uuid,
                                PI.tier, PA.address uri
                            FROM _pico_peer_address PA
                            JOIN _pico_instance PI ON PA.raft_id = PI.raft_id)
                        ORDER BY current_state, target_state;
        """
        )

    with pytest.raises(
        TarantoolError,
        match=error_message,
    ):
        i1.sql("""SELECT * FROM _pico_instance ORDER BY current_state""")

    with pytest.raises(
        TarantoolError,
        match=error_message,
    ):
        i1.sql("""SELECT * FROM _pico_instance ORDER BY target_state""")

    with pytest.raises(
        TarantoolError,
        match=error_message,
    ):
        i1.sql("""SELECT * FROM _pico_instance ORDER BY 6""")

    with pytest.raises(
        TarantoolError,
        match=error_message,
    ):
        i1.sql("""SELECT * FROM _pico_instance ORDER BY 7""")


def test_delete_with_filter(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        create table t (a int, b int, c int, d int, e int, primary key (c, e))
        """
    )
    assert ddl["row_count"] == 1

    dml = i1.sql("""insert into t values (1, 2, 3, 4, 5)""")
    assert dml["row_count"] == 1

    dml = i1.sql("""delete from t where true""")
    assert dml["row_count"] == 1


def test_groupby_with_column_positions(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1 = cluster.instances[0]
    i2 = cluster.instances[1]

    cluster.wait_until_instance_has_this_many_active_buckets(i1, 1500)
    cluster.wait_until_instance_has_this_many_active_buckets(i2, 1500)

    ddl = i1.sql("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
    assert ddl["row_count"] == 1
    ddl = i1.sql("CREATE TABLE s (c INT PRIMARY KEY, d INT)")
    assert ddl["row_count"] == 1

    dml = i1.sql("""INSERT INTO t VALUES (1, 2), (3, 4), (5, 6), (7, 8)""")
    assert dml["row_count"] == 4

    i1.sql("""INSERT INTO s VALUES (2, 1), (4, 3), (6, 5), (8, 7)""")
    assert dml["row_count"] == 4

    dql = i1.sql("""SELECT * FROM t GROUP BY 2, 1 ORDER BY 1""")
    assert dql == [[1, 2], [3, 4], [5, 6], [7, 8]]

    dql = i1.sql("""SELECT b FROM t GROUP BY 1 ORDER BY 1""")
    assert dql == [[2], [4], [6], [8]]

    error_message = "sbroad: invalid query: " + "GROUP BY position 7 is not in select list"
    error_message = re.escape(error_message)

    with pytest.raises(
        TarantoolError,
        match=error_message,
    ):
        i1.sql("""SELECT * FROM t GROUP BY 7""")

    error_message = "sbroad: invalid query: " + "GROUP BY position 3 is not in select list"
    error_message = re.escape(error_message)

    with pytest.raises(
        TarantoolError,
        match=error_message,
    ):
        i1.sql("""SELECT * FROM t GROUP BY 1, 2, 3, 4, 5""")

    dql = i1.sql("""SELECT * FROM t GROUP BY a, 2 ORDER BY 1""")
    assert dql == [[1, 2], [3, 4], [5, 6], [7, 8]]

    dql = i1.sql("""SELECT * FROM t JOIN s ON TRUE GROUP BY a, 2, c, 4""")
    assert sorted(dql) == [
        [1, 2, 2, 1],
        [1, 2, 4, 3],
        [1, 2, 6, 5],
        [1, 2, 8, 7],
        [3, 4, 2, 1],
        [3, 4, 4, 3],
        [3, 4, 6, 5],
        [3, 4, 8, 7],
        [5, 6, 2, 1],
        [5, 6, 4, 3],
        [5, 6, 6, 5],
        [5, 6, 8, 7],
        [7, 8, 2, 1],
        [7, 8, 4, 3],
        [7, 8, 6, 5],
        [7, 8, 8, 7],
    ]


def test_cte_with_tmp_space(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]
    i1.create_user(with_name="dmitry", with_password="Password1", with_auth="chap-sha1")

    # While executing this query, sbroad creates temporary table.
    # This test checks that user can execute that query without
    # read permission granted.
    with i1.connect(timeout=5, user="dmitry", password="Password1") as conn:
        query = conn.sql(
            """
            with a(x) as (values (0), (1)),
                b(x) as (select t1.x::text from a t1 join a t2 on true)
            select * from b;
            """,
        )
        assert query["rows"] == [["0"], ["0"], ["1"], ["1"]]


def test_scalar_function_instance_uuid(cluster: Cluster):
    i1 = cluster.add_instance()
    i2 = cluster.add_instance()
    i1_uuid = i1.uuid()
    i2_uuid = i2.uuid()

    cluster.wait_until_instance_has_this_many_active_buckets(i1, 1500)
    cluster.wait_until_instance_has_this_many_active_buckets(i2, 1500)

    ddl = i1.sql(
        """
        CREATE TABLE warehouse (
            id INTEGER NOT NULL,
            item TEXT NOT NULL,
            type TEXT NOT NULL,
            PRIMARY KEY (id)
        )
        USING memtx DISTRIBUTED BY (id)
        OPTION (TIMEOUT = 3.0);
        """
    )
    assert ddl["row_count"] == 1

    ddl = i1.sql(
        """
        CREATE TABLE items (
            id INTEGER NOT NULL,
            name TEXT NOT NULL,
            stock INTEGER,
            PRIMARY KEY (id)
        )
        USING memtx DISTRIBUTED BY (id)
        OPTION (TIMEOUT = 3.0);
        """
    )
    assert ddl["row_count"] == 1

    result = i1.sql(
        """
        INSERT INTO warehouse VALUES
            (1, 'bricks', 'heavy'),
            (2, 'bars', 'light'),
            (3, 'blocks', 'heavy'),
            (4, 'piles', 'light'),
            (5, 'panels', 'light');
        """
    )
    assert result["row_count"] == 5

    # priviliges block

    user = "dmitry"
    password = "Password1"
    i1.create_user(with_name=user, with_password=password, with_auth="chap-sha1")

    # instance_uuid avaliable to all users
    query = i1.sql(
        """
        SELECT instance_uuid()
        """,
        user,
        password,
    )
    assert query == [[i1_uuid]]

    # including admin
    query = i1.sql(
        """
        SELECT instance_uuid()
        """,
    )
    assert query == [[i1_uuid]]

    # function name format

    # with quotes
    query = i1.sql(
        """
        SELECT "instance_uuid"()
        """,
    )
    assert query == [[i1_uuid]]

    # UPPERCASED
    query = i1.sql(
        """
        SELECT "instance_uuid"()
        """,
    )
    assert query == [[i1_uuid]]

    with pytest.raises(TarantoolError, match="SQL function INSTANCE_UUID not found"):
        i1.sql(
            """
            SELECT "INSTANCE_UUID"()
            """
        )

    # volatile scalar function call isn't allowed in filters - according to query.pest there is
    # `case filter`, `delete filter`, `window function filter`, `update filter`, `selection filter`,
    # `join filter`
    # and in order by, group by

    # selection filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        i1.sql("SELECT * FROM warehouse WHERE item = instance_uuid()")

    # case filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        i1.sql(
            """
            SELECT
              CASE type
                WHEN instance_uuid()
                THEN '1'
              END
            FROM warehouse;
            """
        )

    # update filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and instance_uuid() types doesn't match
        i1.sql(
            """
            UPDATE warehouse SET item = 'chunks', TYPE = 'light' WHERE id = instance_uuid();
            """
        )

    # delete filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and instance_uuid() types doesn't match
        i1.sql(
            """
            DELETE FROM warehouse WHERE id = instance_uuid();
            """
        )

    # window filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and instance_uuid() types doesn't match
        i1.sql(
            """
           SELECT
               id,
               item,
               type,
               COUNT(*) FILTER (WHERE item = instance_uuid()) OVER (PARTITION BY type) AS box_count_per_type
           FROM warehouse
           """
        )

    # join filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and instance_uuid() types doesn't match
        i1.sql(
            """
           SELECT *

           FROM warehouse
           JOIN items
                ON warehouse.item = instance_uuid()
           """
        )

    # order by
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and instance_uuid() types doesn't match
        i1.sql(
            """
            SELECT * FROM items
            ORDER BY instance_uuid()
            """
        )

    # group by
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and instance_uuid() types doesn't match
        i1.sql(
            """
            SELECT type, COUNT(*) FROM warehouse
            WHERE id < 5
            GROUP BY instance_uuid();
            """
        )

    # misc

    # attempt to use distinct in argument list
    with pytest.raises(TarantoolError, match='"distinct" is not allowed inside VOLATILE function call'):
        i1.sql(
            """
            SELECT instance_uuid(distinct );
            """
        )

    # in case of distributed plan, function call return different
    # result on different instances
    query = i1.sql(
        """
        SELECT DISTINCT instance_uuid() FROM warehouse
        """,
        user,
        password,
    )
    assert sorted(query) == sorted([[i1_uuid], [i2_uuid]])

    # local execution
    query = i1.sql(
        """
        SELECT instance_uuid() FROM (VALUES(1, 2))
        """,
        user,
        password,
    )
    assert query == [[i1_uuid]]

    query = i2.sql(
        """
        SELECT instance_uuid() FROM (VALUES(1, 2))
        """,
        user,
        password,
    )
    assert query == [[i2_uuid]]

    # same with global tables
    i1.sql(
        """
        CREATE TABLE "global" ("id" unsigned not null, primary key("id"))
        USING MEMTX
        DISTRIBUTED GLOBALLY
        WAIT APPLIED GLOBALLY
        """
    )

    # empty tables
    query = i1.sql(
        """
        SELECT instance_uuid() FROM global
        """,
        user,
        password,
    )
    assert query == []

    query = i2.sql(
        """
        SELECT instance_uuid() FROM global
        """,
        user,
        password,
    )
    assert query == []

    query = i1.sql(
        """
        INSERT INTO global VALUES(1)
        """
    )

    # same with global tables
    query = i1.retriable_sql(
        """
        SELECT instance_uuid() FROM global
        """,
        user,
        password,
    )
    assert query == [[i1_uuid]]

    query = i2.retriable_sql(
        """
        SELECT instance_uuid() FROM global
        """,
        user,
        password,
    )
    assert query == [[i2_uuid]]


def test_alter_table_rename(cluster: Cluster):
    i1 = cluster.add_instance()
    i2 = cluster.add_instance()

    cluster.wait_until_instance_has_this_many_active_buckets(i1, 1500)
    cluster.wait_until_instance_has_this_many_active_buckets(i2, 1500)

    user = "dmitry"
    password = "Password1"
    i1.create_user(with_name=user, with_password=password, with_auth="chap-sha1")

    distributions_and_names = [("BY (id)", "sharded"), ("GLOBALLY", "global")]

    for distribution_and_name in distributions_and_names:
        distribution = distribution_and_name[0]
        table_name = distribution_and_name[1]
        new_table_name = "new_" + table_name

        # sharded table
        ddl = i1.sql(
            f"""
            CREATE TABLE {table_name} (id INTEGER NOT NULL, PRIMARY KEY (id))
            USING memtx DISTRIBUTED {distribution}
            OPTION (TIMEOUT = 3.0);
            """
        )
        assert ddl["row_count"] == 1

        result = i1.sql(
            f"""
            INSERT INTO {table_name} VALUES (1)
            """
        )
        assert result["row_count"] == 1

        # user doesn't have privilege to rename this table
        with pytest.raises(TarantoolError, match=f"Alter access to space '{table_name}' is denied for user 'dmitry'"):
            i1.sql(
                f"""
                ALTER TABLE {table_name} RENAME TO no_matter
                """,
                user=user,
                password=password,
            )

        acl = i1.sql(
            f"""
            GRANT ALTER ON TABLE {table_name} TO dmitry
            """
        )
        assert acl["row_count"] == 1
        acl = i1.sql(
            f"""
            GRANT WRITE ON TABLE {table_name} TO dmitry
            """
        )
        assert acl["row_count"] == 1
        acl = i1.sql(
            f"""
            GRANT READ ON TABLE {table_name} TO dmitry
            """
        )
        assert acl["row_count"] == 1

        # after grant it's ok to rename
        ddl = i1.sql(
            f"""
            ALTER TABLE {table_name} RENAME TO {new_table_name}
            """,
            user=user,
            password=password,
        )
        assert ddl["row_count"] == 1

        # renamed table content is present
        for intance in [i1, i2]:
            query = i1.sql(
                f"""
                SELECT * FROM {new_table_name}
                """,
                user=user,
                password=password,
            )
            assert sorted(query) == [[1]]

        result = i2.sql(
            f"""
            INSERT INTO {new_table_name} VALUES (2);
            """,
            user=user,
            password=password,
        )
        assert result["row_count"] == 1

        for intance in [i1, i2]:
            query = i1.sql(
                f"""
                SELECT * FROM {new_table_name}
                """,
                user=user,
                password=password,
            )
            assert sorted(query) == [[1], [2]]
