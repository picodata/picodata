import pytest
import re
import uuid

from conftest import Cluster, KeyDef, KeyPart, ReturnError, Retriable


def test_pico_sql(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    usage_msg = re.escape("Usage: sql(query[, params, options])")
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

    third_arg_msg = "SQL options must be a table"
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


def test_cache_works_for_dml_query(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    def assert_cache_miss(query_id):
        data = i1.eval(
            f"""
            return box.execute([[
            select "span", "query_id" from "_sql_stat"
            where "span" = '"tarantool.cache.miss.read.prepared"' and "query_id" = '{query_id}'
            ]])
        """
        )
        assert len(data["rows"]) == 1

    def assert_cache_hit(query_id):
        data = i1.eval(
            f"""
            return box.execute([[
            select "span", "query_id" from "_sql_stat"
            where "span" = '"tarantool.cache.hit.read.prepared"' and "query_id" = '{query_id}'
            ]])
        """
        )
        assert len(data["rows"]) == 1

    ddl = i1.sql(
        """
        create table t (a int not null, primary key (a))
        using memtx
        distributed by (a)
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    ddl = i1.sql(
        """
        create table not_t (b int not null, primary key (b))
        using memtx
        distributed by (b)
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    data = i1.sql(
        """
        insert into not_t values (1), (2)
    """
    )
    assert data["row_count"] == 2

    query = """
    INSERT INTO t
    SELECT b + 10
    FROM not_t
    ON CONFLICT DO REPLACE
    """
    query_id = "id"

    data = i1.sql(query, options={"traceable": True, "query_id": query_id})
    assert data["row_count"] == 2
    assert_cache_miss(query_id)

    data = i1.sql(query, options={"traceable": True, "query_id": query_id})
    assert data["row_count"] == 2
    assert_cache_hit(query_id)

    # for dml sbroad uses tarantool api,
    # so only dql part of the insert is cached.
    # Check we can reuse it for other query
    id2 = "id2"
    data = i1.sql(
        """
    SELECT b + 10
    FROM not_t
    """,
        options={"traceable": True, "query_id": id2},
    )
    assert data["rows"] == [[11], [12]]
    assert_cache_hit(id2)


def test_tracing(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    query_id = "1"
    exec_cnt = 2
    for i in range(exec_cnt):
        data = i1.sql(
            'select * from "_pico_table"',
            options={"traceable": True, "query_id": query_id},
        )
        assert len(data["rows"]) > 0

    # check we can get the most expensive query using local sql
    data = i1.eval(
        """
        return box.execute([[with recursive st as (
        select * from "_sql_stat" where "query_id" in (select qt."query_id" from qt)
                 and "parent_span" = ''
            union all
            select s.* from "_sql_stat" as s, st on s."parent_span" = st."span"
                 and s."query_id" in (select qt."query_id" from qt)
            ), qt as (
            select s."query_id" from "_sql_stat" as s
            join "_sql_query" as q
                on s."query_id" = q."query_id"
            order by s."count" desc
            limit 1
            )
            select * from st
            where "parent_span" = '';
        ]])
        """
    )
    assert len(data["rows"]) == 1
    query_id_pos = [
        i for i, item in enumerate(data["metadata"]) if item["name"] == "query_id"
    ][0]
    assert data["rows"][0][query_id_pos] == query_id
    exec_cnt_pos = [
        i for i, item in enumerate(data["metadata"]) if item["name"] == "count"
    ][0]
    assert data["rows"][0][exec_cnt_pos] == exec_cnt


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


def test_select_uuid(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    id1 = "e4166fc5-e113-46c5-8ae9-970882ca8842"
    id2 = "6f2ba4c4-0a4c-4d79-86ae-43d4f84b70e1"

    ddl = i1.sql(
        """
        create table t (a uuid not null, primary key (a))
        using memtx
        distributed by (a)
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1

    data = i1.sql("""insert into t values(?);""", id1)
    assert data["row_count"] == 1

    i1.sql("""insert into t values(?);""", id2)
    data = i1.sql("""select * from t where a = ?""", uuid.UUID(id2))
    assert data["rows"] == [[uuid.UUID(id2)]]

    data = i1.sql("""select cast(a as Text) from t""")
    assert data == {
        "metadata": [{"name": "COL_1", "type": "string"}],
        "rows": [[id2], [id1]],
    }


def test_pg_params(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        create table t (a int not null, b int, primary key (a))
        using memtx
        distributed by (a)
        option (timeout = 3)
        """
    )
    assert ddl["row_count"] == 1

    data = i1.sql("""insert into t values ($1, $1), ($2, $1)""", 1, 2)
    assert data["row_count"] == 2

    data = i1.sql("""select * from t""")
    assert data["rows"] == [[1, 1], [2, 1]]

    data = i1.sql(
        """
        select b + $1 from t
        group by b + $1
        having sum(a) > $1
    """,
        1,
    )
    assert data["rows"] == [[2]]

    data = i1.sql(
        """
        select $3, $2, $1, $2, $3 from t
        where a = $1
    """,
        1,
        2,
        3,
    )
    assert data["rows"] == [[3, 2, 1, 2, 3]]

    with pytest.raises(ReturnError, match="invalid parameters usage"):
        i1.sql(
            """
            select $1, ? from t
            """
        )


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
        select * from "_pico_table" where "name" = 'global_t'
        """,
    )
    assert len(data["rows"]) == 1


def test_read_from_system_tables(cluster: Cluster):
    instance_count = 2
    cluster.deploy(instance_count=instance_count)
    i1, _ = cluster.instances
    data = i1.sql(
        """
        select * from "_pico_property"
        """,
    )
    assert data["metadata"] == [
        {"name": "key", "type": "string"},
        {"name": "value", "type": "any"},
    ]
    assert len(data["rows"]) == 16

    data = i1.sql(
        """
        select * from "_pico_instance"
        """,
    )
    assert data["metadata"] == [
        {"name": "instance_id", "type": "string"},
        {"name": "instance_uuid", "type": "string"},
        {"name": "raft_id", "type": "unsigned"},
        {"name": "replicaset_id", "type": "string"},
        {"name": "replicaset_uuid", "type": "string"},
        {"name": "current_grade", "type": "array"},
        {"name": "target_grade", "type": "array"},
        {"name": "failure_domain", "type": "map"},
        {"name": "tier", "type": "string"},
    ]
    assert len(data["rows"]) == instance_count


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

    # TODO: remove retries and add more instances when
    # https://git.picodata.io/picodata/picodata/sbroad/-/issues/542
    # is done.
    def check_sq_with_segment_dist():
        data = i1.sql(
            """
            select b from g
            where b in (select c from s where c in (2, 10))
            """,
            timeout=2,
        )
        assert data["rows"] == [[2]]

    Retriable(rps=5, timeout=6).call(check_sq_with_segment_dist)

    def check_sq_with_single_distribution():
        data = i1.sql(
            """
            select b from g
            where b in (select sum(c) from s)
            """,
            timeout=2,
        )
        assert len(data["rows"]) == 0

    Retriable(rps=5, timeout=6).call(check_sq_with_single_distribution)

    def check_sq_with_any_distribution():
        data = i1.sql(
            """
            select b from g
            where b in (select c * 5 from s)
            """,
            timeout=2,
        )
        assert data["rows"] == [[5]]

    Retriable(rps=5, timeout=6).call(check_sq_with_any_distribution)

    def check_sqs_joined_with_or():
        # first subquery selects [1], [2], [3]
        # second subquery must add additional [4] tuple
        data = i1.sql(
            """
            select b from g
            where b in (select c from s) or a in (select count(*) from s)
            """,
            timeout=2,
        )
        assert data["rows"] == [[1], [2], [3], [4]]

    Retriable(rps=5, timeout=6).call(check_sqs_joined_with_or)

    def check_sqs_joined_with_and():
        data = i1.sql(
            """
            select b from g
            where b in (select c from s) and a in (select count(*) from s)
            """,
            timeout=2,
        )
        assert len(data["rows"]) == 0

    Retriable(rps=5, timeout=6).call(check_sqs_joined_with_and)

    def check_sq_in_join_condition_joined_with_and():
        data = i1.sql(
            """
            select c from s inner join
            (select c as c1 from s)
            on c = c1 + 3 and c in (select a from g)
            """,
            timeout=2,
        )
        assert data["rows"] == []

    Retriable(rps=5, timeout=6).call(check_sq_in_join_condition_joined_with_and)

    def check_sq_in_join_condition_joined_with_or():
        # Full join because of 'OR'
        data = i1.sql(
            """
            select min(c) from s inner join
            (select c as c1 from s)
            on c = c1 + 3 or c in (select a from g)
            """,
            timeout=2,
        )
        assert data["rows"] == [[1]]

    Retriable(rps=5, timeout=6).call(check_sq_in_join_condition_joined_with_or)

    def check_bucket_discovery_when_sqs_joined_with_or():
        data = i1.sql(
            """
            select a from g
            where b in (select c from s where c = 1) or
            b in (select c from s where c = 3)
            """,
            timeout=2,
        )
        assert data["rows"] == [[1], [3]]

    Retriable(rps=5, timeout=6).call(check_bucket_discovery_when_sqs_joined_with_or)

    def check_sqs_joined_with_or_and():
        data = i1.sql(
            """
            select a from g
            where b in (select c from s where c = 1) or
            b in (select c from s where c = 3) and
            a < (select sum(c) from s)
            """,
            timeout=2,
        )
        assert data["rows"] == [[1], [3]]

    Retriable(rps=5, timeout=6).call(check_sqs_joined_with_or_and)


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


def test_join_with_global_tbls(cluster: Cluster):
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

    for i, j in [(1, 1), (2, 1), (3, 3)]:
        index = i1.cas("insert", "G", [i, j])
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
    data = i1.sql("""insert into s values (1), (2), (3), (4), (5);""")
    assert data["row_count"] == 5

    expected_rows = [[1], [1], [3]]

    def check_inner_join_global_vs_segment():
        data = i1.sql(
            """
            select b from g
            join s on g.a = s.c
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda e: e[0]) == expected_rows

    Retriable(rps=5, timeout=6).call(check_inner_join_global_vs_segment)

    def check_inner_join_segment_vs_global():
        data = i1.sql(
            """
            select b from s
            join g on g.a = s.c
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda e: e[0]) == expected_rows

    Retriable(rps=5, timeout=6).call(check_inner_join_segment_vs_global)

    def check_inner_join_segment_vs_global_sq_in_cond():
        data = i1.sql(
            """
            select c from s
            join g on 1 = 1 and
            c in (select a*a from g)
            group by c
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda e: e[0]) == [[1], [4]]

    Retriable(rps=5, timeout=6).call(check_inner_join_segment_vs_global_sq_in_cond)

    def check_left_join_segment_vs_global_sq_in_cond():
        data = i1.sql(
            """
            select c, cast(sum(a) as int) from s
            left join g on 1 = 1 and
            c in (select a*a from g)
            where c < 4
            group by c
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda e: e[0]) == [
            [1, 6],
            [2, None],
            [3, None],
        ]

    Retriable(rps=5, timeout=6).call(check_left_join_segment_vs_global_sq_in_cond)

    def check_left_join_any_vs_global():
        data = i1.sql(
            """
            select c, b from
            (select c*c as c from s)
            left join g on c = b
            where c < 5
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda e: e[0]) == [[1, 1], [1, 1], [4, None]]

    Retriable(rps=5, timeout=6).call(check_left_join_any_vs_global)

    def check_inner_join_any_vs_global():
        data = i1.sql(
            """
            select c, b from
            (select c*c as c from s)
            inner join g on c = b
            where c < 5
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda e: e[0]) == [[1, 1], [1, 1]]

    Retriable(rps=5, timeout=6).call(check_inner_join_any_vs_global)

    def check_left_join_single_vs_global():
        data = i1.sql(
            """
            select c, a from
            (select count(*) as c from s)
            left join g on c = a + 2
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda e: e[0]) == [[5, 3]]

    Retriable(rps=5, timeout=6).call(check_left_join_single_vs_global)

    def check_left_join_global_with_expr_in_proj_vs_segment():
        data = i1.sql(
            """
            select b, c from (select b + 3 as b from g)
            left join s on b = c
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda e: e[0]) == [[4, 4], [4, 4], [6, None]]

    Retriable(rps=5, timeout=6).call(
        check_left_join_global_with_expr_in_proj_vs_segment
    )

    def check_left_join_global_vs_any_false_condition():
        data = i1.sql(
            """
            select b, c from g
            left join
            (select c*c as c from s where c > 3)
            on b = c
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda e: e[0]) == [
            [1, None],
            [1, None],
            [3, None],
        ]

    Retriable(rps=5, timeout=6).call(check_left_join_global_vs_any_false_condition)

    def check_left_join_global_vs_any():
        data = i1.sql(
            """
            select b, c from g
            left join
            (select c*c as c from s where c < 3)
            on b = c
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda e: e[0]) == [[1, 1], [1, 1], [3, None]]

    Retriable(rps=5, timeout=6).call(check_left_join_global_vs_any)

    def check_left_join_complex_global_child_vs_any():
        data = i1.sql(
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
        assert sorted(data["rows"], key=lambda e: e[0]) == [[3, 3, 3]]

    Retriable(rps=5, timeout=6).call(check_left_join_complex_global_child_vs_any)


def test_union_all_on_global_tbls(cluster: Cluster):
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

    for i, j in [(1, 1), (2, 2), (3, 2)]:
        index = i1.cas("insert", "G", [i, j])
        i1.raft_wait_index(index, 3)

    ddl = i1.sql(
        """
        create table s (c int not null, d int not null, primary key (c))
        using memtx
        distributed by (c)
        option (timeout = 3)
    """
    )
    assert ddl["row_count"] == 1
    data = i1.sql("""insert into s values (1, 2), (2, 2), (3, 2);""")
    assert data["row_count"] == 3

    expected = [[1], [2], [2], [2], [2], [2]]

    def check_global_vs_any():
        data = i1.sql(
            """
            select b from g
            union all
            select d from s
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda x: x[0]) == expected

    Retriable(rps=5, timeout=6).call(check_global_vs_any)

    def check_any_vs_global():
        data = i1.sql(
            """
            select d from s
            union all
            select b from g
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda x: x[0]) == expected

    Retriable(rps=5, timeout=6).call(check_any_vs_global)

    def check_global_vs_global():
        data = i1.sql(
            """
            select b from g
            union all
            select a from g
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda x: x[0]) == [
            [1],
            [1],
            [2],
            [2],
            [2],
            [3],
        ]

    Retriable(rps=5, timeout=6).call(check_global_vs_global)

    expected = [[1], [1], [2], [2], [3], [3]]

    def check_global_vs_segment():
        data = i1.sql(
            """
            select a from g
            union all
            select c from s
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda x: x[0]) == expected

    Retriable(rps=5, timeout=6).call(check_global_vs_segment)

    def check_segment_vs_global():
        data = i1.sql(
            """
            select c from s
            union all
            select a from g
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda x: x[0]) == expected

    Retriable(rps=5, timeout=6).call(check_segment_vs_global)

    expected = [[1], [2], [3], [6]]

    def check_single_vs_global():
        data = i1.sql(
            """
            select sum(c) from s
            union all
            select a from g
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda x: x[0]) == expected

    Retriable(rps=5, timeout=6).call(check_single_vs_global)

    def check_global_vs_single():
        data = i1.sql(
            """
            select a from g
            union all
            select sum(c) from s
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda x: x[0]) == expected

    Retriable(rps=5, timeout=6).call(check_global_vs_single)

    # some arbitrary queries

    def check_multiple_union_all():
        data = i1.sql(
            """
            select * from (
                select a from g
                where a = 2
                union all
                select d from s
                group by d
            ) union all
            select a from g
            where b = 1
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda x: x[0]) == [[1], [2], [2]]

    Retriable(rps=5, timeout=6).call(check_multiple_union_all)

    def check_union_with_where():
        data = i1.sql(
            """
            select a from g
            where a in (select d from s)
            union all
            select c from s
            where c = 3
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda x: x[0]) == [[2], [3]]

    Retriable(rps=5, timeout=6).call(check_union_with_where)

    def check_complex_segment_child():
        data = i1.sql(
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
        assert sorted(data["rows"], key=lambda x: x[0]) == [[2, 2], [2, 9]]

    Retriable(rps=5, timeout=6).call(check_complex_segment_child)


def test_except_on_global_tbls(cluster: Cluster):
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
        create table s (c int not null, d int not null, primary key (c))
        using memtx
        distributed by (c)
        option (timeout = 3)
        """
    )
    assert ddl["row_count"] == 1

    data = i1.sql("""insert into s values (3, 2), (4, 3), (5, 4), (6, 5), (7, 6);""")
    assert data["row_count"] == 5

    def check_global_vs_global():
        data = i1.sql(
            """
            select a from g
            except
            select a - 1 from g
            """,
            timeout=2,
        )
        assert data["rows"] == [[5]]

    Retriable(rps=5, timeout=6).call(check_global_vs_global)

    def check_global_vs_segment():
        data = i1.sql(
            """
            select a from g
            except
            select c from s
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda x: x[0]) == [[1], [2]]

    Retriable(rps=5, timeout=6).call(check_global_vs_segment)

    def check_global_vs_any():
        data = i1.sql(
            """
            select b from g
            except
            select d from s
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda x: x[0]) == [[1]]

    Retriable(rps=5, timeout=6).call(check_global_vs_any)

    def check_global_vs_single():
        data = i1.sql(
            """
            select b from g
            where b = 1 or b = 2
            except
            select sum(d) from s
            where d = 3
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda x: x[0]) == [[1], [2]]

    Retriable(rps=5, timeout=6).call(check_global_vs_single)

    def check_single_vs_global():
        data = i1.sql(
            """
            select sum(d) from s
            where d = 3 or d = 2
            except
            select b from g
            where b = 1 or b = 2
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda x: x[0]) == [[5]]

    Retriable(rps=5, timeout=6).call(check_single_vs_global)

    def check_segment_vs_global():
        data = i1.sql(
            """
            select c from s
            except
            select a from g
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda x: x[0]) == [[6], [7]]

    Retriable(rps=5, timeout=6).call(check_segment_vs_global)

    def check_any_vs_global():
        data = i1.sql(
            """
            select d from s
            except
            select b from g
            """,
            timeout=2,
        )
        assert sorted(data["rows"], key=lambda x: x[0]) == [[6]]

    Retriable(rps=5, timeout=6).call(check_any_vs_global)

    def check_multiple_excepts():
        data = i1.sql(
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
        assert sorted(data["rows"], key=lambda x: x[0]) == [[7]]

    Retriable(rps=5, timeout=6).call(check_multiple_excepts)


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

    # already dropped -> error, no such space
    with pytest.raises(ReturnError, match="sbroad: space t not found"):
        i2.sql(
            """
            drop table "t"
            option (timeout = 3)
        """
        )

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
    password = "Passw0rd"
    upper_username = "USER"
    rolename = "Role"
    upper_rolename = "ROLE"
    acl = i1.sql(
        f"""
        create user "{username}" with password '{password}'
        using md5 option (timeout = 3)
    """
    )
    assert acl["row_count"] == 1
    assert i1.call("box.space._pico_user.index.name:get", username) is not None

    # Dropping user that doesn't exist should return 0.
    acl = i1.sql(f"drop user {upper_username}")
    assert acl["row_count"] == 0

    # Dropping user that does exist should return 1.
    acl = i1.sql(f'drop user "{username}"')
    assert acl["row_count"] == 1
    assert i1.call("box.space._pico_user.index.name:get", username) is None

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
        i1.sql(f"create user {username} with password 'Badpa5SS' using md5")
        i1.sql(f"create user {username} with password 'Badpa5SS' using md5")
    acl = i1.sql(f"drop user {username}")
    assert acl["row_count"] == 1

    another_password = "Qwerty123"
    # Alter of unexisted user should do nothing.
    acl = i1.sql(f"alter user \"nobody\" with password '{another_password}'")
    assert acl["row_count"] == 0

    # Check altering works.
    acl = i1.sql(f"create user {username} with password '{password}' using md5")
    assert acl["row_count"] == 1
    user_def = i1.call("box.space._pico_user.index.name:get", upper_username)
    users_auth_was = user_def[3]
    # * Password and method aren't changed -> update nothing.
    acl = i1.sql(f"alter user {username} with password '{password}' using md5")
    assert acl["row_count"] == 0
    user_def = i1.call("box.space._pico_user.index.name:get", upper_username)
    users_auth_became = user_def[3]
    assert users_auth_was == users_auth_became

    # * Password is changed -> update hash.
    acl = i1.sql(f"alter user {username} with password '{another_password}' using md5")
    assert acl["row_count"] == 1
    user_def = i1.call("box.space._pico_user.index.name:get", upper_username)
    users_auth_became = user_def[3]
    assert users_auth_was[0] == users_auth_became[0]
    assert users_auth_was[1] != users_auth_became[1]

    # * Password and method are changed -> update method and hash.
    acl = i1.sql(
        f"alter user {username} with password '{another_password}' using chap-sha1"
    )
    assert acl["row_count"] == 1
    user_def = i1.call("box.space._pico_user.index.name:get", upper_username)
    users_auth_became = user_def[3]
    assert users_auth_was[0] != users_auth_became[0]
    assert users_auth_was[1] != users_auth_became[1]
    # * LDAP should ignore password -> update method and hash.
    acl = i1.sql(f"alter user {username} with password '{another_password}' using ldap")
    assert acl["row_count"] == 1
    user_def = i1.call("box.space._pico_user.index.name:get", upper_username)
    users_auth_became = user_def[3]
    assert users_auth_was[0] != users_auth_became[0]
    assert users_auth_became[1] == ""
    acl = i1.sql(f"drop user {username}")
    assert acl["row_count"] == 1

    # Attempt to create role with the name of already existed user
    # should lead to an error.
    acl = i1.sql(
        f""" create user "{username}" with password 'Validpassw0rd' using md5 """
    )
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
    assert i1.call("box.space._pico_user.index.name:get", rolename) is not None

    # Dropping role that does exist should return 1.
    acl = i1.sql(f'drop role "{rolename}"')
    assert acl["row_count"] == 1
    assert i1.call("box.space._pico_user.index.name:get", rolename) is None

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


def test_sql_alter_login(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    username = "USER"
    password = "PA5sWORD"
    # Create user.
    acl = i1.sudo_sql(f"create user {username} with password '{password}'")
    assert acl["row_count"] == 1

    # Alter user with LOGIN option - opertaion is idempotent.
    acl = i1.sudo_sql(f""" alter user {username} with login """)
    assert acl["row_count"] == 1
    # Alter user with NOLOGIN option.
    acl = i1.sudo_sql(f""" alter user {username} with nologin """)
    assert acl["row_count"] == 1
    # Alter user with NOLOGIN again - operation is idempotent.
    acl = i1.sudo_sql(f""" alter user {username} with nologin """)
    assert acl["row_count"] == 1
    # Login privilege is removed
    with pytest.raises(
        Exception,
        match="User does not have login privilege",
    ):
        i1.sql("insert into t values(2);", user=username, password=password)

    # Login privilege cannot be removed from pico_service even by admin.
    with pytest.raises(
        Exception,
        match="Revoke 'login' from 'pico_service' is denied for all users",
    ):
        i1.sudo_sql('alter user "pico_service" with nologin')


def test_sql_acl_privileges(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    username = "USER"
    another_username = "ANOTHER_USER"
    password = "PaSSW0RD"
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
    alice_pwd = "Pa55sword"

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


def test_user_changes_password(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)
    user_name = "U"
    old_password = "Passw0rd"
    new_password = "Pa55word"

    i1.create_user(with_name=user_name, with_password=old_password)

    i1.sql(
        f"""
        ALTER USER "{user_name}" PASSWORD '{new_password}'
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
        distributed by (b)
        """
    )
    assert data["row_count"] == 1

    # Check that the procedure would be created with the expected id.
    next_func_id = i1.eval("return box.internal.generate_func_id(true)")
    data = i2.sql(
        """
        create procedure proc1(int)
        language SQL
        as $$insert into t values(?, ?)$$
        """
    )
    assert data["row_count"] == 1
    data = i1.sql(
        """
        select "id" from "_pico_routine" where "name" = 'PROC1'
        """,
    )
    assert data["rows"] == [[next_func_id]]

    # Check that recreation of the same procedure is idempotent.
    data = i2.sql(
        """
        create procedure proc1(int)
        language SQL
        as $$insert into t values(?, ?)$$
        """
    )
    assert data["row_count"] == 0

    # Check that we can't create a procedure with the same name but different
    # signature.
    with pytest.raises(
        ReturnError, match="routine PROC1 already exists with different parameters"
    ):
        i2.sql(
            """
            create procedure proc1(int, text)
            language SQL
            as $$insert into t values(?, ?)$$
            option(timeout=3)
            """
        )
    with pytest.raises(
        ReturnError, match="routine PROC1 already exists with a different body"
    ):
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
    with pytest.raises(ReturnError, match="ddl operation was aborted"):
        i1.sql(
            """
            create procedure proc2()
            as $$insert into t values(1, 2)$$
            """
        )
    data = i1.sql(""" select * from "_pico_routine" where "name" = 'PROC2' """)
    assert data["rows"] == []
    data = i2.sql(""" select * from "_pico_routine" where "name" = 'PROC2' """)
    assert data["rows"] == []

    # Check that PROC1 is actually dropped
    i1.sql(""" drop procedure proc1 """)
    data = i1.sql(""" select * from "_pico_routine" where "name" = 'PROC1' """)
    assert data["rows"] == []
    data = i2.sql(""" select * from "_pico_routine" where "name" = 'PROC1' """)
    assert data["rows"] == []

    # Check that dropping of the same procedure is idempotent.
    i1.sql(""" drop procedure proc1 """)
    i2.sql(""" drop procedure proc1 """)

    # Create proc for dropping.
    data = i2.sql(
        """
        create procedure proc1(int)
        language SQL
        as $$insert into t values(?, ?)$$
        """
    )
    assert data["row_count"] == 1

    data = i1.sql(
        """
        select "id" from "_pico_routine" where "name" = 'PROC1'
        """,
    )
    assert data["rows"] != []
    routine_id = data["rows"][0][0]

    # Check that dropping raises an error in case of parameters mismatch.
    with pytest.raises(
        ReturnError,
        match=r"routine exists but with a different signature: PROC1\(integer\)",
    ):
        data = i2.sql(""" drop procedure proc1(decimal) """)

    # Check that dropping raises an error in case of parameters mismatch.
    with pytest.raises(
        ReturnError,
        match=r"routine exists but with a different signature: PROC1\(integer\)",
    ):
        data = i2.sql(""" drop procedure proc1(integer, integer) """)

    # Routine mustn't be dropped at the moment.
    data = i1.sql(""" select * from "_pico_routine" where "name" = 'PROC1' """)
    assert data["rows"] != []
    data = i2.sql(""" select * from "_pico_routine" where "name" = 'PROC1' """)
    assert data["rows"] != []

    # Check drop with matching parameters.
    i2.sql(""" drop procedure proc1(integer) """)
    data = i1.sql(""" select * from "_pico_routine" where "name" = 'PROC1' """)
    assert data["rows"] == []
    data = i2.sql(""" select * from "_pico_routine" where "name" = 'PROC1' """)
    assert data["rows"] == []

    # Check that recreated routine has the same id with the recently dropped one.
    i2.sql(
        """
        create procedure proc1(int)
        language SQL
        as $$insert into t values(?, ?)$$
        """
    )
    data = i1.sql(
        """
        select "id" from "_pico_routine" where "name" = 'PROC1'
        """,
    )
    assert data["rows"] != []
    assert routine_id == data["rows"][0][0]


def test_sql_user_password_checks(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, _ = cluster.instances

    with pytest.raises(
        ReturnError,
        match="invalid password: password should contains at least one uppercase letter",
    ):
        i1.sql(
            """
            create user noname with password 'withoutdigitsanduppercase'
            using md5 option (timeout = 3)
            """
        )

    with pytest.raises(
        ReturnError,
        match="invalid password: password should contains at least one lowercase letter",
    ):
        i1.sql(
            """
            create user noname with password 'PASSWORD3'
            using md5 option (timeout = 3)
            """
        )

    with pytest.raises(
        ReturnError,
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
    read_index = i1.raft_read_index()

    ret = cluster.cas(
        "replace",
        "_pico_property",
        ["password_enforce_uppercase", False],
        index=read_index,
    )
    assert ret == read_index + 1

    ret = cluster.cas(
        "replace",
        "_pico_property",
        ["password_enforce_specialchars", True],
        index=read_index,
    )
    assert ret == read_index + 2

    cluster.raft_wait_index(ret)

    with pytest.raises(
        ReturnError,
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
        as $$insert into t values(?, ?) on conflict do nothing$$
        """
    )
    assert data["row_count"] == 1
    data = i2.sql(""" call "proc1"(1, 1) """)
    assert data["row_count"] == 1
    data = i2.sql(""" call "proc1"(1, 1) """)
    assert data["row_count"] == 0
    data = i2.sql(""" call "proc1"(2, 2) """)
    assert data["row_count"] == 1

    data = i1.sql(
        """
        create procedure "proc2"(int)
        language SQL
        as $$insert into t values(?, 42) on conflict do fail$$
        """
    )
    assert data["row_count"] == 1

    # Check that procedure returns an error when called with wrong number of arguments.
    with pytest.raises(ReturnError, match=r"""expected 1 parameter\(s\), got 0"""):
        i2.sql(""" call "proc2"() """)
    with pytest.raises(ReturnError, match=r"""expected 1 parameter\(s\), got 2"""):
        i2.sql(""" call "proc2"(3, 3) """)
    # Check that procedure returns an error when called with the wrong argument type.
    error_msg = "expected integer for parameter on position 0, got string"
    with pytest.raises(ReturnError, match=error_msg):
        i2.sql(""" call "proc2"('hello') """)

    # Check internal statement errors are propagated.
    with pytest.raises(ReturnError, match="Duplicate key exists in unique index"):
        i2.sql(""" call "proc2"(1) """)

    # Check parameters are passed correctly.
    data = i1.sql(""" call "proc2"(?) """, 4)
    assert data["row_count"] == 1
    data = i1.sql(
        """ call "proc2"($1) option(sql_vdbe_max_steps = $1, vtable_max_rows = $1)""",
        5,
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
    with pytest.raises(
        ReturnError, match="AccessDenied: Execute access to function 'proc1' is denied"
    ):
        i1.sql(""" call "proc1"(3, 3) """, user=username, password=alice_pwd)


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
        as $$insert into t values(?, ?)$$
        """
    )
    assert data["row_count"] == 1

    data = i1.sql(
        """
        alter procedure foo
        rename to bar
        option ( timeout = 4 )
        """
    )
    assert data["row_count"] == 1

    data = i2.sql(
        """
        select "name" from "_pico_routine"
        where "name" = 'BAR' or "name" = 'FOO'
        """
    )
    assert data["rows"] == [["BAR"]]

    # procedure foo doesn't exist
    data = i1.sql(
        """
        alter procedure foo
        rename to bar
        """
    )
    assert data["row_count"] == 0

    # rename back, use syntax with parameters
    data = i1.sql(
        """
        alter procedure "BAR"(int)
        rename to "FOO"
        """
    )
    assert data["row_count"] == 1

    data = i2.sql(
        """
        select "name" from "_pico_routine"
        where "name" = 'BAR' or "name" = 'FOO'
        """
    )
    assert data["rows"] == [["FOO"]]

    data = i2.sql(
        """
        create procedure bar()
        language SQL
        as $$insert into t values(200, 200)$$
        """
    )
    assert data["row_count"] == 1

    with pytest.raises(ReturnError, match="Name 'FOO' is already taken"):
        data = i1.sql(
            """
            alter procedure bar()
            rename to foo
            """
        )

    with pytest.raises(
        ReturnError, match="routine exists but with a different signature: BAR()"
    ):
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
        ReturnError, match="Alter access to function 'BAR' is denied for user 'alice'"
    ):
        i1.sql(
            """alter procedure bar rename to buzz""", user=username, password=alice_pwd
        )

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
    with pytest.raises(ReturnError, match="ddl operation was aborted"):
        i1.sql(
            """
            alter procEdurE foo rENaME tO "foobar"
            """
        )

    data = i1.sql(""" select * from "_pico_routine" where "name" = 'foobar' """)
    assert data["rows"] == []
    data = i2.sql(""" select * from "_pico_routine" where "name" = 'foobar' """)
    assert data["rows"] == []


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
        acl = i1.sudo_sql(
            f"""
            grant write on table "{table_name}" to "{user}"
            """
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
            ddl = i1.sudo_sql(query)
        else:
            ddl = i1.sql(query, user=as_user, password=as_pwd)
        assert ddl["row_count"] == 1

    def drop_procedure(name: str, as_user=None, as_pwd=None):
        query = f"drop procedure {name}"
        ddl = None
        if not as_user:
            ddl = i1.sudo_sql(query)
        else:
            ddl = i1.sql(query, user=as_user, password=as_pwd)
        assert ddl["row_count"] == 1

    def rename_procedure(old_name: str, new_name: str, as_user=None, as_pwd=None):
        query = f"alter procedure {old_name} rename to {new_name}"
        ddl = None
        if not as_user:
            ddl = i1.sudo_sql(query)
        else:
            ddl = i1.sql(query, user=as_user, password=as_pwd)
        assert ddl["row_count"] == 1

    def grant_procedure(priv: str, user: str, fun=None, as_user=None, as_pwd=None):
        query = f"grant {priv}"
        if fun:
            query += f' on procedure "{fun}"'
        else:
            query += " procedure"
        query += f' to "{user}"'
        acl = (
            i1.sql(query, user=as_user, password=as_pwd)
            if as_user
            else i1.sudo_sql(query)
        )

        assert acl["row_count"] == 1

    def revoke_procedure(priv: str, user: str, fun=None, as_user=None, as_pwd=None):
        query = f"revoke {priv}"
        if fun:
            query += f' on procedure "{fun}"'
        else:
            query += " procedure"
        query += f' from "{user}"'
        acl = (
            i1.sql(query, user=as_user, password=as_pwd)
            if as_user
            else i1.sudo_sql(query)
        )

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
            ReturnError,
            match=f"AccessDenied: Execute access to function '{fun}' "
            + f"is denied for user '{username}'",
        ):
            call_procedure(fun, *args, as_user=username, as_pwd=pwd)

    def check_create_access_denied(fun, username, pwd):
        with pytest.raises(
            ReturnError,
            match=f"AccessDenied: Create access to function '{fun}' "
            + f"is denied for user '{username}'",
        ):
            create_procedure(fun, 0, as_user=username, as_pwd=pwd)

    def check_drop_access_denied(fun, username, pwd):
        with pytest.raises(
            ReturnError,
            match=f"AccessDenied: Drop access to function '{fun}' "
            + f"is denied for user '{username}'",
        ):
            drop_procedure(fun, as_user=username, as_pwd=pwd)

    def check_rename_access_denied(old_name, new_name, username, pwd):
        with pytest.raises(
            ReturnError,
            match=f"AccessDenied: Alter access to function '{old_name}' "
            + f"is denied for user '{username}'",
        ):
            rename_procedure(old_name, new_name, as_user=username, as_pwd=pwd)

    # ----------------- Default privliges -----------------

    # Check that a user can't create a procedure without permition.
    check_create_access_denied("FOOBAZSPAM", alice, alice_pwd)

    # Check that a non-owner user without drop privilege can't drop proc
    create_procedure("FOO", 0)
    check_drop_access_denied("FOO", bob, bob_pwd)

    # Check that a non-owner user can't rename proc
    check_rename_access_denied("FOO", "BAR", bob, bob_pwd)

    # Check that a user without permession can't call proc
    check_execute_access_denied("FOO", bob, bob_pwd)

    # Check that owner can call proc
    call_procedure("FOO")

    # Check that owner can rename proc
    rename_procedure("FOO", "BAR")

    # Check that owner can drop proc
    drop_procedure("BAR")

    # ----------------- Default privliges -----------------

    # ----------------- grant-revoke privilege -----------------

    # ALL PROCEDURES

    # Check admin can grant create procedure to user
    grant_procedure("create", alice)
    create_procedure("FOO", 0, alice, alice_pwd)
    drop_procedure("FOO", alice, alice_pwd)
    create_procedure("FOO", 0)
    check_drop_access_denied("FOO", alice, alice_pwd)
    check_rename_access_denied("FOO", "BAR", alice, alice_pwd)
    drop_procedure("FOO")

    # Check admin can revoke create procedure from user
    revoke_procedure("create", alice)
    check_create_access_denied("FOO", alice, alice_pwd)

    # check grant execute to all procedures
    grant_procedure("create", bob)
    create_procedure("FOO", 0, bob, bob_pwd)
    grant_procedure("execute", alice)
    call_procedure("FOO", as_user=alice, as_pwd=alice_pwd)

    # check revoke execute from all procedures
    revoke_procedure("execute", alice)
    check_execute_access_denied("FOO", alice, alice_pwd)
    revoke_procedure("create", bob)

    # check grant drop for all procedures
    drop_procedure("FOO")
    create_procedure("FOO", 0)
    check_drop_access_denied("FOO", bob, bob_pwd)
    grant_procedure("drop", bob)
    drop_procedure("FOO", bob, bob_pwd)

    # check revoke drop for all procedures
    create_procedure("FOO", 0)
    revoke_procedure("drop", bob)
    check_drop_access_denied("FOO", bob, bob_pwd)

    drop_procedure("FOO")

    # Check that user can't grant create procedure (only admin can)
    with pytest.raises(
        ReturnError,
        match=f"AccessDenied: Grant to routine '' is denied for user '{alice}'",
    ):
        grant_procedure("create", bob, as_user=alice, as_pwd=alice_pwd)

    # Check that user can't grant execute procedure (only admin can)
    with pytest.raises(
        ReturnError,
        match=f"AccessDenied: Grant to routine '' is denied for user '{alice}'",
    ):
        grant_procedure("execute", bob, as_user=alice, as_pwd=alice_pwd)

    # SPECIFIC PROCEDURE

    # check grant execute specific procedure
    grant_procedure("create", alice)
    create_procedure("FOO", 0, alice, alice_pwd)
    check_execute_access_denied("FOO", bob, bob_pwd)
    grant_procedure("execute", bob, "FOO", as_user=alice, as_pwd=alice_pwd)
    call_procedure("FOO", as_user=bob, as_pwd=bob_pwd)

    # check admin can revoke execute from user
    revoke_procedure("execute", bob, "FOO", alice, alice_pwd)
    check_execute_access_denied("FOO", bob, bob_pwd)

    # check owner of procedure can grant drop to other user
    check_drop_access_denied("FOO", bob, bob_pwd)
    grant_procedure("drop", bob, "FOO", as_user=alice, as_pwd=alice_pwd)
    check_rename_access_denied("FOO", "BAR", bob, bob_pwd)
    drop_procedure("FOO", bob, bob_pwd)

    # check owner of procedure can revoke drop to other user
    create_procedure("FOO", 0, alice, alice_pwd)
    check_drop_access_denied("FOO", bob, bob_pwd)
    grant_procedure("drop", bob, "FOO", as_user=alice, as_pwd=alice_pwd)
    revoke_procedure("drop", bob, "FOO", alice, alice_pwd)
    check_drop_access_denied("FOO", bob, bob_pwd)

    # check we can't grant create specific procedure
    with pytest.raises(
        ReturnError,
        match="sbroad: invalid privilege",
    ):
        grant_procedure("create", bob, "FOO")

    # check we can't grant to non-existing user
    with pytest.raises(
        ReturnError,
        match="Nor user, neither role with name pasha exists",
    ):
        grant_procedure("drop", "pasha", "FOO")

    # check we can't revoke from non-existing user
    with pytest.raises(
        ReturnError,
        match="Nor user, neither role with name pasha exists",
    ):
        revoke_procedure("execute", "pasha", "FOO")
