"""End-to-end coverage for §5 dynamic filter pushdown.

A two-node cluster runs an INNER JOIN where the right side is much
smaller than the left. With the alter-system flag ON the planner must
emit a dynamic filter (visible in EXPLAIN); with the flag OFF the plan
must not contain one. The user-visible result set is identical in both
cases. LEFT/RIGHT JOIN must never produce a dynamic filter.
"""

from conftest import Cluster


def _setup_join_tables(i):
    i.sql("CREATE TABLE big (k INT PRIMARY KEY, v TEXT) DISTRIBUTED BY (k)")
    i.sql("CREATE TABLE small (k INT PRIMARY KEY, w TEXT) DISTRIBUTED BY (k)")
    for k in range(100):
        i.sql(f"INSERT INTO big VALUES ({k}, 'b{k}')")
    for k in (5, 10, 15):
        i.sql(f"INSERT INTO small VALUES ({k}, 's{k}')")


def _explain(i, query):
    return "\n".join(i.sql(f"EXPLAIN {query}")).lower()


def test_dynfilter_pushdown_inner_join(cluster: Cluster):
    cluster.deploy(instance_count=2, init_replication_factor=1)
    cluster.wait_until_buckets_balanced()
    i1 = cluster.instances[0]

    i1.sql("ALTER SYSTEM SET sql_dynamic_filter_pushdown = true")
    _setup_join_tables(i1)

    query = "SELECT big.v, small.w FROM big JOIN small ON big.k = small.k"

    plan_on = _explain(i1, query)
    assert "dynamic filter" in plan_on, plan_on

    rows_on = i1.sql(f"{query} ORDER BY big.v")
    assert rows_on == [["b10", "s10"], ["b15", "s15"], ["b5", "s5"]]

    i1.sql("ALTER SYSTEM SET sql_dynamic_filter_pushdown = false")
    plan_off = _explain(i1, query)
    assert "dynamic filter" not in plan_off, plan_off

    rows_off = i1.sql(f"{query} ORDER BY big.v")
    assert rows_off == rows_on


def test_dynfilter_skipped_for_outer_joins(cluster: Cluster):
    cluster.deploy(instance_count=2, init_replication_factor=1)
    cluster.wait_until_buckets_balanced()
    i1 = cluster.instances[0]

    i1.sql("ALTER SYSTEM SET sql_dynamic_filter_pushdown = true")
    _setup_join_tables(i1)

    for kind in ("LEFT", "RIGHT"):
        plan = _explain(
            i1,
            f"SELECT big.v, small.w FROM big {kind} JOIN small ON big.k = small.k",
        )
        assert "dynamic filter" not in plan, (kind, plan)
