"""End-to-end coverage for §5 dynamic filter pushdown.

v1 precondition (see sql-planner/src/ir/transformation/dynamic_filter.rs)
requires both immediate children of an INNER JOIN to be `Motion(Full)`.
sbroad's redistribution pass never produces that shape from parser-emitted
SQL — typical inner joins yield `(no_motion, Motion(Full))`, and
`(Single, Single)` joins emit no motions at all. So the EXPLAIN footer
cannot be observed from a cluster test; the resolver's behavior is
verified by unit tests in the planner crate.

What this file checks at cluster level:

  * toggling `sql_dynamic_filter_pushdown` ON/OFF never changes the
    result set (correctness invariant, must hold regardless of whether
    the resolver actually fires);
  * LEFT JOIN execution is unaffected by the flag — guard against a
    future widening of the precondition that accidentally tags outer
    joins.
"""

from conftest import Cluster


def _setup_join_tables(i):
    i.sql("CREATE TABLE big (k INT PRIMARY KEY, v TEXT) DISTRIBUTED BY (k)")
    i.sql("CREATE TABLE small (k INT PRIMARY KEY, w TEXT) DISTRIBUTED BY (k)")
    for k in range(100):
        i.sql(f"INSERT INTO big VALUES ({k}, 'b{k}')")
    for k in (5, 10, 15):
        i.sql(f"INSERT INTO small VALUES ({k}, 's{k}')")


def test_dynfilter_inner_join_result_unchanged(cluster: Cluster):
    """Toggling the flag does not change the inner-join result set."""
    cluster.deploy(instance_count=2, init_replication_factor=1)
    cluster.wait_until_buckets_balanced()
    i1 = cluster.instances[0]
    _setup_join_tables(i1)

    query = "SELECT big.v, small.w FROM big JOIN small ON big.k = small.k ORDER BY big.v"
    expected = [["b10", "s10"], ["b15", "s15"], ["b5", "s5"]]

    i1.sql("ALTER SYSTEM SET sql_dynamic_filter_pushdown = true")
    assert i1.sql(query) == expected

    i1.sql("ALTER SYSTEM SET sql_dynamic_filter_pushdown = false")
    assert i1.sql(query) == expected


def test_dynfilter_left_join_result_unchanged(cluster: Cluster):
    """LEFT JOIN must produce the same rows regardless of the flag."""
    cluster.deploy(instance_count=2, init_replication_factor=1)
    cluster.wait_until_buckets_balanced()
    i1 = cluster.instances[0]
    _setup_join_tables(i1)

    query = "SELECT small.w, big.v FROM small LEFT JOIN big ON small.k = big.k ORDER BY small.k"
    expected = [["s5", "b5"], ["s10", "b10"], ["s15", "b15"]]

    i1.sql("ALTER SYSTEM SET sql_dynamic_filter_pushdown = true")
    assert i1.sql(query) == expected

    i1.sql("ALTER SYSTEM SET sql_dynamic_filter_pushdown = false")
    assert i1.sql(query) == expected
