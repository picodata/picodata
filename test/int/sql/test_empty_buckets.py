from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("empty_buckets.sql")
class TestEmptyBuckets(ClusterTwoInstances):
    """
    Queries with an empty set of calculated buckets should be executed using
    `Buckets::Any`, e.g. `SELECT 1 UNION ALL SELECT a FROM t WHERE false` must
    return a single row.
    """

    pass
