from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("explain_buckets.sql")
class TestExplainBuckets(ClusterSingleInstance):
    pass
