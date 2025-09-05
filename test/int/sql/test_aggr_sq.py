from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("aggnested.sql")
class TestSqliteAggnested(ClusterSingleInstance):
    pass


@sql_test_file("distinct.sql")
class TestSqliteDistinct(ClusterSingleInstance):
    pass


@sql_test_file("distinct2.sql")
class TestSqliteDistinct2(ClusterSingleInstance):
    pass


@sql_test_file("distinctagg.sql")
class TestSqliteDistinctAgg(ClusterSingleInstance):
    pass


@sql_test_file("agg_without_scan.sql")
class TestSqliteAggWithoutScan(ClusterSingleInstance):
    pass
