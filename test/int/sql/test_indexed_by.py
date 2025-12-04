from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("indexed_by.sql")
class TestGroupBy(ClusterSingleInstance):
    pass
