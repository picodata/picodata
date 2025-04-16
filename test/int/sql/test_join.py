from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("join.sql")
class TestJoin(ClusterSingleInstance):
    pass
