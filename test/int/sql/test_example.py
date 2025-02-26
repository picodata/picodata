from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("example.sql")
class TestExample1(ClusterSingleInstance):
    pass


@sql_test_file("example.sql")
class TestExample2(ClusterSingleInstance):
    pass
