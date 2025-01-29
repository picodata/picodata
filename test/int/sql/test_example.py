from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("example.sql")
class TestExample1(ClusterTwoInstances):
    pass


@sql_test_file("example.sql")
class TestExample2(ClusterTwoInstances):
    pass
