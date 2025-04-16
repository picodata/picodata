from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("explain.sql")
class TestExplain(ClusterTwoInstances):
    pass
