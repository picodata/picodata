from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("cte2.sql")
class TestCte2(ClusterTwoInstances):
    pass
