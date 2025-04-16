from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("cte.sql")
class TestCte(ClusterSingleInstance):
    pass
