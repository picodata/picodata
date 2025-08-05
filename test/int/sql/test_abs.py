from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("abs.sql")
class TestAbs(ClusterSingleInstance):
    pass
