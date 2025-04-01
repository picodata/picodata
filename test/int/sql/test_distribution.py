from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("except.sql")
class TestExcept(ClusterSingleInstance):
    pass
