from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("operators.sql")
class TestOperators(ClusterSingleInstance):
    pass
