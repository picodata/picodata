from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("expression.sql")
class TestGroupBy(ClusterTwoInstances):
    pass
