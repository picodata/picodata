from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("groupby.sql")
class TestGroupBy(ClusterTwoInstances):
    pass
