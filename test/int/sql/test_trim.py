from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("trim.sql")
class TestTrim(ClusterTwoInstances):
    pass
