from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("substring.sql")
class TestSubstring(ClusterTwoInstances):
    pass
