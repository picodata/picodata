from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("array_unsupported_operations.sql")
class TestArrayUnsupportedOperations(ClusterSingleInstance):
    pass
