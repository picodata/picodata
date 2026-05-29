from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("array.sql")
class TestArray(ClusterSingleInstance):
    pass
