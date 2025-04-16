from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("limit.sql")
class TestLimit(ClusterSingleInstance):
    pass
