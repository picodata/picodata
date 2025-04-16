from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("insert.sql")
class TestInsert(ClusterSingleInstance):
    pass
