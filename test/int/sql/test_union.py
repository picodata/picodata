from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("union.sql")
class TestUnion(ClusterSingleInstance):
    pass
