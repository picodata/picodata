from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("left_outer_join.sql")
class TestLeftOuterJoin(ClusterSingleInstance):
    pass
