from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("unnamed_join.sql")
class TestUnnamedJoin(ClusterSingleInstance):
    pass
