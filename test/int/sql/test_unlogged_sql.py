from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("unlogged.sql")
class TestUnloggedDDL(ClusterSingleInstance):
    pass
