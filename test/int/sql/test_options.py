from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("options.sql")
class TestOptions(ClusterSingleInstance):
    pass
