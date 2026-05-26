from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("asterisk.sql")
class TestAsterisk(ClusterSingleInstance):
    pass
