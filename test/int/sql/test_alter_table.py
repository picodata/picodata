from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("alter_table_logic.sql")
class TestGroupBy(ClusterTwoInstances):
    pass
