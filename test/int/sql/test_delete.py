from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("delete.sql")
class TestDelete(ClusterTwoInstances):
    pass
