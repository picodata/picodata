from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("sqlfunc.sql")
class TestSqlFunctions(ClusterTwoInstances):
    pass
