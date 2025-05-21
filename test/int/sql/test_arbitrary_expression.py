from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("arbitrary_exprs.sql")
class TestArbitraryExprs(ClusterSingleInstance):
    pass
