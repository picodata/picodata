from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("sq_as_expr.sql")
class TestSqAsExpr(ClusterSingleInstance):
    pass
