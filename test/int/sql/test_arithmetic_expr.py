from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("arithmetic_expr.sql")
class TestArithmeticExpr(ClusterSingleInstance):
    pass
