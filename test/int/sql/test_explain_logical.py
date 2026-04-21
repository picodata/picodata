from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("explain_logical.sql")
class TestExplainLogical(ClusterSingleInstance):
    pass
