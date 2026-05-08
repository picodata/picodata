from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("explain_context.sql")
class TestExplainContext(ClusterSingleInstance):
    pass
