from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("unnamed_subquery.sql")
class TestUnnamedSubquery(ClusterSingleInstance):
    pass
