from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("groupby.sql")
class TestOrderBy(ClusterSingleInstance):
    pass
