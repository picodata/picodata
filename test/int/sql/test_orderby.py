from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("orderby.sql")
class TestOrderBy(ClusterTwoInstances):
    pass
