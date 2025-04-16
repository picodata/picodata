from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("target_queries.sql")
class TestSubstring(ClusterSingleInstance):
    pass


@sql_test_file("random_queries.sql")
class TestGroupby(ClusterSingleInstance):
    pass
