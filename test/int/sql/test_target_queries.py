from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("target_queries.sql")
class TestTargetQueries1(ClusterSingleInstance):
    pass


@sql_test_file("random_queries.sql")
class TestTargetQueries2(ClusterSingleInstance):
    pass
