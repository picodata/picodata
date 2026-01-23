from framework.sqltester import (
    ClusterSingleInstance,
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("target_queries.sql")
class TestTargetQueries1(ClusterSingleInstance):
    pass


@sql_test_file("random_queries.sql")
class TestTargetQueries2(ClusterSingleInstance):
    pass


@sql_test_file("test_gl2527.sql")
class TestUpdate(ClusterTwoInstances):
    pass


@sql_test_file("test_known_defects.sql")
class TestKnownDefects(ClusterSingleInstance):
    pass
