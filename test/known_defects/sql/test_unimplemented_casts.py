from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("unimplemented_casts.sql")
class TestUnimplementedCasts(ClusterSingleInstance):
    pass
