from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("api.sql")
class TestApi(ClusterSingleInstance):
    pass
