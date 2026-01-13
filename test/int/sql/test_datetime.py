from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("test_datetime.sql")
class TestDatetime(ClusterTwoInstances):
    pass
