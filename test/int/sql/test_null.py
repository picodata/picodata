from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


# Must run on a multi-instance cluster: verifies NULL values are transferred correctly between instances.
@sql_test_file("null.sql")
class TestNull(ClusterTwoInstances):
    pass
