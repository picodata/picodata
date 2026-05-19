from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("serialize_as_empty.sql")
class TestSerializeAsEmpty(ClusterTwoInstances):
    pass
