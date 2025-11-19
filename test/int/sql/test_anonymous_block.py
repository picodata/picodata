from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("anonymous_block.sql")
class TestAnonBlocks(ClusterSingleInstance):
    pass
