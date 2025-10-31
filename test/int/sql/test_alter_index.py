from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("alter_index_rename.sql")
class TestRenameIndex(ClusterTwoInstances):
    pass
