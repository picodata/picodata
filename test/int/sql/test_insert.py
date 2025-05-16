from framework.sqltester import (
    ClusterSingleInstance,
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("insert.sql")
class TestInsert(ClusterSingleInstance):
    pass


@sql_test_file("insert_global_table.sql")
class TestInsertGlobalTbl(ClusterTwoInstances):
    pass
