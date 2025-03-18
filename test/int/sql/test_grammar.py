from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("alter_table.sql")
class TestAlterTable(ClusterSingleInstance):
    pass


@sql_test_file("public_schema.sql")
class TestPublicSchema(ClusterSingleInstance):
    pass
