from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("gen_invalid_sql.sql")
class TestInvalidSQLGenError(ClusterSingleInstance):
    pass
