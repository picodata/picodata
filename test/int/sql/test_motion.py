from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("motion.sql")
class TestMotion(ClusterSingleInstance):
    pass
