from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("eliminate_motion.sql")
class TestEliminateMotion(ClusterTwoInstances):
    pass
