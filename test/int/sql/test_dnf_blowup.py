from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("dnf_blowup.sql")
class TestDnfBlowup(ClusterSingleInstance):
    pass
