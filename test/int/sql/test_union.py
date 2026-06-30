from framework.sqltester import (
    ClusterSingleInstance,
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("union-single-instance.sql")
class TestUnionSingle(ClusterSingleInstance):
    pass


@sql_test_file("union-two-instances.sql")
class TestUnionMultiple(ClusterTwoInstances):
    pass
