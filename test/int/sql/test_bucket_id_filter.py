from framework.sqltester import (
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("bucket_id_filter.sql")
class TestBucketIdFilter(ClusterTwoInstances):
    pass
