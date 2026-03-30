from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("unstable_bucket_id.sql")
class TestUnstableBucketId(ClusterSingleInstance):
    pass
