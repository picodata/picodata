from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("dql_bucket_id_in_pk.sql")
class TestDqlBucketIdInPk(ClusterSingleInstance):
    pass
