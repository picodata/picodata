from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("storage_cache.sql")
class TestStorageCache(ClusterSingleInstance):
    pass
