from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


# A GROUP BY alias is parsed into an untyped placeholder reference, so the
# frontend types it as NULL and misses type errors: indexing an int alias
# should fail with "cannot index expression of type int", but instead the
# query reaches the storage and dies at SQL compilation. To be fixed by the
# parser rewrite.
@sql_test_file("group_by_alias_typing.sql")
class TestGroupByAliasTyping(ClusterSingleInstance):
    pass
