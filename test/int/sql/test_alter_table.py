from framework.sqltester import (
    ClusterTwoInstances,
    Cluster,
    sql_test_file,
)


@sql_test_file("alter_table_logic.sql")
class TestGroupBy(ClusterTwoInstances):
    pass


def test_cache_is_not_stale_on_alter(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    i1.sql("create table g (id int primary key) distributed globally")
    i1.sql("select * from g")  # add query to the prepared statement cache
    i1.sql("alter table g add column value text")
    i1.sql("insert into g values (1, 'foo'), (2, 'bar')")
    results = i1.sql("select * from g")
    assert results == [[1, "foo"], [2, "bar"]]  # make sure the resulting query is not using the stale cache entry
