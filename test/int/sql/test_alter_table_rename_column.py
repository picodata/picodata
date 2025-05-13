from framework.sqltester import (
    Cluster,
    ClusterTwoInstances,
    sql_test_file,
)


@sql_test_file("alter_table_rename_column.sql")
class TestRenameColumn(ClusterTwoInstances):
    pass


# this test is implemented as an actual python test because sqltest struggles with parsing the nested list structure of `_pico_index.parts`
def test_index_metadata_is_updated(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    i1.sql("DROP TABLE IF EXISTS test_table")
    i1.sql("DROP INDEX IF EXISTS test_index")
    i1.sql("CREATE TABLE test_table(id INT PRIMARY KEY, status TEXT)")
    i1.sql("CREATE INDEX test_index ON test_table (status)")
    i1.sql("ALTER TABLE test_table RENAME COLUMN status TO new_name")
    results = i1.sql("SELECT parts FROM _pico_index WHERE name = 'test_index'")
    assert results == [[[["new_name", "string", None, True, None]]]]
