from conftest import (
    Cluster,
)


def test_empty_queries(cluster: Cluster):
    cluster.deploy(instance_count=2)
    instance = cluster.instances[0]

    empty = instance.sql("")
    assert empty["row_count"] == 0

    empty = instance.sql(";")
    assert empty["row_count"] == 0

    empty = instance.sql("  ")
    assert empty["row_count"] == 0

    empty = instance.sql("   ;")
    assert empty["row_count"] == 0

    empty = instance.sql(";   ")
    assert empty["row_count"] == 0

    empty = instance.sql(";;;;;;")
    assert empty["row_count"] == 0

    empty = instance.sql("; ; ;")
    assert empty["row_count"] == 0
