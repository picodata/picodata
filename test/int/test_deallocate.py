from conftest import (
    Cluster,
)


def test_deallocate(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1 = cluster.instances[0]

    res = i1.sql("DEALLOCATE name")
    assert res["row_count"] == 0

    res = i1.sql("DEALLOCATE ALL")
    assert res["row_count"] == 0

    res = i1.sql("DEALLOCATE prepare")
    assert res["row_count"] == 0

    res = i1.sql("DEALLOCATE PREPARE prepare")
    assert res["row_count"] == 0

    res = i1.sql("DEALLOCATE PREPARE name")
    assert res["row_count"] == 0

    res = i1.sql("DEALLOCATE PREPARE ALL")
    assert res["row_count"] == 0
