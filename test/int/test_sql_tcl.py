from conftest import (
    Cluster,
)


def test_tcl(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1 = cluster.instances[0]

    tcl = i1.sql(
        """
        BEGIN
        """
    )
    assert tcl["row_count"] == 0

    tcl = i1.sql("""COMMIT""")
    assert tcl["row_count"] == 0

    tcl = i1.sql("""ROLLBACK""")
    assert tcl["row_count"] == 0
