from conftest import Cluster


def test_add_migration(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances
    i1.promote_or_fail()
    i1.eval("picolib.add_migration(1, 'migration body')")
    migrations_table = i2.call("box.space.migrations:select")
    assert [[1, "migration body"]] == migrations_table


def test_push_schema_version(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances
    i1.promote_or_fail()
    i1.eval("picolib.push_schema_version(3)")
    key = "desired_schema_version"
    assert [[key, 3]] == i2.call("box.space.cluster_state:select", [key])
