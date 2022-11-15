from conftest import Cluster
import funcy  # type: ignore


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


def test_apply_migrations(cluster: Cluster):
    # Scenario: apply migration to cluster
    #   Given a cluster with added migration
    #   When it push up desired_schema_version
    #   Then the migration is applied
    #   And replicaset's current_schema_version is set to desired_schema_version

    cluster.deploy(instance_count=3)
    i1, _, _ = cluster.instances
    i1.promote_or_fail()
    i1.assert_raft_status("Leader")
    i1.call(
        "picolib.add_migration",
        1,
        """
        box.schema.space.create('test_space', {
            format = {
                {name = 'id', type = 'unsigned'},
                {name = 'value', type = 'string'},
            }
        })
        """,
    )
    i1.call(
        "picolib.add_migration",
        2,
        "box.space.test_space:create_index('pk', {parts = {'id'}})",
    )

    i1.call("picolib.push_schema_version", 2)

    @funcy.retry(tries=30, timeout=0.2)  # type: ignore
    def assert_space_insert(conn):
        assert conn.insert("test_space", [1, "foo"])

    @funcy.retry(tries=30, timeout=0.2)  # type: ignore
    def assert_replicaset_version(conn):
        position = conn.schema.get_field("replicasets", "current_schema_version")["id"]
        assert [2, 2, 2] == [tuple[position] for tuple in conn.select("replicasets")]

    for i in cluster.instances:
        with i.connect(timeout=1) as conn:
            assert_space_insert(conn)
            assert_replicaset_version(conn)
