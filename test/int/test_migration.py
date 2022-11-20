from conftest import Cluster


def test_add_migration(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances
    i1.promote_or_fail()
    i1.eval("pico.add_migration(1, 'migration body')")
    migrations_table = i2.call("box.space.migrations:select")
    assert [[1, "migration body"]] == migrations_table


def test_push_schema_version(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances
    i1.promote_or_fail()
    i1.eval("pico.push_schema_version(3)")
    key = "desired_schema_version"
    assert [[key, 3]] == i2.call("box.space.cluster_state:select", [key])


def test_apply_migrations(cluster: Cluster):
    # Scenario: apply migration to cluster
    #   Given a cluster with added migration
    #   When it push up desired_schema_version
    #   Then the migration is applied
    #   And replicaset's current_schema_version is set to desired_schema_version

    cluster.deploy(instance_count=3)
    i1 = cluster.instances[0]
    i1.promote_or_fail()
    i1.assert_raft_status("Leader")

    for (n, sql) in {
        1: """create table "test_space" ("id" int primary key)""",
        2: """alter table "test_space" add column "value" varchar(100)""",
    }.items():
        i1.call("pico.add_migration", n, sql)

    i1.call("pico.migrate")

    def assert_space_insert(conn):
        assert conn.insert("test_space", [1, "foo"])

    def assert_replicaset_version(conn, v):
        position = conn.schema.get_field("replicasets", "current_schema_version")["id"]
        assert [v, v, v] == [tuple[position] for tuple in conn.select("replicasets")]

    for i in cluster.instances:
        with i.connect(timeout=1) as conn:
            assert_space_insert(conn)
            assert_replicaset_version(conn, 2)
