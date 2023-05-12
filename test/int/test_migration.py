from conftest import Cluster


def test_add_migration(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances
    i1.promote_or_fail()
    i1.eval("pico.add_migration(1, 'migration body')")
    assert i2.call("box.space._picodata_migration:select") == [[1, "migration body"]]


def test_push_schema_version(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances
    i1.promote_or_fail()
    i1.eval("pico.push_schema_version(3)")
    key = "desired_schema_version"
    assert i2.call("box.space._picodata_property:select", [key]) == [[key, 3]]


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

    def replicaset_schema_versions(instance):
        return instance.eval(
            """
            return box.space._picodata_replicaset:pairs()
                :map(function(replicaset)
                    return replicaset.current_schema_version
                end)
                :totable()
            """
        )

    for n, sql in {
        1: """create table "test_space" ("id" int primary key)""",
        2: """alter table "test_space" add column "value" varchar(100)""",
    }.items():
        i1.call("pico.add_migration", n, sql)

    assert i1.call("pico.migrate", 1) == 1

    for i in cluster.instances:
        format = i.call("box.space.test_space:format")
        assert [f["name"] for f in format] == ["id"]

        assert set(replicaset_schema_versions(i)) == {1}

    # idempotent
    assert i1.call("pico.migrate", 1) == 1

    assert i1.call("pico.migrate", 2) == 2

    for i in cluster.instances:
        format = i.call("box.space.test_space:format")
        assert [f["name"] for f in format] == ["id", "value"]
        assert set(replicaset_schema_versions(i)) == {2}

    # idempotent
    assert i1.call("pico.migrate", 1) == 2
    assert i1.call("pico.migrate", 2) == 2
