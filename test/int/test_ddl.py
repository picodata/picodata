from conftest import Cluster


def test_ddl_create_space_basic(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2, init_replication_factor=2)

    # At cluster boot schema version is 0
    assert i1.call("box.space._picodata_property:get", "current_schema_version")[1] == 0
    assert i2.call("box.space._picodata_property:get", "current_schema_version")[1] == 0

    # Propose a space creation and abort it
    op = dict(
        kind="ddl_prepare",
        schema_version=1,
        ddl=dict(
            kind="create_space",
            id=666,
            name="stuff",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=[dict(field="id")],
            distribution=dict(kind="global"),
        ),
    )
    # TODO: rewrite the test using pico.cas, when it supports ddl
    i1.call("pico.raft_propose", op)
    # TODO: check the intermediate state
    i1.call("pico.raft_propose", dict(kind = "ddl_abort"))

    # No space was created
    assert i1.call("box.space._picodata_space:get", 666) is None
    assert i1.call("box.space._space:get", 666) is None
    assert i2.call("box.space._picodata_space:get", 666) is None
    assert i2.call("box.space._space:get", 666) is None

    # Schema version hasn't changed
    # XXX: This seems weird
    assert i1.call("box.space._picodata_property:get", "current_schema_version")[1] == 0
    assert i2.call("box.space._picodata_property:get", "current_schema_version")[1] == 0

    # Propose a space creation and commit it
    op = dict(
        kind="ddl_prepare",
        schema_version=2,
        ddl=dict(
            kind="create_space",
            id=666,
            name="stuff",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=[dict(field="id")],
            distribution=dict(kind="global"),
        ),
    )
    i1.call("pico.raft_propose", op)
    i1.call("pico.raft_propose", dict(kind = "ddl_commit"))

    # Schema version was updated
    assert i1.call("box.space._picodata_property:get", "current_schema_version")[1] == 2
    assert i2.call("box.space._picodata_property:get", "current_schema_version")[1] == 2

    # Space was created and is operable
    space_info = [666, "stuff", ["global"], [["id", "unsigned", False]], 2, True]
    assert i1.call("box.space._picodata_space:get", 666) == space_info
    assert i2.call("box.space._picodata_space:get", 666) == space_info
    # TODO: check box.space._space was also update when it's supported

    # Primary index was also created
    # TODO: maybe we want to replace these `None`s with the default values when
    # inserting the index definition into _picodata_index?
    index_info = [666, 0, "primary_key", True, [["id", None, None, None, None]], 2, True]
    assert i1.call("box.space._picodata_index:get", [666, 0]) == index_info
    assert i2.call("box.space._picodata_index:get", [666, 0]) == index_info
    # TODO: check box.space._index was also update when it's supported

    # Add a new replicaset master
    i3 = cluster.add_instance(wait_online=True, replicaset_id="r2")

    # It's schema was updated automatically
    assert i3.call("box.space._picodata_property:get", "current_schema_version")[1] == 2
    assert i3.call("box.space._picodata_space:get", 666) == space_info
    assert i3.call("box.space._picodata_index:get", [666, 0]) == index_info

    # Add a follower to the new replicaset
    i4 = cluster.add_instance(wait_online=True, replicaset_id="r2")

    # It's schema was updated automatically as well
    assert i4.call("box.space._picodata_property:get", "current_schema_version")[1] == 2
    assert i4.call("box.space._picodata_space:get", 666) == space_info
    assert i4.call("box.space._picodata_index:get", [666, 0]) == index_info
