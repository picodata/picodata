from conftest import Cluster


def test_ddl_create_space_bulky(cluster: Cluster):
    # TODO: add 2 more instances, to check that another replicaset is handled
    # correctly
    i1, i2 = cluster.deploy(instance_count=2, init_replication_factor=2)

    # At cluster boot schema version is 0
    assert i1.call("box.space._picodata_property:get", "current_schema_version")[1] == 0
    assert i2.call("box.space._picodata_property:get", "current_schema_version")[1] == 0

    # Propose a space creation which will fail
    op = dict(
        kind="ddl_prepare",
        schema_version=1,
        ddl=dict(
            kind="create_space",
            id=666,
            name="stuff",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=[dict(field="(this will cause an error)")],
            distribution=dict(kind="global"),
        ),
    )
    # TODO: rewrite the test using pico.cas, when it supports ddl
    i1.call("pico.raft_propose", op)

    # No space was created
    assert i1.call("box.space._picodata_space:get", 666) is None
    assert i1.call("box.space._space:get", 666) is None
    assert i2.call("box.space._picodata_space:get", 666) is None
    assert i2.call("box.space._space:get", 666) is None

    # Schema version hasn't changed
    # XXX: This seems weird
    assert i1.call("box.space._picodata_property:get", "current_schema_version")[1] == 0
    assert i2.call("box.space._picodata_property:get", "current_schema_version")[1] == 0

    # Propose a space creation which will succeed
    op = dict(
        kind="ddl_prepare",
        # This version number must not be equal to the version of the aborted
        # change, otherwise all of the instances joined after this request was
        # committed will be blocked during the attempt to apply the previous
        # DdlAbort
        schema_version=2,
        ddl=dict(
            kind="create_space",
            id=666,
            name="stuff",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=[dict(field=1, type="unsigned")],
            distribution=dict(kind="global"),
        ),
    )
    i1.call("pico.raft_propose", op)

    # Schema version was updated
    assert i1.call("box.space._picodata_property:get", "current_schema_version")[1] == 2
    assert i2.call("box.space._picodata_property:get", "current_schema_version")[1] == 2

    # Space was created and is operable
    space_info = [666, "stuff", ["global"], [["id", "unsigned", False]], 2, True]
    assert i1.call("box.space._picodata_space:get", 666) == space_info
    assert i2.call("box.space._picodata_space:get", 666) == space_info

    space_meta = [
        666,
        1,
        "stuff",
        "memtx",
        0,
        dict(),
        [dict(name="id", type="unsigned", is_nullable=False)],
    ]
    assert i1.call("box.space._space:get", 666) == space_meta
    assert i2.call("box.space._space:get", 666) == space_meta

    # Primary index was also created
    # TODO: maybe we want to replace these `None`s with the default values when
    # inserting the index definition into _picodata_index?
    index_info = [
        666,
        0,
        "primary_key",
        True,
        [[1, "unsigned", None, None, None]],
        2,
        True,
    ]
    assert i1.call("box.space._picodata_index:get", [666, 0]) == index_info
    assert i2.call("box.space._picodata_index:get", [666, 0]) == index_info

    index_meta = [
        666,
        0,
        "primary_key",
        "tree",
        dict(),
        [[1, "unsigned", None, None, None]],
    ]
    assert i1.call("box.space._index:get", [666, 0]) == index_meta
    assert i2.call("box.space._index:get", [666, 0]) == index_meta

    # Add a new replicaset master
    i3 = cluster.add_instance(wait_online=True, replicaset_id="r2")

    # It's schema was updated automatically
    assert i3.call("box.space._picodata_property:get", "current_schema_version")[1] == 2
    assert i3.call("box.space._picodata_space:get", 666) == space_info
    assert i3.call("box.space._picodata_index:get", [666, 0]) == index_info
    # TODO: this fails
    assert i3.call("box.space._space:get", 666) == space_meta
    assert i3.call("box.space._index:get", [666, 0]) == index_meta

    # Add a follower to the new replicaset
    i4 = cluster.add_instance(wait_online=True, replicaset_id="r2")

    # It's schema was updated automatically as well
    assert i4.call("box.space._picodata_property:get", "current_schema_version")[1] == 2
    assert i4.call("box.space._picodata_space:get", 666) == space_info
    assert i4.call("box.space._picodata_index:get", [666, 0]) == index_info
    assert i4.call("box.space._space:get", 666) == space_meta
    assert i4.call("box.space._index:get", [666, 0]) == index_meta


def test_ddl_create_space_partial_failure(cluster: Cluster):
    i1, i2, i3 = cluster.deploy(instance_count=3)

    # Create a space on one instance
    # which will conflict with the clusterwide space.
    i3.eval("box.schema.space.create(...)", "space_name_conflict")

    # Propose a space creation which will fail
    op = dict(
        kind="ddl_prepare",
        schema_version=1,
        ddl=dict(
            kind="create_space",
            id=666,
            name="space_name_conflict",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=[dict(field=1, type="unsigned")],
            distribution=dict(kind="global"),
        ),
    )
    # TODO: rewrite the test using pico.cas, when it supports ddl
    prepare_index = i1.call("pico.raft_propose", op)
    abort_index = prepare_index + 1

    i1.call(".proc_sync_raft", abort_index, (3, 0))
    i2.call(".proc_sync_raft", abort_index, (3, 0))
    i3.call(".proc_sync_raft", abort_index, (3, 0))

    # No space was created
    assert i1.call("box.space._picodata_space:get", 666) is None
    assert i1.call("box.space._space:get", 666) is None
    assert i2.call("box.space._picodata_space:get", 666) is None
    assert i2.call("box.space._space:get", 666) is None
    assert i3.call("box.space._picodata_space:get", 666) is None
    assert i3.call("box.space._space:get", 666) is None

    # TODO: add instance which will conflict with this ddl and make sure it
    # panics
