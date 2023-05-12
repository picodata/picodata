from conftest import Cluster


def test_ddl_create_space_bulky(cluster: Cluster):
    # TODO: add 2 more instances, to check that another replicaset is handled
    # correctly
    i1, i2 = cluster.deploy(instance_count=2, init_replication_factor=2)

    # At cluster boot schema version is 0
    assert i1.call("box.space._pico_property:get", "current_schema_version")[1] == 0
    assert i2.call("box.space._pico_property:get", "current_schema_version")[1] == 0

    # Propose a space creation which will fail
    op = dict(
        kind="ddl_prepare",
        schema_version=i1.next_schema_version(),
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
    assert i1.call("box.space._pico_space:get", 666) is None
    assert i1.call("box.space._space:get", 666) is None
    assert i2.call("box.space._pico_space:get", 666) is None
    assert i2.call("box.space._space:get", 666) is None

    # Schema version hasn't changed
    assert i1.call("box.space._pico_property:get", "current_schema_version")[1] == 0
    assert i2.call("box.space._pico_property:get", "current_schema_version")[1] == 0

    # Propose a space creation which will succeed
    op = dict(
        kind="ddl_prepare",
        schema_version=i1.next_schema_version(),
        ddl=dict(
            kind="create_space",
            id=666,
            name="stuff",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=[dict(field=0, type="unsigned")],
            distribution=dict(kind="global"),
        ),
    )
    # TODO: rewrite the test using pico.cas, when it supports ddl
    index = i1.call("pico.raft_propose", op)

    i1.call(".proc_sync_raft", index, (3, 0))
    i2.call(".proc_sync_raft", index, (3, 0))

    # Schema version was updated
    assert i1.call("box.space._pico_property:get", "current_schema_version")[1] == 2
    assert i2.call("box.space._pico_property:get", "current_schema_version")[1] == 2

    # Space was created and is operable
    pico_space_def = [666, "stuff", ["global"], [["id", "unsigned", False]], 2, True]
    assert i1.call("box.space._pico_space:get", 666) == pico_space_def
    assert i2.call("box.space._pico_space:get", 666) == pico_space_def

    tt_space_def = [
        666,
        1,
        "stuff",
        "memtx",
        0,
        dict(),
        [dict(name="id", type="unsigned", is_nullable=False)],
    ]
    assert i1.call("box.space._space:get", 666) == tt_space_def
    assert i2.call("box.space._space:get", 666) == tt_space_def

    # Primary index was also created
    # TODO: maybe we want to replace these `None`s with the default values when
    # inserting the index definition into _pico_index?
    pico_pk_def = [
        666,
        0,
        "primary_key",
        True,
        [[0, "unsigned", None, False, None]],
        2,
        True,
        True,
    ]
    assert i1.call("box.space._pico_index:get", [666, 0]) == pico_pk_def
    assert i2.call("box.space._pico_index:get", [666, 0]) == pico_pk_def

    tt_pk_def = [
        666,
        0,
        "primary_key",
        "tree",
        dict(unique=True),
        [[0, "unsigned", None, False, None]],
    ]
    assert i1.call("box.space._index:get", [666, 0]) == tt_pk_def
    assert i2.call("box.space._index:get", [666, 0]) == tt_pk_def

    # Add a new replicaset master
    i3 = cluster.add_instance(wait_online=True, replicaset_id="r2")

    # It's schema was updated automatically
    assert i3.call("box.space._pico_property:get", "current_schema_version")[1] == 2
    assert i3.call("box.space._pico_space:get", 666) == pico_space_def
    assert i3.call("box.space._pico_index:get", [666, 0]) == pico_pk_def
    # TODO: this fails
    assert i3.call("box.space._space:get", 666) == tt_space_def
    assert i3.call("box.space._index:get", [666, 0]) == tt_pk_def

    # Add a follower to the new replicaset
    i4 = cluster.add_instance(wait_online=True, replicaset_id="r2")

    # It's schema was updated automatically as well
    assert i4.call("box.space._pico_property:get", "current_schema_version")[1] == 2
    assert i4.call("box.space._pico_space:get", 666) == pico_space_def
    assert i4.call("box.space._pico_index:get", [666, 0]) == pico_pk_def
    assert i4.call("box.space._space:get", 666) == tt_space_def
    assert i4.call("box.space._index:get", [666, 0]) == tt_pk_def


def test_ddl_create_sharded_space(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2, init_replication_factor=2)

    # Propose a space creation which will succeed
    schema_version = i1.next_schema_version()
    op = dict(
        kind="ddl_prepare",
        schema_version=schema_version,
        ddl=dict(
            kind="create_space",
            id=666,
            name="stuff",
            format=[
                dict(name="id", type="unsigned", is_nullable=False),
                dict(name="foo", type="integer", is_nullable=False),
                dict(name="bar", type="string", is_nullable=False),
            ],
            primary_key=[dict(field="id")],
            distribution=dict(kind="sharded_implicitly", sharding_key=["foo", "bar"]),
        ),
    )
    # TODO: rewrite the test using pico.cas, when it supports ddl
    index = i1.call("pico.raft_propose", op)

    i1.call(".proc_sync_raft", index, (3, 0))
    i2.call(".proc_sync_raft", index, (3, 0))

    # Space was created and is operable
    pico_space_def = [
        666,
        "stuff",
        ["sharded_implicitly", ["foo", "bar"], "murmur3"],
        [
            ["id", "unsigned", False],
            ["bucket_id", "unsigned", False],
            ["foo", "integer", False],
            ["bar", "string", False],
        ],
        schema_version,
        True,
    ]
    assert i1.call("box.space._pico_space:get", 666) == pico_space_def
    assert i2.call("box.space._pico_space:get", 666) == pico_space_def

    tt_space_def = [
        666,
        1,
        "stuff",
        "memtx",
        0,
        dict(),
        [
            dict(name="id", type="unsigned", is_nullable=False),
            dict(name="bucket_id", type="unsigned", is_nullable=False),
            dict(name="foo", type="integer", is_nullable=False),
            dict(name="bar", type="string", is_nullable=False),
        ],
    ]
    assert i1.call("box.space._space:get", 666) == tt_space_def
    assert i2.call("box.space._space:get", 666) == tt_space_def

    # Primary index was also created
    pico_pk_def = [
        666,
        0,
        "primary_key",
        True,
        [[0, "unsigned", None, False, None]],
        schema_version,
        True,
        True,
    ]
    assert i1.call("box.space._pico_index:get", [666, 0]) == pico_pk_def
    assert i2.call("box.space._pico_index:get", [666, 0]) == pico_pk_def

    tt_pk_def = [
        666,
        0,
        "primary_key",
        "tree",
        dict(unique=True),
        [[0, "unsigned", None, False, None]],
    ]
    assert i1.call("box.space._index:get", [666, 0]) == tt_pk_def
    assert i2.call("box.space._index:get", [666, 0]) == tt_pk_def

    # This time bucket id was also created
    pico_bucket_id_def = [
        666,
        1,
        "bucket_id",
        True,
        [[1, "unsigned", None, False, None]],
        schema_version,
        True,
        False,
    ]
    assert i1.call("box.space._pico_index:get", [666, 1]) == pico_bucket_id_def
    assert i2.call("box.space._pico_index:get", [666, 1]) == pico_bucket_id_def

    tt_bucket_id_def = [
        666,
        1,
        "bucket_id",
        "tree",
        dict(unique=False),
        [[1, "unsigned", None, False, None]],
    ]
    assert i1.call("box.space._index:get", [666, 1]) == tt_bucket_id_def
    assert i2.call("box.space._index:get", [666, 1]) == tt_bucket_id_def


def test_ddl_create_space_partial_failure(cluster: Cluster):
    i1, i2, i3 = cluster.deploy(instance_count=3)

    # Create a space on one instance
    # which will conflict with the clusterwide space.
    i3.eval("box.schema.space.create(...)", "space_name_conflict")

    # Propose a space creation which will fail
    op = dict(
        kind="ddl_prepare",
        schema_version=i1.next_schema_version(),
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
    assert i1.call("box.space._pico_space:get", 666) is None
    assert i1.call("box.space._space:get", 666) is None
    assert i2.call("box.space._pico_space:get", 666) is None
    assert i2.call("box.space._space:get", 666) is None
    assert i3.call("box.space._pico_space:get", 666) is None
    assert i3.call("box.space._space:get", 666) is None

    # TODO: add instance which will conflict with this ddl and make sure it
    # panics


def test_ddl_from_snapshot(cluster: Cluster):
    # Second instance is only for quorum
    i1, i2 = cluster.deploy(instance_count=2)

    i1.assert_raft_status("Leader")

    # TODO: check other ddl operations
    # Propose a space creation which will succeed
    op = dict(
        kind="ddl_prepare",
        schema_version=i1.next_schema_version(),
        ddl=dict(
            kind="create_space",
            id=666,
            name="stuff",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=[dict(field=1, type="unsigned")],
            distribution=dict(kind="global"),
        ),
    )
    # TODO: rewrite the test using pico.cas, when it supports ddl
    ret = i1.call("pico.raft_propose", op)

    # Make sure everyone is synchronized
    i1.call(".proc_sync_raft", ret, (3, 0))
    i2.call(".proc_sync_raft", ret, (3, 0))

    tt_space_def = [
        666,
        1,
        "stuff",
        "memtx",
        0,
        dict(),
        [dict(name="id", type="unsigned", is_nullable=False)],
    ]
    assert i1.call("box.space._space:get", 666) == tt_space_def
    assert i2.call("box.space._space:get", 666) == tt_space_def

    tt_pk_def = [
        666,
        0,
        "primary_key",
        "tree",
        dict(unique=True),
        [[1, "unsigned", None, None, None]],
    ]
    assert i1.call("box.space._index:get", [666, 0]) == tt_pk_def
    assert i2.call("box.space._index:get", [666, 0]) == tt_pk_def

    # Compact the log to trigger snapshot for the newcommer
    i1.raft_compact_log()
    i2.raft_compact_log()

    i3 = cluster.add_instance(wait_online=True)

    # Check space was created from the snapshot data
    assert i3.call("box.space._space:get", 666) == tt_space_def
    assert i3.call("box.space._index:get", [666, 0]) == tt_pk_def
