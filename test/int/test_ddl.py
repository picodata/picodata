from conftest import Cluster


def test_ddl_create_space_bulky(cluster: Cluster):
    i1, i2, i3, i4 = cluster.deploy(instance_count=4, init_replication_factor=2)

    # At cluster boot schema version is 0
    assert i1.call("box.space._pico_property:get", "current_schema_version")[1] == 0
    assert i2.call("box.space._pico_property:get", "current_schema_version")[1] == 0
    assert i3.call("box.space._pico_property:get", "current_schema_version")[1] == 0
    assert i4.call("box.space._pico_property:get", "current_schema_version")[1] == 0

    # And next schema version will be 1
    assert i1.next_schema_version() == 1
    assert i2.next_schema_version() == 1
    assert i3.next_schema_version() == 1
    assert i4.next_schema_version() == 1

    ############################################################################
    # Propose a space creation which will fail

    abort_index = i1.ddl_create_space(
        dict(
            id=666,
            name="stuff",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=[dict(field="(this will cause an error)")],
            distribution=dict(kind="global"),
        ),
    )

    i1.call(".proc_sync_raft", abort_index, (3, 0))
    i2.call(".proc_sync_raft", abort_index, (3, 0))
    i3.call(".proc_sync_raft", abort_index, (3, 0))
    i4.call(".proc_sync_raft", abort_index, (3, 0))

    # No space was created
    assert i1.call("box.space._pico_space:get", 666) is None
    assert i2.call("box.space._pico_space:get", 666) is None
    assert i3.call("box.space._pico_space:get", 666) is None
    assert i4.call("box.space._pico_space:get", 666) is None
    assert i1.call("box.space._space:get", 666) is None
    assert i2.call("box.space._space:get", 666) is None
    assert i3.call("box.space._space:get", 666) is None
    assert i4.call("box.space._space:get", 666) is None

    # Schema version hasn't changed
    assert i1.call("box.space._pico_property:get", "current_schema_version")[1] == 0
    assert i2.call("box.space._pico_property:get", "current_schema_version")[1] == 0
    assert i3.call("box.space._pico_property:get", "current_schema_version")[1] == 0
    assert i4.call("box.space._pico_property:get", "current_schema_version")[1] == 0

    # But next schema version did change
    assert i1.next_schema_version() == 2
    assert i2.next_schema_version() == 2
    assert i3.next_schema_version() == 2
    assert i4.next_schema_version() == 2

    ############################################################################
    # Propose a space creation which will succeed

    commit_index = i1.ddl_create_space(
        dict(
            id=666,
            name="stuff",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=[dict(field="id")],
            distribution=dict(kind="global"),
        ),
    )

    i1.call(".proc_sync_raft", commit_index, (3, 0))
    i2.call(".proc_sync_raft", commit_index, (3, 0))
    i3.call(".proc_sync_raft", commit_index, (3, 0))
    i4.call(".proc_sync_raft", commit_index, (3, 0))

    # This time schema version did change
    assert i1.call("box.space._pico_property:get", "current_schema_version")[1] == 2
    assert i2.call("box.space._pico_property:get", "current_schema_version")[1] == 2
    assert i3.call("box.space._pico_property:get", "current_schema_version")[1] == 2
    assert i4.call("box.space._pico_property:get", "current_schema_version")[1] == 2

    # And so did next schema version obviously
    assert i1.next_schema_version() == 3
    assert i2.next_schema_version() == 3
    assert i3.next_schema_version() == 3
    assert i4.next_schema_version() == 3

    # Space was created and is operable
    pico_space_def = [666, "stuff", ["global"], [["id", "unsigned", False]], 2, True]
    assert i1.call("box.space._pico_space:get", 666) == pico_space_def
    assert i2.call("box.space._pico_space:get", 666) == pico_space_def
    assert i3.call("box.space._pico_space:get", 666) == pico_space_def
    assert i4.call("box.space._pico_space:get", 666) == pico_space_def

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
    assert i3.call("box.space._space:get", 666) == tt_space_def
    assert i4.call("box.space._space:get", 666) == tt_space_def

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
    assert i3.call("box.space._pico_index:get", [666, 0]) == pico_pk_def
    assert i4.call("box.space._pico_index:get", [666, 0]) == pico_pk_def

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
    assert i3.call("box.space._index:get", [666, 0]) == tt_pk_def
    assert i4.call("box.space._index:get", [666, 0]) == tt_pk_def

    ############################################################################
    # A new replicaset catches up after the fact successfully

    i5 = cluster.add_instance(wait_online=True, replicaset_id="r3")

    assert i5.call("box.space._pico_property:get", "current_schema_version")[1] == 2
    assert i5.next_schema_version() == 3
    assert i5.call("box.space._pico_space:get", 666) == pico_space_def
    assert i5.call("box.space._pico_index:get", [666, 0]) == pico_pk_def
    assert i5.call("box.space._space:get", 666) == tt_space_def
    assert i5.call("box.space._index:get", [666, 0]) == tt_pk_def

    i6 = cluster.add_instance(wait_online=True, replicaset_id="r3")

    # It's schema was updated automatically as well
    assert i6.call("box.space._pico_property:get", "current_schema_version")[1] == 2
    assert i6.next_schema_version() == 3
    assert i6.call("box.space._pico_space:get", 666) == pico_space_def
    assert i6.call("box.space._pico_index:get", [666, 0]) == pico_pk_def
    assert i6.call("box.space._space:get", 666) == tt_space_def
    assert i6.call("box.space._index:get", [666, 0]) == tt_pk_def


def test_ddl_create_sharded_space(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2, init_replication_factor=2)

    # Propose a space creation which will succeed
    schema_version = i1.next_schema_version()
    index = i1.ddl_create_space(
        dict(
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

    i1.call(".proc_sync_raft", index, (3, 0))
    i2.call(".proc_sync_raft", index, (3, 0))

    ############################################################################
    # Space was created and is operable
    pico_space_def = [
        666,
        "stuff",
        ["sharded_implicitly", ["foo", "bar"], "murmur3"],
        [
            ["id", "unsigned", False],
            # Automatically generated by picodata
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

    ############################################################################
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

    ############################################################################
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
    space_def = dict(
        id=666,
        name="space_name_conflict",
        format=[dict(name="id", type="unsigned", is_nullable=False)],
        primary_key=[dict(field="id")],
        distribution=dict(kind="global"),
    )
    index = i1.ddl_create_space(space_def)

    i2.call(".proc_sync_raft", index, (3, 0))
    i3.call(".proc_sync_raft", index, (3, 0))

    # No space was created
    assert i1.call("box.space._pico_space:get", 666) is None
    assert i2.call("box.space._pico_space:get", 666) is None
    assert i3.call("box.space._pico_space:get", 666) is None
    assert i1.call("box.space._space:get", 666) is None
    assert i2.call("box.space._space:get", 666) is None
    assert i3.call("box.space._space:get", 666) is None

    # TODO: terminate i3, commit create space and wake i3 back up


def test_ddl_from_snapshot(cluster: Cluster):
    # Second instance is only for quorum
    i1, i2 = cluster.deploy(instance_count=2, init_replication_factor=2)

    i1.assert_raft_status("Leader")

    # TODO: check other ddl operations
    # Propose a space creation which will succeed
    index = i1.ddl_create_space(
        dict(
            id=666,
            name="stuff",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=[dict(field="id")],
            distribution=dict(kind="sharded_implicitly", sharding_key=["id"]),
        ),
    )

    i2.call(".proc_sync_raft", index, (3, 0))

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
        ],
    ]
    assert i1.call("box.space._space:get", 666) == tt_space_def
    assert i2.call("box.space._space:get", 666) == tt_space_def

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

    # Compact the log to trigger snapshot for the newcommer
    i1.raft_compact_log()
    i2.raft_compact_log()

    # A replicaset master catches up from snapshot
    i3 = cluster.add_instance(wait_online=True, replicaset_id="R2")
    assert i3.call("box.space._space:get", 666) == tt_space_def
    assert i3.call("box.space._index:get", [666, 0]) == tt_pk_def
    assert i3.call("box.space._index:get", [666, 1]) == tt_bucket_id_def
    assert i3.call("box.space._schema:get", "pico_schema_version")[1] == 1

    # A replicaset follower catches up from snapshot
    i4 = cluster.add_instance(wait_online=True, replicaset_id="R2")
    assert i4.call("box.space._space:get", 666) == tt_space_def
    assert i4.call("box.space._index:get", [666, 0]) == tt_pk_def
    assert i4.call("box.space._index:get", [666, 1]) == tt_bucket_id_def
    assert i4.call("box.space._schema:get", "pico_schema_version")[1] == 1
