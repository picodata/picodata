import pytest
from conftest import Cluster, ReturnError


def test_ddl_create_space_lua(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    # Successful space creation
    cluster.create_space(
        dict(
            id=1,
            name="some_name",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        )
    )
    pico_space_def = [1, "some_name", ["global"], [["id", "unsigned", False]], 1, True]
    assert i1.call("box.space._pico_space:get", 1) == pico_space_def
    assert i2.call("box.space._pico_space:get", 1) == pico_space_def

    # Space creation error
    with pytest.raises(ReturnError) as e1:
        cluster.create_space(
            dict(
                id=2,
                name="different_name",
                format=[dict(name="id", type="unsigned", is_nullable=False)],
                primary_key=["not_defined"],
                distribution="global",
            )
        )
    assert e1.value.args == (
        "ddl failed: space creation failed: no field with name: not_defined",
    )


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

    space_id = 713
    abort_index = i1.propose_create_space(
        dict(
            id=space_id,
            name="stuff",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=[dict(field="(this will cause an error)")],
            distribution=dict(kind="global"),
        ),
    )

    # TODO: use `raft_wait_index`
    i1.call(".proc_sync_raft", abort_index, (3, 0))
    i2.call(".proc_sync_raft", abort_index, (3, 0))
    i3.call(".proc_sync_raft", abort_index, (3, 0))
    i4.call(".proc_sync_raft", abort_index, (3, 0))

    # No space was created
    assert i1.call("box.space._pico_space:get", space_id) is None
    assert i2.call("box.space._pico_space:get", space_id) is None
    assert i3.call("box.space._pico_space:get", space_id) is None
    assert i4.call("box.space._pico_space:get", space_id) is None
    assert i1.call("box.space._space:get", space_id) is None
    assert i2.call("box.space._space:get", space_id) is None
    assert i3.call("box.space._space:get", space_id) is None
    assert i4.call("box.space._space:get", space_id) is None

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

    cluster.create_space(
        dict(
            id=space_id,
            name="stuff",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        ),
    )

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
    pico_space_def = [
        space_id,
        "stuff",
        ["global"],
        [["id", "unsigned", False]],
        2,
        True,
    ]
    assert i1.call("box.space._pico_space:get", space_id) == pico_space_def
    assert i2.call("box.space._pico_space:get", space_id) == pico_space_def
    assert i3.call("box.space._pico_space:get", space_id) == pico_space_def
    assert i4.call("box.space._pico_space:get", space_id) == pico_space_def

    tt_space_def = [
        space_id,
        1,
        "stuff",
        "memtx",
        0,
        dict(group_id=1),
        [dict(name="id", type="unsigned", is_nullable=False)],
    ]
    assert i1.call("box.space._space:get", space_id) == tt_space_def
    assert i2.call("box.space._space:get", space_id) == tt_space_def
    assert i3.call("box.space._space:get", space_id) == tt_space_def
    assert i4.call("box.space._space:get", space_id) == tt_space_def

    # Primary index was also created
    # TODO: maybe we want to replace these `None`s with the default values when
    # inserting the index definition into _pico_index?
    pico_pk_def = [
        space_id,
        0,
        "primary_key",
        True,
        [[0, "unsigned", None, False, None]],
        2,
        True,
        True,
    ]
    assert i1.call("box.space._pico_index:get", [space_id, 0]) == pico_pk_def
    assert i2.call("box.space._pico_index:get", [space_id, 0]) == pico_pk_def
    assert i3.call("box.space._pico_index:get", [space_id, 0]) == pico_pk_def
    assert i4.call("box.space._pico_index:get", [space_id, 0]) == pico_pk_def

    tt_pk_def = [
        space_id,
        0,
        "primary_key",
        "tree",
        dict(unique=True),
        [[0, "unsigned", None, False, None]],
    ]
    assert i1.call("box.space._index:get", [space_id, 0]) == tt_pk_def
    assert i2.call("box.space._index:get", [space_id, 0]) == tt_pk_def
    assert i3.call("box.space._index:get", [space_id, 0]) == tt_pk_def
    assert i4.call("box.space._index:get", [space_id, 0]) == tt_pk_def

    ############################################################################
    # A new replicaset catches up after the fact successfully

    i5 = cluster.add_instance(wait_online=True, replicaset_id="r3")

    assert i5.call("box.space._pico_property:get", "current_schema_version")[1] == 2
    assert i5.next_schema_version() == 3
    assert i5.call("box.space._pico_space:get", space_id) == pico_space_def
    assert i5.call("box.space._pico_index:get", [space_id, 0]) == pico_pk_def
    assert i5.call("box.space._space:get", space_id) == tt_space_def
    assert i5.call("box.space._index:get", [space_id, 0]) == tt_pk_def

    i6 = cluster.add_instance(wait_online=True, replicaset_id="r3")

    # It's schema was updated automatically as well
    assert i6.call("box.space._pico_property:get", "current_schema_version")[1] == 2
    assert i6.next_schema_version() == 3
    assert i6.call("box.space._pico_space:get", space_id) == pico_space_def
    assert i6.call("box.space._pico_index:get", [space_id, 0]) == pico_pk_def
    assert i6.call("box.space._space:get", space_id) == tt_space_def
    assert i6.call("box.space._index:get", [space_id, 0]) == tt_pk_def


def test_ddl_create_sharded_space(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2, init_replication_factor=2)

    # Propose a space creation which will succeed
    schema_version = i1.next_schema_version()
    space_id = 679
    cluster.create_space(
        dict(
            id=space_id,
            name="stuff",
            format=[
                dict(name="id", type="unsigned", is_nullable=False),
                dict(name="foo", type="integer", is_nullable=False),
                dict(name="bar", type="string", is_nullable=False),
            ],
            primary_key=["id"],
            distribution=dict(sharding_key=["foo", "bar"], sharding_fn="murmur3"),
        ),
    )

    ############################################################################
    # Space was created and is operable
    pico_space_def = [
        space_id,
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
    assert i1.call("box.space._pico_space:get", space_id) == pico_space_def
    assert i2.call("box.space._pico_space:get", space_id) == pico_space_def

    tt_space_def = [
        space_id,
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
    assert i1.call("box.space._space:get", space_id) == tt_space_def
    assert i2.call("box.space._space:get", space_id) == tt_space_def

    ############################################################################
    # Primary index was also created
    pico_pk_def = [
        space_id,
        0,
        "primary_key",
        True,
        [[0, "unsigned", None, False, None]],
        schema_version,
        True,
        True,
    ]
    assert i1.call("box.space._pico_index:get", [space_id, 0]) == pico_pk_def
    assert i2.call("box.space._pico_index:get", [space_id, 0]) == pico_pk_def

    tt_pk_def = [
        space_id,
        0,
        "primary_key",
        "tree",
        dict(unique=True),
        [[0, "unsigned", None, False, None]],
    ]
    assert i1.call("box.space._index:get", [space_id, 0]) == tt_pk_def
    assert i2.call("box.space._index:get", [space_id, 0]) == tt_pk_def

    ############################################################################
    # This time bucket id was also created
    pico_bucket_id_def = [
        space_id,
        1,
        "bucket_id",
        True,
        [[1, "unsigned", None, False, None]],
        schema_version,
        True,
        False,
    ]
    assert i1.call("box.space._pico_index:get", [space_id, 1]) == pico_bucket_id_def
    assert i2.call("box.space._pico_index:get", [space_id, 1]) == pico_bucket_id_def

    tt_bucket_id_def = [
        space_id,
        1,
        "bucket_id",
        "tree",
        dict(unique=False),
        [[1, "unsigned", None, False, None]],
    ]
    assert i1.call("box.space._index:get", [space_id, 1]) == tt_bucket_id_def
    assert i2.call("box.space._index:get", [space_id, 1]) == tt_bucket_id_def


def test_ddl_create_space_partial_failure(cluster: Cluster):
    i1, i2, i3 = cluster.deploy(instance_count=3)

    # Create a space on one instance
    # which will conflict with the clusterwide space.
    i3.eval("box.schema.space.create(...)", "space_name_conflict")

    # Propose a space creation which will fail
    space_id = 876
    space_def = dict(
        id=space_id,
        name="space_name_conflict",
        format=[dict(name="id", type="unsigned", is_nullable=False)],
        primary_key=[dict(field="id")],
        distribution=dict(kind="global"),
    )
    index = i1.propose_create_space(space_def)

    i2.call(".proc_sync_raft", index, (3, 0))
    i3.call(".proc_sync_raft", index, (3, 0))

    # No space was created
    assert i1.call("box.space._pico_space:get", space_id) is None
    assert i2.call("box.space._pico_space:get", space_id) is None
    assert i3.call("box.space._pico_space:get", space_id) is None
    assert i1.call("box.space._space:get", space_id) is None
    assert i2.call("box.space._space:get", space_id) is None
    assert i3.call("box.space._space:get", space_id) is None

    # Put i3 to sleep
    i3.terminate()

    # Propose the same space creation which this time succeeds, because there's
    # no conflict on any online instances.
    index = i1.propose_create_space(space_def)
    i2.call(".proc_sync_raft", index, (3, 0))

    assert i1.call("box.space._space:get", space_id) is not None
    assert i2.call("box.space._space:get", space_id) is not None

    # Wake i3 up and currently it just panics...
    i3.fail_to_start()


@pytest.mark.xfail(reason="lsn isn't replicated properly")
def test_successful_wakeup_after_ddl(cluster: Cluster):
    # Manual replicaset distribution.
    # 5 instances are needed for quorum (2 go offline).
    i1 = cluster.add_instance(replicaset_id="r1", wait_online=True)
    i2 = cluster.add_instance(replicaset_id="r1", wait_online=True)
    i3 = cluster.add_instance(replicaset_id="r2", wait_online=True)
    i4 = cluster.add_instance(replicaset_id="r2", wait_online=True)
    i5 = cluster.add_instance(replicaset_id="r3", wait_online=True)

    # This is a replicaset follower which will be catching up
    i4.terminate()
    # This is a replicaset master (sole member) which will be catching up
    i5.terminate()

    # Propose a space creation which will succeed
    space_id = 901
    space_def = dict(
        id=space_id,
        name="space_name_conflict",
        format=[dict(name="id", type="unsigned", is_nullable=False)],
        primary_key=["id"],
        distribution="global",
    )
    index = i1.create_space(space_def)

    i2.call(".proc_sync_raft", index, (3, 0))
    i3.call(".proc_sync_raft", index, (3, 0))

    # Space created
    assert i1.call("box.space._space:get", space_id) is not None
    assert i2.call("box.space._space:get", space_id) is not None
    assert i3.call("box.space._space:get", space_id) is not None

    # Wake up the catcher-uppers
    i4.start()
    i5.start()
    # FIXME: currently wait_lsn never stops, because lsn doesn't get replicated
    # tarantool bug?
    i4.wait_online()
    i5.wait_online()

    # They caught up!
    assert i4.call("box.space._space:get", space_id) is not None
    assert i5.call("box.space._space:get", space_id) is not None


def test_ddl_from_snapshot(cluster: Cluster):
    # Second instance is only for quorum
    i1, i2 = cluster.deploy(instance_count=2, init_replication_factor=2)

    i1.assert_raft_status("Leader")

    # TODO: check other ddl operations
    # Propose a space creation which will succeed
    space_id = 632
    cluster.create_space(
        dict(
            id=space_id,
            name="stuff",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution=dict(sharding_key=["id"], sharding_fn="murmur3"),
        ),
    )

    tt_space_def = [
        space_id,
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
    assert i1.call("box.space._space:get", space_id) == tt_space_def
    assert i2.call("box.space._space:get", space_id) == tt_space_def

    tt_pk_def = [
        space_id,
        0,
        "primary_key",
        "tree",
        dict(unique=True),
        [[0, "unsigned", None, False, None]],
    ]
    assert i1.call("box.space._index:get", [space_id, 0]) == tt_pk_def
    assert i2.call("box.space._index:get", [space_id, 0]) == tt_pk_def

    tt_bucket_id_def = [
        space_id,
        1,
        "bucket_id",
        "tree",
        dict(unique=False),
        [[1, "unsigned", None, False, None]],
    ]
    assert i1.call("box.space._index:get", [space_id, 1]) == tt_bucket_id_def
    assert i2.call("box.space._index:get", [space_id, 1]) == tt_bucket_id_def

    # Compact the log to trigger snapshot for the newcommer
    i1.raft_compact_log()
    i2.raft_compact_log()

    # A replicaset master catches up from snapshot
    i3 = cluster.add_instance(wait_online=True, replicaset_id="R2")
    assert i3.call("box.space._space:get", space_id) == tt_space_def
    assert i3.call("box.space._index:get", [space_id, 0]) == tt_pk_def
    assert i3.call("box.space._index:get", [space_id, 1]) == tt_bucket_id_def
    assert i3.call("box.space._schema:get", "pico_schema_version")[1] == 1

    # A replicaset follower catches up from snapshot
    i4 = cluster.add_instance(wait_online=True, replicaset_id="R2")
    assert i4.call("box.space._space:get", space_id) == tt_space_def
    assert i4.call("box.space._index:get", [space_id, 0]) == tt_pk_def
    assert i4.call("box.space._index:get", [space_id, 1]) == tt_bucket_id_def
    assert i4.call("box.space._schema:get", "pico_schema_version")[1] == 1
