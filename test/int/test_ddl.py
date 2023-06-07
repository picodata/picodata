import pytest
from conftest import Cluster, ReturnError


def test_ddl_abort(cluster: Cluster):
    cluster.deploy(instance_count=2)

    with pytest.raises(ReturnError) as e1:
        cluster.abort_ddl()
    assert e1.value.args == ("ddl failed: there is no pending ddl operation",)

    # TODO: test manual abort when we have long-running ddls


def test_ddl_create_space_lua(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    # Successful global space creation
    space_id = 1026
    cluster.create_space(
        dict(
            id=space_id,
            name="some_name",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        )
    )
    pico_space_def = [
        space_id,
        "some_name",
        ["global"],
        [["id", "unsigned", False]],
        1,
        True,
    ]
    assert i1.call("box.space._pico_space:get", space_id) == pico_space_def
    assert i2.call("box.space._pico_space:get", space_id) == pico_space_def

    # Space creation error
    with pytest.raises(ReturnError) as e1:
        cluster.create_space(
            dict(
                id=1027,
                name="different_name",
                format=[dict(name="id", type="unsigned", is_nullable=False)],
                primary_key=["not_defined"],
                distribution="global",
            )
        )
    assert e1.value.args == (
        "ddl failed: space creation failed: no field with name: not_defined",
    )

    # Automatic space id
    cluster.create_space(
        dict(
            name="space 2",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        )
    )
    space_id = 1027
    pico_space_def = [
        space_id,
        "space 2",
        ["global"],
        [["id", "unsigned", False]],
        2,
        True,
    ]
    assert i1.call("box.space._pico_space:get", space_id) == pico_space_def
    assert i2.call("box.space._pico_space:get", space_id) == pico_space_def

    # Another one
    cluster.create_space(
        dict(
            name="space the third",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        )
    )
    space_id = 1028
    pico_space_def = [
        space_id,
        "space the third",
        ["global"],
        [["id", "unsigned", False]],
        3,
        True,
    ]
    assert i1.call("box.space._pico_space:get", space_id) == pico_space_def
    assert i2.call("box.space._pico_space:get", space_id) == pico_space_def


def test_ddl_create_space_bulky(cluster: Cluster):
    i1, i2, i3, i4 = cluster.deploy(instance_count=4, init_replication_factor=2)

    # At cluster boot schema version is 0
    assert i1.call("box.space._pico_property:get", "global_schema_version")[1] == 0
    assert i2.call("box.space._pico_property:get", "global_schema_version")[1] == 0
    assert i3.call("box.space._pico_property:get", "global_schema_version")[1] == 0
    assert i4.call("box.space._pico_property:get", "global_schema_version")[1] == 0

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

    i1.raft_wait_index(abort_index, 3)
    i2.raft_wait_index(abort_index, 3)
    i3.raft_wait_index(abort_index, 3)
    i4.raft_wait_index(abort_index, 3)

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
    assert i1.call("box.space._pico_property:get", "global_schema_version")[1] == 0
    assert i2.call("box.space._pico_property:get", "global_schema_version")[1] == 0
    assert i3.call("box.space._pico_property:get", "global_schema_version")[1] == 0
    assert i4.call("box.space._pico_property:get", "global_schema_version")[1] == 0

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
    assert i1.call("box.space._pico_property:get", "global_schema_version")[1] == 2
    assert i2.call("box.space._pico_property:get", "global_schema_version")[1] == 2
    assert i3.call("box.space._pico_property:get", "global_schema_version")[1] == 2
    assert i4.call("box.space._pico_property:get", "global_schema_version")[1] == 2

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

    assert i5.call("box.space._pico_property:get", "global_schema_version")[1] == 2
    assert i5.next_schema_version() == 3
    assert i5.call("box.space._pico_space:get", space_id) == pico_space_def
    assert i5.call("box.space._pico_index:get", [space_id, 0]) == pico_pk_def
    assert i5.call("box.space._space:get", space_id) == tt_space_def
    assert i5.call("box.space._index:get", [space_id, 0]) == tt_pk_def

    i6 = cluster.add_instance(wait_online=True, replicaset_id="r3")

    # It's schema was updated automatically as well
    assert i6.call("box.space._pico_property:get", "global_schema_version")[1] == 2
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
            distribution="sharded",
            sharding_key=["foo", "bar"],
            sharding_fn="murmur3",
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
    # i2 & i3 are for quorum
    i1, i2, i3, i4, i5 = cluster.deploy(instance_count=5)

    # Create a conflict to block clusterwide space creation.
    space_name = "space_name_conflict"
    i4.eval("box.schema.space.create(...)", space_name)
    i5.eval("box.schema.space.create(...)", space_name)

    # Propose a space creation which will fail
    space_id = 876
    space_def = dict(
        id=space_id,
        name=space_name,
        format=[dict(name="id", type="unsigned", is_nullable=False)],
        primary_key=[dict(field="id")],
        distribution=dict(kind="global"),
    )
    index = i1.propose_create_space(space_def)

    i2.raft_wait_index(index)
    i3.raft_wait_index(index)
    i4.raft_wait_index(index)
    i5.raft_wait_index(index)

    # No space was created
    assert i1.call("box.space._space:get", space_id) is None
    assert i2.call("box.space._space:get", space_id) is None
    assert i3.call("box.space._space:get", space_id) is None
    assert i4.call("box.space._space:get", space_id) is None
    assert i5.call("box.space._space:get", space_id) is None

    # Put one of the conflicting instances to sleep, to showcase it doesn't fix
    # the conflict
    i5.terminate()

    # Fix the conflict on the other instance.
    i4.eval("box.space[...]:drop()", space_name)

    # Propose again, now the proposal hangs indefinitely, because all replicaset
    # masters are required to be present during schema change, but i5 is asleep.
    index = i1.propose_create_space(space_def, wait_index=False)
    with pytest.raises(ReturnError, match="timeout"):
        i1.raft_wait_index(index, timeout=3)

    entry, *_ = i1.call(
        "box.space._raft_log:select", None, dict(iterator="lt", limit=1)
    )
    # Has not yet been finalized
    assert entry[4][1][0] == "ddl_prepare"

    # Expel the last conflicting instance to fix the conflict.
    i1.call("pico.expel", "i5")
    applied_index = i1.call("box.space._raft_state:get", "applied")[1]

    # After that expel we expect a ddl commit
    i1.raft_wait_index(applied_index + 1)
    i2.raft_wait_index(applied_index + 1)
    i3.raft_wait_index(applied_index + 1)
    i4.raft_wait_index(applied_index + 1)

    # Now ddl has been applied
    assert i1.call("box.space._space:get", space_id) is not None
    assert i2.call("box.space._space:get", space_id) is not None
    assert i3.call("box.space._space:get", space_id) is not None
    assert i4.call("box.space._space:get", space_id) is not None


def test_successful_wakeup_after_ddl(cluster: Cluster):
    # Manual replicaset distribution.
    i1 = cluster.add_instance(replicaset_id="r1", wait_online=True)
    i2 = cluster.add_instance(replicaset_id="r2", wait_online=True)
    i3 = cluster.add_instance(replicaset_id="r2", wait_online=True)

    # This is a replica which will be catching up
    i3.terminate()
    # Replicaset master cannot wakeup after a ddl, because all masters must be
    # present for the ddl to be committed.

    # Propose a space creation which will succeed
    space_id = 901
    space_def = dict(
        id=space_id,
        name="ids",
        format=[dict(name="id", type="unsigned", is_nullable=False)],
        primary_key=["id"],
        distribution="global",
    )
    index = i1.create_space(space_def)

    i2.raft_wait_index(index, 3)

    # Space created
    assert i1.call("box.space._space:get", space_id) is not None
    assert i2.call("box.space._space:get", space_id) is not None

    # Wake up the catching-up instance.
    i3.start()
    i3.wait_online()

    # It caught up!
    assert i3.call("box.space._space:get", space_id) is not None


def test_ddl_from_snapshot_at_boot(cluster: Cluster):
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
            distribution="sharded",
            sharding_key=["id"],
            sharding_fn="murmur3",
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

    # A replicaset master boots up from snapshot
    i3 = cluster.add_instance(wait_online=True, replicaset_id="R2")
    assert i3.call("box.space._space:get", space_id) == tt_space_def
    assert i3.call("box.space._index:get", [space_id, 0]) == tt_pk_def
    assert i3.call("box.space._index:get", [space_id, 1]) == tt_bucket_id_def
    assert i3.call("box.space._schema:get", "local_schema_version")[1] == 1

    # A replicaset follower boots up from snapshot
    i4 = cluster.add_instance(wait_online=True, replicaset_id="R2")
    assert i4.call("box.space._space:get", space_id) == tt_space_def
    assert i4.call("box.space._index:get", [space_id, 0]) == tt_pk_def
    assert i4.call("box.space._index:get", [space_id, 1]) == tt_bucket_id_def
    assert i4.call("box.space._schema:get", "local_schema_version")[1] == 1


def test_ddl_from_snapshot_at_catchup(cluster: Cluster):
    # Second instance is only for quorum
    i1 = cluster.add_instance(wait_online=True, replicaset_id="r1")
    i2 = cluster.add_instance(wait_online=True, replicaset_id="R2")
    i3 = cluster.add_instance(wait_online=True, replicaset_id="R2")

    i1.assert_raft_status("Leader")

    i3.terminate()

    # TODO: check other ddl operations
    # Propose a space creation which will succeed
    space_id = 649
    index = i1.create_space(
        dict(
            id=space_id,
            name="stuff",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        ),
    )
    i1.raft_wait_index(index)
    i2.raft_wait_index(index)

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

    # Compact the log to trigger snapshot applying on the catching up instance
    i1.raft_compact_log()
    i2.raft_compact_log()

    # Wake up the catching up instance
    i3.start()
    i3.wait_online()

    # A replica catches up by snapshot
    assert i3.call("box.space._space:get", space_id) == tt_space_def
    assert i3.call("box.space._index:get", [space_id, 0]) == tt_pk_def
    assert i3.call("box.space._schema:get", "local_schema_version")[1] == 1


def test_ddl_create_space_at_catchup_with_master_switchover(cluster: Cluster):
    # For quorum.
    i1, i2 = cluster.deploy(instance_count=2, init_replication_factor=1)
    # This is a master, who will be present at ddl.
    i3 = cluster.add_instance(wait_online=True, replicaset_id="r99")
    # This is a replica, who will become master and will catch up.
    i4 = cluster.add_instance(wait_online=True, replicaset_id="r99")

    i4.terminate()

    # TODO: check other ddl operations
    # Propose a space creation which will succeed
    space_name = "table"
    cluster.create_space(
        dict(
            name=space_name,
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        ),
    )

    assert i1.call("box.space._space.index.name:get", space_name) is not None
    assert i2.call("box.space._space.index.name:get", space_name) is not None
    assert i3.call("box.space._space.index.name:get", space_name) is not None

    # Compact the log to trigger snapshot applying on the catching up instance
    i1.raft_compact_log()
    i2.raft_compact_log()

    # Terminate master to trigger switchover.
    i3.terminate()

    # Wake up the catching up instance, who will also become master.
    i4.start()
    i4.wait_online()

    # A master catches up by snapshot
    assert i4.call("box.space._space.index.name:get", space_name) is not None
