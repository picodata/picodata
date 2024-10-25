import pytest
from conftest import (
    PICO_SERVICE_ID,
    Cluster,
    ReturnError,
    Retriable,
    Instance,
    TarantoolError,
)


def test_ddl_abort(cluster: Cluster):
    cluster.deploy(instance_count=2)

    with pytest.raises(ReturnError, match="there is no pending ddl operation"):
        cluster.abort_ddl()

    # TODO: test manual abort when we have long-running ddls


################################################################################
def test_ddl_create_table_bulky(cluster: Cluster):
    i1, i2, i3, i4 = cluster.deploy(instance_count=4, init_replication_factor=2)

    # At cluster boot schema version is 1
    assert i1.call("box.space._pico_property:get", "global_schema_version")[1] == 1
    assert i2.call("box.space._pico_property:get", "global_schema_version")[1] == 1
    assert i3.call("box.space._pico_property:get", "global_schema_version")[1] == 1
    assert i4.call("box.space._pico_property:get", "global_schema_version")[1] == 1

    # And next schema version will be 2
    assert i1.next_schema_version() == 2
    assert i2.next_schema_version() == 2
    assert i3.next_schema_version() == 2
    assert i4.next_schema_version() == 2

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
            engine="memtx",
            owner=0,
        ),
    )

    i1.raft_wait_index(abort_index, 3)
    i2.raft_wait_index(abort_index, 3)
    i3.raft_wait_index(abort_index, 3)
    i4.raft_wait_index(abort_index, 3)

    # No space was created
    assert i1.call("box.space._pico_table:get", space_id) is None
    assert i2.call("box.space._pico_table:get", space_id) is None
    assert i3.call("box.space._pico_table:get", space_id) is None
    assert i4.call("box.space._pico_table:get", space_id) is None
    assert i1.call("box.space._space:get", space_id) is None
    assert i2.call("box.space._space:get", space_id) is None
    assert i3.call("box.space._space:get", space_id) is None
    assert i4.call("box.space._space:get", space_id) is None

    # Schema version hasn't changed
    assert i1.call("box.space._pico_property:get", "global_schema_version")[1] == 1
    assert i2.call("box.space._pico_property:get", "global_schema_version")[1] == 1
    assert i3.call("box.space._pico_property:get", "global_schema_version")[1] == 1
    assert i4.call("box.space._pico_property:get", "global_schema_version")[1] == 1

    # But next schema version did change
    assert i1.next_schema_version() == 3
    assert i2.next_schema_version() == 3
    assert i3.next_schema_version() == 3
    assert i4.next_schema_version() == 3

    ############################################################################
    # Propose a space creation which will succeed

    cluster.create_table(
        dict(
            name="stuff",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        ),
    )

    space_id = i1.eval("return box.space.stuff.id")

    # This time schema version did change
    assert i1.call("box.space._pico_property:get", "global_schema_version")[1] == 3
    assert i2.call("box.space._pico_property:get", "global_schema_version")[1] == 3
    assert i3.call("box.space._pico_property:get", "global_schema_version")[1] == 3
    assert i4.call("box.space._pico_property:get", "global_schema_version")[1] == 3

    # And so did next schema version obviously
    assert i1.next_schema_version() == 4
    assert i2.next_schema_version() == 4
    assert i3.next_schema_version() == 4
    assert i4.next_schema_version() == 4

    # Space was created and is operable
    initiator_id = PICO_SERVICE_ID
    pico_space_def = [
        space_id,
        "stuff",
        {"Global": None},
        [{"field_type": "unsigned", "is_nullable": False, "name": "id"}],
        3,
        True,
        "memtx",
        initiator_id,
        "",
    ]
    assert i1.call("box.space._pico_table:get", space_id) == pico_space_def
    assert i2.call("box.space._pico_table:get", space_id) == pico_space_def
    assert i3.call("box.space._pico_table:get", space_id) == pico_space_def
    assert i4.call("box.space._pico_table:get", space_id) == pico_space_def

    tt_space_def = [
        space_id,
        initiator_id,
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
        "stuff_pkey",
        "tree",
        [dict(unique=True)],
        [["id", "unsigned", None, False, None]],
        True,
        3,
    ]
    assert i1.call("box.space._pico_index:get", [space_id, 0]) == pico_pk_def
    assert i2.call("box.space._pico_index:get", [space_id, 0]) == pico_pk_def
    assert i3.call("box.space._pico_index:get", [space_id, 0]) == pico_pk_def
    assert i4.call("box.space._pico_index:get", [space_id, 0]) == pico_pk_def

    tt_pk_def = [
        space_id,
        0,
        "stuff_pkey",
        "tree",
        dict(unique=True),
        [[0, "unsigned", None, False, None]],
    ]
    assert i1.call("box.space._index:get", [space_id, 0]) == tt_pk_def
    assert i2.call("box.space._index:get", [space_id, 0]) == tt_pk_def
    assert i3.call("box.space._index:get", [space_id, 0]) == tt_pk_def
    assert i4.call("box.space._index:get", [space_id, 0]) == tt_pk_def

    ############################################################################
    # A new replicaset boots up after the fact successfully

    i5 = cluster.add_instance(wait_online=True, replicaset_name="r3")

    assert i5.call("box.space._pico_property:get", "global_schema_version")[1] == 3
    assert i5.next_schema_version() == 4
    assert i5.call("box.space._pico_table:get", space_id) == pico_space_def
    assert i5.call("box.space._pico_index:get", [space_id, 0]) == pico_pk_def
    assert i5.call("box.space._space:get", space_id) == tt_space_def
    assert i5.call("box.space._index:get", [space_id, 0]) == tt_pk_def

    i6 = cluster.add_instance(wait_online=True, replicaset_name="r3")

    # It's schema was updated automatically as well
    assert i6.call("box.space._pico_property:get", "global_schema_version")[1] == 3
    assert i6.next_schema_version() == 4
    assert i6.call("box.space._pico_table:get", space_id) == pico_space_def
    assert i6.call("box.space._pico_index:get", [space_id, 0]) == pico_pk_def
    assert i6.call("box.space._space:get", space_id) == tt_space_def
    assert i6.call("box.space._index:get", [space_id, 0]) == tt_pk_def

    # TODO: test replica becoming master in the process of catching up


################################################################################
def test_ddl_create_sharded_space(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2, init_replication_factor=2)

    # Propose a space creation which will succeed
    schema_version = i1.next_schema_version()
    cluster.create_table(
        dict(
            name="stuff",
            format=[
                dict(name="id", type="unsigned", is_nullable=False),
                dict(name="foo", type="integer", is_nullable=False),
                dict(name="bar", type="string", is_nullable=False),
            ],
            primary_key=["id"],
            distribution="sharded",
            sharding_key=["foo", "bar"],
        ),
    )
    space_id = i1.eval("return box.space.stuff.id")

    ############################################################################
    # Space was created and is operable
    initiator_id = PICO_SERVICE_ID
    pico_space_def = [
        space_id,
        "stuff",
        {"ShardedImplicitly": [["foo", "bar"], "murmur3", "default"]},
        [
            {"field_type": "unsigned", "is_nullable": False, "name": "id"},
            # Automatically generated by picodata
            {"field_type": "unsigned", "is_nullable": False, "name": "bucket_id"},
            {"field_type": "integer", "is_nullable": False, "name": "foo"},
            {"field_type": "string", "is_nullable": False, "name": "bar"},
        ],
        schema_version,
        True,
        "memtx",
        initiator_id,
        "",
    ]
    assert i1.call("box.space._pico_table:get", space_id) == pico_space_def
    assert i2.call("box.space._pico_table:get", space_id) == pico_space_def

    tt_space_def = [
        space_id,
        initiator_id,
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
        "stuff_pkey",
        "tree",
        [dict(unique=True)],
        [["id", "unsigned", None, False, None]],
        True,
        schema_version,
    ]
    assert i1.call("box.space._pico_index:get", [space_id, 0]) == pico_pk_def
    assert i2.call("box.space._pico_index:get", [space_id, 0]) == pico_pk_def

    tt_pk_def = [
        space_id,
        0,
        "stuff_pkey",
        "tree",
        dict(unique=True),
        [[0, "unsigned", None, False, None]],
    ]
    assert i1.call("box.space._index:get", [space_id, 0]) == tt_pk_def
    assert i2.call("box.space._index:get", [space_id, 0]) == tt_pk_def

    ############################################################################
    # This time bucket id was also created
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


################################################################################
def test_ddl_create_table_unfinished_from_snapshot(cluster: Cluster):
    i1, i2, i3 = cluster.deploy(instance_count=3)

    # Put i3 to sleep, so that schema change get's blocked.
    i3.terminate()

    # Start schema change.
    space_id = 732
    index = i1.propose_create_space(
        dict(
            id=space_id,
            name="some space name",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=[dict(field="id")],
            distribution=dict(
                kind="sharded_implicitly",
                sharding_key=["id"],
                sharding_fn="murmur3",
                tier="default",
            ),
            engine="memtx",
            owner=0,
        ),
        wait_index=False,
    )

    # Schema change is blocked.
    with pytest.raises(TarantoolError, match="timeout"):
        i1.raft_wait_index(index, timeout=3)

    # Space is created but is not operable.
    assert i1.call("box.space._space:get", space_id) is not None
    assert not i1.eval("return box.space._pico_table:get(...).operable", space_id)
    assert i2.call("box.space._space:get", space_id) is not None
    assert not i2.eval("return box.space._pico_table:get(...).operable", space_id)

    # Compact raft log to trigger snapshot with an unfinished schema change.
    i1.raft_compact_log()
    i2.raft_compact_log()

    # Add a new replicaset who will boot from snapshot.
    i4 = cluster.add_instance(wait_online=True)

    # TODO: test readonly replica doing the same

    # It has received an unfinished schema change.
    assert not i4.eval("return box.space._pico_table:get(...).operable", space_id)

    # Wake the instance, who was blocking the schema change.
    i3.start()
    i3.wait_online()

    def check(instance):
        assert instance.call("box.space._space:get", space_id) is not None
        assert instance.eval("return box.space._pico_table:get(...).operable", space_id)

    # The schema change finalized.
    for i in cluster.instances:
        Retriable(timeout=10).call(check, i)


################################################################################
def test_ddl_create_table_abort(cluster: Cluster):
    i1, i2, i3 = cluster.deploy(instance_count=3, init_replication_factor=1)

    # Create a conflict to force ddl abort.
    space_name = "space_name_conflict"
    i3.eval("box.schema.space.create(...)", space_name)

    # Terminate i3 so that other instances actually partially apply the ddl.
    i3.terminate()

    # Initiate ddl create space.
    space_id = 887
    index_fin = i1.propose_create_space(
        dict(
            id=space_id,
            name=space_name,
            format=[
                dict(name="id", type="unsigned", is_nullable=False),
            ],
            primary_key=[dict(field="id")],
            distribution=dict(
                kind="sharded_implicitly",
                sharding_key=["id"],
                sharding_fn="murmur3",
                tier="default",
            ),
            engine="memtx",
            owner=0,
        ),
        wait_index=False,
    )

    index_prepare = index_fin - 1
    i1.raft_wait_index(index_prepare)
    i2.raft_wait_index(index_prepare)

    def get_index_names(i, space_id):
        return i.eval(
            """
            local space_id = ...
            local res = box.space._pico_index:select({space_id})
            for i, t in ipairs(res) do
                res[i] = t.name
            end
            return res
        """,
            space_id,
        )

    assert i1.call("box.space._space:get", space_id) is not None
    assert get_index_names(i1, space_id) == [f"{space_name}_pkey"]
    assert i2.call("box.space._space:get", space_id) is not None
    assert get_index_names(i2, space_id) == [f"{space_name}_pkey"]

    # Wake the instance so that governor finds out there's a conflict
    # and aborts the ddl op.
    i3.start()
    i3.wait_online()

    def check_table_is_gone(peer):
        assert peer.call("box.space._space:get", space_id) is None

    # Everything was cleaned up.
    Retriable(timeout=10).call(check_table_is_gone, i1)
    Retriable(timeout=10).call(check_table_is_gone, i2)
    Retriable(timeout=10).call(check_table_is_gone, i3)

    assert get_index_names(i1, space_id) == []
    assert get_index_names(i2, space_id) == []
    assert get_index_names(i3, space_id) == []


################################################################################
def test_ddl_create_table_partial_failure(cluster: Cluster):
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
        engine="memtx",
        owner=0,  # guest
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
    with pytest.raises(TarantoolError, match="timeout"):
        i1.raft_wait_index(index, timeout=3)

    entry, *_ = i1.call(
        "box.space._raft_log:select", None, dict(iterator="lt", limit=1)
    )
    # Has not yet been finalized
    assert entry[4][0] == "ddl_prepare"

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


################################################################################
def test_successful_wakeup_after_ddl(cluster: Cluster):
    # Manual replicaset distribution.
    i1 = cluster.add_instance(replicaset_name="r1", wait_online=True)
    i2 = cluster.add_instance(replicaset_name="r2", wait_online=True)
    i3 = cluster.add_instance(replicaset_name="r2", wait_online=True)

    # This is a replica which will be catching up
    i3.terminate()
    # Replicaset master cannot wakeup after a ddl, because all masters must be
    # present for the ddl to be committed.

    # Propose a space creation which will succeed
    space_def = dict(
        name="ids",
        format=[dict(name="id", type="unsigned", is_nullable=False)],
        primary_key=["id"],
        distribution="global",
    )
    index = i1.create_table(space_def)
    i2.raft_wait_index(index, 3)
    space_id = i1.eval("return box.space.ids.id")

    # Space created
    assert i1.call("box.space._space:get", space_id) is not None
    assert i2.call("box.space._space:get", space_id) is not None

    # Wake up the catching-up instance.
    i3.start()
    i3.wait_online()

    # It caught up!
    assert i3.call("box.space._space:get", space_id) is not None


################################################################################
def test_ddl_create_table_from_snapshot_at_boot(cluster: Cluster):
    # Second instance is only for quorum
    i1, i2 = cluster.deploy(instance_count=2, init_replication_factor=2)

    i1.assert_raft_status("Leader")

    # TODO: check other ddl operations
    # Propose a space creation which will succeed
    cluster.create_table(
        dict(
            name="stuff",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="sharded",
            sharding_key=["id"],
        ),
    )
    space_id = i1.eval("return box.space.stuff.id")

    initiator_id = PICO_SERVICE_ID
    tt_space_def = [
        space_id,
        initiator_id,
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
        "stuff_pkey",
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
    i3 = cluster.add_instance(wait_online=True, replicaset_name="R2")
    assert i3.call("box.space._space:get", space_id) == tt_space_def
    assert i3.call("box.space._index:get", [space_id, 0]) == tt_pk_def
    assert i3.call("box.space._index:get", [space_id, 1]) == tt_bucket_id_def
    assert i3.call("box.space._schema:get", "local_schema_version")[1] == 2

    # A replicaset follower boots up from snapshot
    i4 = cluster.add_instance(wait_online=True, replicaset_name="R2")
    assert i4.call("box.space._space:get", space_id) == tt_space_def
    assert i4.call("box.space._index:get", [space_id, 0]) == tt_pk_def
    assert i4.call("box.space._index:get", [space_id, 1]) == tt_bucket_id_def
    assert i4.call("box.space._schema:get", "local_schema_version")[1] == 2


################################################################################
def test_ddl_create_table_from_snapshot_at_catchup(cluster: Cluster):
    # Second instance is only for quorum
    i1 = cluster.add_instance(wait_online=True, replicaset_name="r1")
    i2 = cluster.add_instance(wait_online=True, replicaset_name="R2")
    i3 = cluster.add_instance(wait_online=True, replicaset_name="R2")

    i1.assert_raft_status("Leader")

    i3.terminate()

    # TODO: check other ddl operations
    # Propose a space creation which will succeed
    index = i1.create_table(
        dict(
            name="stuff",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
            engine="memtx",
        ),
    )
    i1.raft_wait_index(index)
    i2.raft_wait_index(index)
    space_id = i1.eval("return box.space.stuff.id")

    initiator_id = PICO_SERVICE_ID
    tt_space_def = [
        space_id,
        initiator_id,
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
        "stuff_pkey",
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
    assert i3.call("box.space._schema:get", "local_schema_version")[1] == 2


################################################################################
def test_ddl_create_table_at_catchup_with_master_switchover(cluster: Cluster):
    # For quorum.
    i1, i2 = cluster.deploy(instance_count=2, init_replication_factor=1)
    # This is a master, who will be present at ddl.
    i3 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    # This is a replica, who will become master and will catch up.
    i4 = cluster.add_instance(wait_online=True, replicaset_name="r99")

    i4.terminate()

    # TODO: check other ddl operations
    # Propose a space creation which will succeed
    space_name = "table"
    cluster.create_table(
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


################################################################################
def test_ddl_drop_table_normal(cluster: Cluster):
    # 2 replicasets with 2 replicas each
    i1, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)

    # Set up.
    space_name = "things"
    cluster.create_table(
        dict(
            name=space_name,
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        ),
    )

    for i in cluster.instances:
        assert i.call("box.space._space.index.name:get", space_name) is not None

    # Actual behaviour we're testing
    cluster.drop_table(space_name)

    for i in cluster.instances:
        assert i.call("box.space._space.index.name:get", space_name) is None

    # Now we can create another space with the same name.
    cluster.create_table(
        dict(
            name=space_name,
            format=[
                dict(name="id", type="unsigned", is_nullable=False),
                dict(name="value", type="double", is_nullable=False),
            ],
            primary_key=["id"],
            distribution="global",
        ),
    )

    for i in cluster.instances:
        assert i.call("box.space._space.index.name:get", space_name) is not None


################################################################################
def test_ddl_drop_table_partial_failure(cluster: Cluster):
    # First 3 are fore quorum.
    i1, i2, i3 = cluster.deploy(instance_count=3, init_replication_factor=1)
    # Test subjects.
    i4 = cluster.add_instance(wait_online=True, replicaset_name="R99")
    i5 = cluster.add_instance(wait_online=True, replicaset_name="R99")

    # Set up.
    table_name = "trinkets"
    cluster.create_table(
        dict(
            name=table_name,
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        ),
    )
    index = i1.cas("insert", table_name, [9])
    for i in cluster.instances:
        i.raft_wait_index(index)

    # Put a replicaset to sleep.
    i4.terminate()
    i5.terminate()

    table_id = i1.sql(
        """
        SELECT "id" FROM "_pico_table" WHERE "name"=?
        """,
        table_name,
        sudo=True,
    )[0][0]

    # Ddl fails because all masters must be present.
    with pytest.raises(TarantoolError, match="timeout"):
        i1.drop_table(table_name)

    # Has not yet been finalized
    pending_schema_change = i1.call(
        "box.space._pico_property:get", "pending_schema_change"
    )
    assert pending_schema_change[1][0] == "drop_table"
    assert pending_schema_change[1][1] == table_id

    # Space is not yet dropped.
    assert i1.call("box.space._space.index.name:get", table_name) is not None
    assert i2.call("box.space._space.index.name:get", table_name) is not None
    assert i3.call("box.space._space.index.name:get", table_name) is not None

    # And no data is lost yet.
    assert i1.call("box.space.trinkets:get", 9) == [9]
    assert i2.call("box.space.trinkets:get", 9) == [9]
    assert i3.call("box.space.trinkets:get", 9) == [9]

    # But the space is marked not operable.
    rows = i1.sql('select "operable" from "_pico_table" where "name" = ?', table_name)
    assert rows == [[False]]
    rows = i2.sql('select "operable" from "_pico_table" where "name" = ?', table_name)
    assert rows == [[False]]
    rows = i3.sql('select "operable" from "_pico_table" where "name" = ?', table_name)
    assert rows == [[False]]

    # TODO: test manual ddl abort

    # Wakeup the sleeping master.
    i4.start()
    i4.wait_online()

    def check_no_pending_schema_change(i: Instance):
        rows = i.sql(
            """select count(*) from "_pico_property" where "key" = 'pending_schema_change'"""
        )
        assert rows == [[0]]

    # Wait until the schema change is finalized
    Retriable(timeout=5, rps=2).call(check_no_pending_schema_change, i1)

    # Now space is dropped.
    assert i1.call("box.space._space.index.name:get", table_name) is None
    assert i2.call("box.space._space.index.name:get", table_name) is None
    assert i3.call("box.space._space.index.name:get", table_name) is None
    assert i4.call("box.space._space.index.name:get", table_name) is None

    # And a replica catches up by raft log successfully.
    i5.start()
    i5.wait_online()
    assert i5.call("box.space._space.index.name:get", table_name) is None


################################################################################
def test_ddl_drop_table_by_raft_log_at_catchup(cluster: Cluster):
    # i1 is for quorum
    i1, *_ = cluster.deploy(instance_count=1, init_replication_factor=1)
    i2 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    # This one will be catching up.
    i3 = cluster.add_instance(wait_online=True, replicaset_name="r99")

    # Set up.
    cluster.create_table(
        dict(
            name="replace_me",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="sharded",
            sharding_key=["id"],
        ),
    )
    for i in cluster.instances:
        assert i.call("box.space._space.index.name:get", "replace_me") is not None

    cluster.create_table(
        dict(
            name="drop_me",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        ),
    )
    for i in cluster.instances:
        assert i.call("box.space._space.index.name:get", "drop_me") is not None

    # i3 will be catching up.
    i3.terminate()

    # Drop the spaces
    for space_name in ["replace_me", "drop_me"]:
        cluster.drop_table(space_name)
        assert i1.call("box.space._space.index.name:get", space_name) is None
        assert i2.call("box.space._space.index.name:get", space_name) is None

    #
    # We replace a sharded space with a global one to check indexes were dropped
    # correctly.
    cluster.create_table(
        dict(
            name="replace_me",
            format=[
                dict(name="#", type="unsigned", is_nullable=False),
            ],
            primary_key=["#"],
            distribution="global",
        ),
    )

    assert i1.call("box.space._space.index.name:get", "replace_me") is not None
    assert i2.call("box.space._space.index.name:get", "replace_me") is not None

    # Wake up the catching up instance.
    i3.start()
    i3.wait_online()
    # Wait for the raft index at which "drop_me" is dropped.
    i3.raft_wait_index(i1.raft_get_index())

    # The space was dropped.
    assert i3.call("box.space._space.index.name:get", "drop_me") is None

    # The space was dropped and a new one was created without conflict.
    format = i3.eval("return box.space[...]:format()", "replace_me")
    assert [f["name"] for f in format] == ["#"]


################################################################################
def test_ddl_drop_table_by_raft_log_at_boot(cluster: Cluster):
    # These guys are for quorum.
    i1, i2 = cluster.deploy(instance_count=2, init_replication_factor=1)

    #
    # Set up.
    #
    cluster.create_table(
        dict(
            name="replace_me",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="sharded",
            sharding_key=["id"],
        ),
    )
    for i in cluster.instances:
        assert i.call("box.space._space.index.name:get", "replace_me") is not None

    cluster.create_table(
        dict(
            name="drop_me",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        ),
    )
    for i in cluster.instances:
        assert i.call("box.space._space.index.name:get", "drop_me") is not None

    #
    # Drop spaces.
    #
    for space_name in ["replace_me", "drop_me"]:
        cluster.drop_table(space_name)
        assert i1.call("box.space._space.index.name:get", space_name) is None
        assert i2.call("box.space._space.index.name:get", space_name) is None

    #
    # We replace a sharded space with a global one to check indexes were dropped
    # correctly.
    cluster.create_table(
        dict(
            name="replace_me",
            format=[
                dict(name="#", type="unsigned", is_nullable=False),
            ],
            primary_key=["#"],
            distribution="global",
        ),
    )

    assert i1.call("box.space._space.index.name:get", "replace_me") is not None
    assert i2.call("box.space._space.index.name:get", "replace_me") is not None

    #
    # Add a new replicaset.
    #
    i3 = cluster.add_instance(wait_online=False, replicaset_name="r99")
    i4 = cluster.add_instance(wait_online=False, replicaset_name="r99")
    i3.start()
    i4.start()
    i3.wait_online()
    i4.wait_online()

    #
    # Both caught up successfully.
    #
    assert i3.call("box.space._space.index.name:get", "drop_me") is None
    assert i4.call("box.space._space.index.name:get", "drop_me") is None

    format = i3.eval("return box.space[...]:format()", "replace_me")
    assert [f["name"] for f in format] == ["#"]
    format = i4.eval("return box.space[...]:format()", "replace_me")
    assert [f["name"] for f in format] == ["#"]


################################################################################
def test_ddl_drop_table_by_snapshot_on_replica(cluster: Cluster):
    # i1 is for quorum
    i1, *_ = cluster.deploy(instance_count=1, init_replication_factor=1)
    i2 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    # This one will be catching up.
    i3 = cluster.add_instance(wait_online=True, replicaset_name="r99")

    # Set up.
    cluster.create_table(
        dict(
            name="replace_me",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="sharded",
            sharding_key=["id"],
        ),
    )
    for i in cluster.instances:
        assert i.call("box.space._space.index.name:get", "replace_me") is not None

    cluster.create_table(
        dict(
            name="drop_me",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="sharded",
            sharding_key=["id"],
        ),
    )
    for i in cluster.instances:
        assert i.call("box.space._space.index.name:get", "drop_me") is not None

    # i3 will be catching up.
    i3.terminate()

    for space_name in ["replace_me", "drop_me"]:
        cluster.drop_table(space_name)
        assert i1.call("box.space._space.index.name:get", space_name) is None
        assert i2.call("box.space._space.index.name:get", space_name) is None

    # We replace a sharded space with a global one to check indexes were dropped
    # correctly.
    cluster.create_table(
        dict(
            name="replace_me",
            format=[
                dict(name="#", type="unsigned", is_nullable=False),
            ],
            primary_key=["#"],
            distribution="global",
        ),
    )

    assert i1.call("box.space._space.index.name:get", "replace_me") is not None
    assert i2.call("box.space._space.index.name:get", "replace_me") is not None

    # Compact raft log to trigger snapshot generation.
    i1.raft_compact_log()
    i2.raft_compact_log()

    # Wake up the catching up instance.
    i3.start()
    i3.wait_online()

    # The space was dropped.
    assert i3.call("box.space._space.index.name:get", "drop_me") is None

    # The space was dropped and a new one was created without conflict.
    format = i3.eval("return box.space[...]:format()", "replace_me")
    assert [f["name"] for f in format] == ["#"]


################################################################################
def test_ddl_drop_table_by_snapshot_on_master(cluster: Cluster):
    # These ones are for quorum.
    i1, i2 = cluster.deploy(instance_count=2, init_replication_factor=1)
    # This is a replicaset master, who will be following along with the ddl.
    i3 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    # This is a replica, who will become master and be catching up.
    i4 = cluster.add_instance(wait_online=True, replicaset_name="r99")

    # Set up.
    cluster.create_table(
        dict(
            name="space_to_drop",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        ),
    )
    cluster.create_table(
        dict(
            name="space_to_replace",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="sharded",
            sharding_key=["id"],
        ),
    )

    for space_name in ["space_to_drop", "space_to_replace"]:
        for i in cluster.instances:
            assert i.call("box.space._space.index.name:get", space_name) is not None

    # i4 will be catching up.
    i4.terminate()

    #
    # Drop spaces.
    #
    for space_name in ["space_to_drop", "space_to_replace"]:
        cluster.drop_table(space_name)
        assert i1.call("box.space._space.index.name:get", space_name) is None
        assert i2.call("box.space._space.index.name:get", space_name) is None
        assert i3.call("box.space._space.index.name:get", space_name) is None

    # We replace a sharded space with a global one to check indexes were dropped
    # correctly.
    cluster.create_table(
        dict(
            name="space_to_replace",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        ),
    )

    assert i1.call("box.space._space.index.name:get", "space_to_replace") is not None
    assert i2.call("box.space._space.index.name:get", "space_to_replace") is not None
    assert i3.call("box.space._space.index.name:get", "space_to_replace") is not None

    # Compact raft log to trigger snapshot generation.
    i1.raft_compact_log()
    i2.raft_compact_log()

    # Put i3 to sleep to trigger master switchover.
    i3.terminate()

    # Wake up the catching up instance. i4 has become master and.
    i4.start()
    i4.wait_online()

    # The space was dropped.
    # assert i4.call("box.space._space.index.name:get", "space_to_drop") is None
    # The space was replaced.
    assert i4.call("box.space._space.index.name:get", "space_to_replace") is not None


################################################################################
def test_local_spaces_dont_conflict_with_pico_create_table(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    cluster.create_table(
        dict(
            name="a space",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        )
    )
    assert i1.eval("return box.space._space.index.name:get(...).id", "a space") == 1025

    i1.call("box.execute", 'create table "another space" ("id" unsigned primary key)')
    assert (
        i1.eval("return box.space._space.index.name:get(...).id", "another space")
        == 1026
    )

    cluster.create_table(
        dict(
            name="one more space",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        )
    )
    assert (
        i1.eval("return box.space._space.index.name:get(...).id", "one more space")
        == 1027
    )


################################################################################
def test_pico_create_table_doesnt_conflict_with_local_spaces(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    # Tarantool doesn't care about _schema.max_id anymore and now if we want it
    # to put spaces into our id range, we have to do so explicitly, otherwise
    # space will have id in the picodata reserved range.
    i1.eval("box.schema.create_space(...)", "a space", dict(id=1025))
    assert i1.eval("return box.space._space.index.name:get(...).id", "a space") == 1025

    cluster.create_table(
        dict(
            name="another space",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        )
    )
    assert (
        i1.eval("return box.space._space.index.name:get(...).id", "another space")
        == 1026
    )

    i1.call("box.execute", 'create table "one more space" ("id" unsigned primary key)')
    assert (
        i1.eval("return box.space._space.index.name:get(...).id", "one more space")
        == 1027
    )


################################################################################
def test_ddl_alter_space_by_snapshot(cluster: Cluster):
    # These ones are for quorum.
    i1 = cluster.add_instance(wait_online=True, replicaset_name="R1")
    i2 = cluster.add_instance(wait_online=True, replicaset_name="R1")
    i3 = cluster.add_instance(wait_online=True, replicaset_name="R1")
    i4 = cluster.add_instance(wait_online=True, replicaset_name="R2")
    i5 = cluster.add_instance(wait_online=True, replicaset_name="R2")

    #
    # Set up.
    #
    space_name = "space_which_changes_format"
    cluster.create_table(
        dict(
            name=space_name,
            format=[
                dict(name="id", type="unsigned", is_nullable=False),
                dict(name="value", type="unsigned", is_nullable=False),
            ],
            primary_key=["id"],
            distribution="global",
        ),
    )

    for i in cluster.instances:
        assert i.call("box.space._space.index.name:get", space_name) is not None

    for row in ([1, 10], [2, 20], [3, 30]):
        index = cluster.cas("insert", space_name, row)
        cluster.raft_wait_index(index, 3)

    #
    # This one will be catching up by snapshot.
    #
    i5.terminate()

    #
    # Change the space format.
    #
    cluster.drop_table(space_name)
    assert i1.call("box.space._space.index.name:get", space_name) is None
    assert i2.call("box.space._space.index.name:get", space_name) is None
    assert i3.call("box.space._space.index.name:get", space_name) is None
    assert i4.call("box.space._space.index.name:get", space_name) is None

    cluster.create_table(
        dict(
            name=space_name,
            format=[
                dict(name="id", type="unsigned", is_nullable=False),
                dict(name="value", type="string", is_nullable=False),
            ],
            primary_key=["id"],
            distribution="global",
        ),
    )

    for row in ([1, "one"], [2, "two"], [3, "three"]):  # type: ignore
        index = cluster.cas("insert", space_name, row)
        cluster.raft_wait_index(index, 3)

    # Compact raft log to trigger snapshot generation.
    i1.raft_compact_log()
    i2.raft_compact_log()
    i3.raft_compact_log()
    i4.raft_compact_log()

    # Shut down the replicaset master to trigger switchover.
    i4.terminate()

    # Wake up the catching up instance which becomes the master.
    i5.start()
    i5.wait_online()

    # The space was replaced.
    rows = i5.eval("return box.space[...]:select()", space_name)
    assert rows == [[1, "one"], [2, "two"], [3, "three"]]


def test_ddl_when_box_cfg_read_only(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    default_replication_factor: 2
    name: test
    tier:
        default:
"""
    )

    i1 = cluster.add_instance(tier="default")
    i1.start()
    i1.wait_online()

    i2 = cluster.add_instance(tier="default")
    i2.start()
    i2.wait_online()

    i2.create_user(with_name="andy", with_password="Testpa55", with_auth="chap-sha1")
    i2.sql('GRANT CREATE TABLE TO "andy"', sudo=True)

    read_only = i2.eval("return box.cfg.read_only")
    assert read_only

    with i2.connect(timeout=5, user="andy", password="Testpa55") as conn:
        query = conn.sql(
            """
            CREATE TABLE ids
                (id INTEGER NOT NULL,
                 PRIMARY KEY(id))
                 USING MEMTX DISTRIBUTED BY (id);
            """,
        )
        assert query["row_count"] == 1


def test_long_term_transaction_causing_rpc_timeouts(cluster: Cluster):
    """
    This test is designed to reproduces the issue described in
    https://git.picodata.io/picodata/picodata/picodata/-/issues/748
    """
    i1, i2, _ = cluster.deploy(instance_count=3)

    ddl = i1.sql("CREATE TABLE t (id INT PRIMARY KEY, data INT, data2 INT)")
    assert ddl["row_count"] == 1

    # Simulate a long-term transaction by blocking the next schema change for 1.5 seconds.
    # The RPC timeout is set to 1 second, so this block will trigger an RPC timeout.
    # After the timeout occurs, another RPC will be re-sent and blocked by the schema change lock.
    # Once the injection is disabled, the initial RPC will create the index, complete
    # the transaction and release the lock, allowing subsequent RPC to begin and send an
    # acknowledgement to the governor.
    i2.eval(
        """
        local fiber = require('fiber')
        function block_next_apply_schema_change_transaction_for_one_and_a_half_secs()
            pico._inject_error("BLOCK_APPLY_SCHEMA_CHANGE_TRANSACTION", true)
            fiber.sleep(1.5)
            pico._inject_error("BLOCK_APPLY_SCHEMA_CHANGE_TRANSACTION", false)
        end
        fiber.create(block_next_apply_schema_change_transaction_for_one_and_a_half_secs)
        """
    )

    ddl = i1.sql("CREATE INDEX tdata ON t (data) OPTION (TIMEOUT = 3)")
    assert ddl["row_count"] == 1


def test_wait_applied_options(cluster: Cluster):
    i1, i2, _ = cluster.deploy(instance_count=3)

    # Wait applied options shouldn't affect operations in stable networks.
    ddl = i1.sql(
        """
        CREATE TABLE t1 (id INT PRIMARY KEY)
        WAIT APPLIED GLOBALLY
        OPTION (TIMEOUT = 3)
        """
    )
    assert ddl["row_count"] == 1

    ddl = i2.sql(
        """
        CREATE TABLE t2 (id INT PRIMARY KEY)
        WAIT APPLIED LOCALLY
        OPTION (TIMEOUT = 3)
        """
    )
    assert ddl["row_count"] == 1

    # Simulate unstable network by injecting an error blocking wait index RPC
    # that is called by the client to get acknowledgements from other
    # replicasets that the DDL operation is committed locally.
    i2.call("pico._inject_error", "BLOCK_PROC_WAIT_INDEX", True)

    # i2 doesn't acknowledge operation commitment, so WAIT APPLIED GLOBALLY
    # option results in an error.
    with pytest.raises(
        TarantoolError,
        match="ddl operation committed, "
        "but failed to receive acknowledgements from all replicasets",
    ):
        i1.sql(
            """
            CREATE TABLE t3 (id INT PRIMARY KEY)
            WAIT APPLIED GLOBALLY
            OPTION (TIMEOUT = 1)
            """
        )

    # Verify that the table was created despite the timeout error.
    ddl = i1.sql(
        """
        CREATE TABLE IF NOT EXISTS t3 (id INT PRIMARY KEY)
        WAIT APPLIED GLOBALLY
        OPTION (TIMEOUT = 1)
        """
    )
    assert ddl["row_count"] == 0

    # WAIT APPLIED LOCALLY doesn't require acknowlegments from other
    # replicasets, so operation should be performed with no errors.
    ddl = i1.sql(
        """
        CREATE TABLE t4 (id INT PRIMARY KEY)
        WAIT APPLIED LOCALLY
        OPTION (TIMEOUT = 3)
        """
    )
    assert ddl["row_count"] == 1

    # Disable injection.
    i2.call("pico._inject_error", "BLOCK_PROC_WAIT_INDEX", False)

    # WAIT APPLIED with other SQL commands
    ddl = i1.sql(
        """
        CREATE TABLE t (id INT PRIMARY KEY)
        WAIT APPLIED GLOBALLY
        OPTION (TIMEOUT = 3)
        """
    )
    assert ddl["row_count"] == 1

    ddl = i1.sql(
        """
        CREATE INDEX index ON t (id)
        WAIT APPLIED GLOBALLY
        OPTION (TIMEOUT = 3)
        """
    )
    assert ddl["row_count"] == 1

    ddl = i1.sql(
        """
        DROP INDEX index
        WAIT APPLIED GLOBALLY
        OPTION (TIMEOUT = 3)
        """
    )
    assert ddl["row_count"] == 1

    ddl = i1.sql(
        """
        CREATE PROCEDURE proc(INT)
        LANGUAGE SQL
        AS $$INSERT INTO t VALUES(?)$$
        WAIT APPLIED GLOBALLY
        OPTION (TIMEOUT = 3)
        """
    )
    assert ddl["row_count"] == 1

    ddl = i1.sql(
        """
        DROP PROCEDURE proc
        WAIT APPLIED GLOBALLY
        OPTION (TIMEOUT = 3)
        """
    )
    assert ddl["row_count"] == 1
