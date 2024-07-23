import pytest
from conftest import PICO_SERVICE_ID, Cluster, ReturnError, Retriable, Instance


def test_ddl_abort(cluster: Cluster):
    cluster.deploy(instance_count=2)

    with pytest.raises(ReturnError, match="there is no pending ddl operation"):
        cluster.abort_ddl()

    # TODO: test manual abort when we have long-running ddls


################################################################################
def test_ddl_lua_api(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    #
    # pico.create_table
    #

    # Successful global space creation
    cluster.create_table(
        dict(
            id=1026,
            name="some_name",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        )
    )

    # Called with the same args -> ok.
    cluster.create_table(
        dict(
            id=1026,
            name="some_name",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        )
    )

    # FIXME: this should fail:
    # see https://git.picodata.io/picodata/picodata/picodata/-/issues/331
    # Called with same name/id but different format -> error.
    cluster.create_table(
        dict(
            id=1026,
            name="some_name",
            format=[
                dict(name="key", type="string", is_nullable=False),
                dict(name="value", type="any", is_nullable=False),
            ],
            primary_key=["key"],
            distribution="global",
        )
    )

    # No such field for primary key -> error.
    with pytest.raises(ReturnError, match="no field with name: not_defined"):
        cluster.create_table(
            dict(
                id=1027,
                name="different_name",
                format=[dict(name="id", type="unsigned", is_nullable=False)],
                primary_key=["not_defined"],
                distribution="global",
            )
        )

    # Automatic space id
    cluster.create_table(
        dict(
            name="space 2",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
        )
    )
    space_id = 1027
    initiator_id = PICO_SERVICE_ID
    pico_space_def = [
        space_id,
        "space 2",
        {"Global": None},
        [{"field_type": "unsigned", "is_nullable": False, "name": "id"}],
        2,
        True,
        "memtx",
        initiator_id,
        "",
    ]
    assert i1.call("box.space._pico_table:get", space_id) == pico_space_def
    assert i2.call("box.space._pico_table:get", space_id) == pico_space_def

    # Another one
    cluster.create_table(
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

    # test vinyl space can be created
    space_id = 1029
    pico_space_def = [
        space_id,
        "stuffy",
        {"ShardedImplicitly": [["foo"], "murmur3", "default"]},
        [
            {"field_type": "unsigned", "is_nullable": False, "name": "id"},
            {"field_type": "unsigned", "is_nullable": False, "name": "bucket_id"},
            {"field_type": "integer", "is_nullable": False, "name": "foo"},
        ],
        4,
        True,
        "vinyl",
        initiator_id,
        "",
    ]
    cluster.create_table(
        dict(
            id=space_id,
            name="stuffy",
            format=[
                dict(name="id", type="unsigned", is_nullable=False),
                dict(name="foo", type="integer", is_nullable=False),
            ],
            primary_key=["id"],
            distribution="sharded",
            sharding_key=["foo"],
            sharding_fn="murmur3",
            engine="vinyl",
        ),
    )
    assert i1.call("box.space._pico_table:get", space_id) == pico_space_def
    assert i1.eval("return box.space.stuffy.engine") == "vinyl"
    assert i2.eval("return box.space.stuffy.engine") == "vinyl"

    #
    # pico.drop_table
    #

    # No such space name -> ok.
    cluster.drop_table("Space does not exist")

    # No such space id -> ok.
    cluster.drop_table(69105)

    # Ok by name.
    cluster.drop_table("some_name")
    for i in cluster.instances:
        assert (
            i.call("box.space._pico_table.index._pico_table_name:get", "some_name")
            is None
        )

    # Ok by id.
    cluster.drop_table(space_id)
    for i in cluster.instances:
        assert i.call("box.space._pico_table:get", space_id) is None

    #
    # Options validation
    #

    # Options is not table -> error.
    with pytest.raises(ReturnError, match="options should be a table"):
        i1.call("pico.drop_table", "some_name", "timeout after 3 seconds please")

    # Unknown option -> error.
    with pytest.raises(ReturnError, match="unexpected option 'deadline'"):
        i1.call("pico.drop_table", "some_name", dict(deadline="June 7th"))

    # Unknown option -> error.
    with pytest.raises(
        ReturnError, match="options parameter 'timeout' should be of type number"
    ):
        i1.call("pico.drop_table", "some_name", dict(timeout="3s"))


################################################################################
def test_ddl_create_table_bulky(cluster: Cluster):
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

    cluster.create_table(
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
    initiator_id = PICO_SERVICE_ID
    pico_space_def = [
        space_id,
        "stuff",
        {"Global": None},
        [{"field_type": "unsigned", "is_nullable": False, "name": "id"}],
        2,
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
        2,
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

    i5 = cluster.add_instance(wait_online=True, replicaset_id="r3")

    assert i5.call("box.space._pico_property:get", "global_schema_version")[1] == 2
    assert i5.next_schema_version() == 3
    assert i5.call("box.space._pico_table:get", space_id) == pico_space_def
    assert i5.call("box.space._pico_index:get", [space_id, 0]) == pico_pk_def
    assert i5.call("box.space._space:get", space_id) == tt_space_def
    assert i5.call("box.space._index:get", [space_id, 0]) == tt_pk_def

    i6 = cluster.add_instance(wait_online=True, replicaset_id="r3")

    # It's schema was updated automatically as well
    assert i6.call("box.space._pico_property:get", "global_schema_version")[1] == 2
    assert i6.next_schema_version() == 3
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
    space_id = 679
    cluster.create_table(
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
    pico_bucket_id_def = [
        space_id,
        1,
        "stuff_bucket_id",
        "tree",
        [dict(unique=False)],
        [["bucket_id", "unsigned", None, False, None]],
        True,
        schema_version,
    ]
    assert i1.call("box.space._pico_index:get", [space_id, 1]) == pico_bucket_id_def
    assert i2.call("box.space._pico_index:get", [space_id, 1]) == pico_bucket_id_def

    tt_bucket_id_def = [
        space_id,
        1,
        "stuff_bucket_id",
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
    with pytest.raises(ReturnError, match="timeout"):
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

    # The schema change finalized.
    for i in cluster.instances:
        assert i.call("box.space._space:get", space_id) is not None
        assert i.eval("return box.space._pico_table:get(...).operable", space_id)


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
    assert get_index_names(i1, space_id) == [
        f"{space_name}_pkey",
        f"{space_name}_bucket_id",
    ]
    assert i2.call("box.space._space:get", space_id) is not None
    assert get_index_names(i2, space_id) == [
        f"{space_name}_pkey",
        f"{space_name}_bucket_id",
    ]

    # Wake the instance so that governor finds out there's a conflict
    # and aborts the ddl op.
    i3.start()
    i3.wait_online()

    # Everything was cleaned up.
    assert i1.call("box.space._space:get", space_id) is None
    assert i2.call("box.space._space:get", space_id) is None
    assert i3.call("box.space._space:get", space_id) is None

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
    with pytest.raises(ReturnError, match="timeout"):
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
    index = i1.create_table(space_def)

    i2.raft_wait_index(index, 3)

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
    space_id = 632
    cluster.create_table(
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
        "stuff_bucket_id",
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


################################################################################
def test_ddl_create_table_from_snapshot_at_catchup(cluster: Cluster):
    # Second instance is only for quorum
    i1 = cluster.add_instance(wait_online=True, replicaset_id="r1")
    i2 = cluster.add_instance(wait_online=True, replicaset_id="R2")
    i3 = cluster.add_instance(wait_online=True, replicaset_id="R2")

    i1.assert_raft_status("Leader")

    i3.terminate()

    # TODO: check other ddl operations
    # Propose a space creation which will succeed
    space_id = 649
    index = i1.create_table(
        dict(
            id=space_id,
            name="stuff",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="global",
            engine="memtx",
        ),
    )
    i1.raft_wait_index(index)
    i2.raft_wait_index(index)

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
    assert i3.call("box.space._schema:get", "local_schema_version")[1] == 1


################################################################################
def test_ddl_create_table_at_catchup_with_master_switchover(cluster: Cluster):
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
                dict(name="value", type="any", is_nullable=False),
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
    i4 = cluster.add_instance(wait_online=True, replicaset_id="R99")
    i5 = cluster.add_instance(wait_online=True, replicaset_id="R99")

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
    with pytest.raises(ReturnError, match="timeout"):
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
    i2 = cluster.add_instance(wait_online=True, replicaset_id="r99")
    # This one will be catching up.
    i3 = cluster.add_instance(wait_online=True, replicaset_id="r99")

    # Set up.
    cluster.create_table(
        dict(
            name="replace_me",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="sharded",
            sharding_key=["id"],
            sharding_fn="murmur3",
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
            sharding_fn="murmur3",
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
    i3 = cluster.add_instance(wait_online=False, replicaset_id="r99")
    i4 = cluster.add_instance(wait_online=False, replicaset_id="r99")
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
    i2 = cluster.add_instance(wait_online=True, replicaset_id="r99")
    # This one will be catching up.
    i3 = cluster.add_instance(wait_online=True, replicaset_id="r99")

    # Set up.
    cluster.create_table(
        dict(
            name="replace_me",
            format=[dict(name="id", type="unsigned", is_nullable=False)],
            primary_key=["id"],
            distribution="sharded",
            sharding_key=["id"],
            sharding_fn="murmur3",
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
            sharding_fn="murmur3",
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
    i3 = cluster.add_instance(wait_online=True, replicaset_id="r99")
    # This is a replica, who will become master and be catching up.
    i4 = cluster.add_instance(wait_online=True, replicaset_id="r99")

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
            sharding_fn="murmur3",
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
    i1 = cluster.add_instance(wait_online=True, replicaset_id="R1")
    i2 = cluster.add_instance(wait_online=True, replicaset_id="R1")
    i3 = cluster.add_instance(wait_online=True, replicaset_id="R1")
    i4 = cluster.add_instance(wait_online=True, replicaset_id="R2")
    i5 = cluster.add_instance(wait_online=True, replicaset_id="R2")

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
