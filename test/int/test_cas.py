import pytest
from conftest import (
    Cluster,
    Instance,
    TarantoolError,
    ReturnError,
    CasRange,
    ErrorCode,
    log_crawler,
)

_3_SEC = 3


def test_cas_errors(instance: Instance):
    index, term = instance.eval(
        """
        local index = box.space._raft_state:get("applied").value
        local term = box.space._raft_state:get("term").value
        return {index, term}
        """
    )

    # Bad requested_term == current_term + 1
    with pytest.raises(TarantoolError) as e1:
        instance.cas(
            "insert",
            "_pico_property",
            ["foo", "420"],
            index=index,
            term=term + 1,
        )
    assert e1.value.args[:2] == (
        ErrorCode.TermMismatch,
        "operation request from different term 3, current term is 2",
    )

    # Bad requested_term == current_term - 1
    with pytest.raises(TarantoolError) as e2:
        instance.cas(
            "insert",
            "_pico_property",
            ["foo", "420"],
            index=index,
            term=term - 1,
        )
    assert e2.value.args[:2] == (
        ErrorCode.TermMismatch,
        "operation request from different term 1, current term is 2",
    )

    # Wrong term for existing index
    with pytest.raises(TarantoolError) as e3:
        instance.cas(
            "insert",
            "_pico_property",
            ["foo", "420"],
            index=1,
            term=2,  # actually 1
        )
    assert e3.value.args[:2] == (
        ErrorCode.CasEntryTermMismatch,
        "EntryTermMismatch: entry at index 1 has term 1, request implies term 2",
    )

    # Wrong index (too big)
    with pytest.raises(TarantoolError) as e4:
        instance.cas(
            "insert",
            "_pico_property",
            ["foo", "420"],
            index=2048,
        )
    assert e4.value.args[:2] == (
        ErrorCode.CasNoSuchRaftIndex,
        f"NoSuchIndex: raft entry at index 2048 does not exist yet, the last is {index}",
    )

    # Compact the whole raft log
    assert instance.raft_compact_log() == index + 1

    # Wrong index (compacted)
    with pytest.raises(TarantoolError) as e5:
        instance.cas("insert", "_pico_property", ["foo", "420"], index=index - 1)
    assert e5.value.args[:2] == (
        ErrorCode.RaftLogCompacted,
        f"Compacted: raft index {index - 1} is compacted at {index}",
    )

    # Prohibited tables for all users, even for admin
    for table in ["_pico_table", "_pico_index"]:
        with pytest.raises(TarantoolError) as e5:
            instance.cas("insert", table, [0], ranges=[CasRange(eq=0)], user=1)
        assert e5.value.args[:2] == (
            ErrorCode.CasTableNotAllowed,
            f"TableNotAllowed: table {table} cannot be modified by DML Raft Operation directly",
        )

    # Config prohibited for modification
    with pytest.raises(TarantoolError) as e5:
        instance.cas("insert", "_pico_db_config", ["shredding", "", True], ranges=[CasRange(eq=0)], user=1)
    assert e5.value.args[:2] == (
        ErrorCode.CasConfigNotAllowed,
        "ConfigNotAllowed: config shredding cannot be modified",
    )

    # Field type error
    with pytest.raises(TarantoolError) as error:
        instance.cas(
            "insert",
            "_pico_property",
            [1, 2, 3],
        )
    assert error.value.args[:2] == (
        # This is sad, the type is now checked when checking the implicit
        # predicate which results in a stupid error message...
        "ER_KEY_PART_TYPE",
        "Supplied key type of part 0 does not match index part type: expected string",  # noqa: E501
    )

    # Delete of undeletable property
    with pytest.raises(TarantoolError) as error:
        instance.cas(
            "delete",
            "_pico_property",
            key=["next_schema_version"],
        )
    assert error.value.args[:2] == (
        "ER_PROC_LUA",
        "property next_schema_version cannot be deleted",  # noqa: E501
    )

    # Incorrect type of builtin property
    with pytest.raises(TarantoolError, match="incorrect type of property global_schema_version"):
        instance.cas(
            "replace",
            "_pico_property",
            ["global_schema_version", "this is not a version number"],
        )

    # Too many values for builtin property
    with pytest.raises(TarantoolError) as error:
        instance.cas(
            "replace",
            "_pico_property",
            ["global_schema_version", 13, 37],
        )
    assert error.value.args[:2] == (
        "ER_PROC_LUA",
        "too many fields: got 3, expected 2",
    )

    # Not enough values for builtin property
    with pytest.raises(TarantoolError) as error:
        instance.cas(
            "replace",
            "_pico_property",
            ["auto_offline_timeout"],
        )
    assert error.value.args[:2] == (
        "ER_INDEX_FIELD_COUNT",
        "Tuple field 2 (value) required by space format is missing",  # noqa: E501
    )

    # Resulting raft log entry is too big
    with pytest.raises(TarantoolError) as error:
        # NOTE: this size is carefully chosen so that the inserted tuple doesn't
        # exceed the threshold, but the raft log tuple (which also contains some
        # additional metadata) does exceed the limit.
        size = 1024 * 1024 - 18
        instance.cas(
            "insert",
            "_pico_property",
            ["X", "X" * size],
        )
    assert error.value.args[:2] == (
        "ER_SLAB_ALLOC_MAX",
        "tuple size 1048600 exceeds the allowed limit",  # noqa: E501
    )


def test_cas_predicate(instance: Instance):
    instance.raft_compact_log()
    read_index = instance.raft_read_index(_3_SEC)

    # Successful insert
    ret, res_row_cnt = instance.cas("insert", "_pico_property", ["fruit", "apple"], index=read_index)
    assert ret == read_index + 1
    assert res_row_cnt == 1
    instance.raft_wait_index(ret, _3_SEC)
    assert instance.raft_read_index(_3_SEC) == ret
    assert instance.pico_property("fruit") == "apple"

    # CaS rejected via the implicit predicate
    with pytest.raises(TarantoolError) as e1:
        instance.cas("insert", "_pico_property", ["fruit", "orange"], index=read_index)
    assert e1.value.args[:2] == (
        ErrorCode.CasConflictFound,
        f"ConflictFound: found a conflicting entry at index {read_index + 1}",
    )

    # CaS rejected via the implicit predicate, even though there's an explicit one
    with pytest.raises(TarantoolError) as e2:
        instance.cas(
            "insert",
            "_pico_property",
            ["fruit", "orange"],
            index=read_index,
            ranges=[CasRange(eq="vegetable")],
        )
    assert e2.value.args[:2] == (
        ErrorCode.CasConflictFound,
        f"ConflictFound: found a conflicting entry at index {read_index + 1}",
    )

    # CaS rejected via the implicit predicate, different kind of operation
    with pytest.raises(TarantoolError) as e3:
        instance.cas("replace", "_pico_property", ["fruit", "orange"], index=read_index)
    assert e3.value.args[:2] == (
        ErrorCode.CasConflictFound,
        f"ConflictFound: found a conflicting entry at index {read_index + 1}",
    )

    # CaS rejected via the implicit predicate, different kind of operation
    with pytest.raises(TarantoolError) as e4:
        instance.cas("delete", "_pico_property", key=["fruit"], index=read_index)
    assert e4.value.args[:2] == (
        ErrorCode.CasConflictFound,
        f"ConflictFound: found a conflicting entry at index {read_index + 1}",
    )

    # CaS rejected via the implicit predicate, different kind of operation
    with pytest.raises(TarantoolError) as e5:
        instance.cas(
            "update",
            "_pico_property",
            key=["fruit"],
            ops=[("+", "value", 1)],
            index=read_index,
        )
    assert e5.value.args[:2] == (
        ErrorCode.CasConflictFound,
        f"ConflictFound: found a conflicting entry at index {read_index + 1}",
    )

    # CaS rejected via the explicit predicate, even though implicit range doesn't match
    with pytest.raises(TarantoolError) as e6:
        instance.cas(
            "insert",
            "_pico_property",
            ["animal", "chicken"],
            index=read_index,
            ranges=[CasRange(eq="fruit")],
        )
    assert e6.value.args[:2] == (
        ErrorCode.CasConflictFound,
        f"ConflictFound: found a conflicting entry at index {read_index + 1}",
    )

    # Stale index, yet successful insert of another key
    ret, res_row_cnt = instance.cas(
        "insert",
        "_pico_property",
        ["flower", "tulip"],
        index=read_index,
    )
    assert ret == read_index + 2
    assert res_row_cnt == 1
    instance.raft_wait_index(ret, _3_SEC)
    assert instance.raft_read_index(_3_SEC) == ret
    assert instance.pico_property("flower") == "tulip"


def test_cas_batch(cluster: Cluster):
    def value(s: str, k: str):
        return cluster.instances[0].eval(
            f"""
            local tuple = box.space.{s}:get(...)
            return tuple and tuple.value
            """,
            k,
        )

    cluster.deploy(instance_count=2)

    cluster.create_table(
        dict(
            id=1026,
            name="some_space",
            format=[
                dict(name="key", type="string", is_nullable=False),
                dict(name="value", type="string", is_nullable=False),
            ],
            primary_key=["key"],
            distribution="global",
        )
    )
    i1 = cluster.instances[0]
    read_index = i1.raft_read_index(_3_SEC)

    index1, _, res_row_count1 = cluster.batch_cas(
        [
            dict(
                table="some_space",
                kind="insert",
                tuple=["car", "bike"],
            ),
            dict(
                table="some_space",
                kind="insert",
                tuple=["tree", "pine"],
            ),
            dict(
                table="some_space",
                kind="insert",
                tuple=["stone", "diamond"],
            ),
            dict(
                table="_pico_property",
                kind="replace",
                tuple=["raft_entry_max_size", 50],
            ),
        ]
    )
    assert index1 == read_index + 1
    assert res_row_count1 == 4
    cluster.raft_wait_index(index1, _3_SEC)
    assert cluster.instances[0].raft_read_index(_3_SEC) == index1
    assert value("some_space", "car") == "bike"
    assert value("some_space", "tree") == "pine"
    assert value("some_space", "stone") == "diamond"
    assert value("_pico_property", "raft_entry_max_size") == 50

    index, _, res_row_count = cluster.batch_cas(
        [
            dict(
                table="some_space",
                kind="delete",
                key=["car"],
            ),
            dict(
                table="some_space",
                kind="delete",
                key=["tree"],
            ),
            dict(
                table="some_space",
                kind="delete",
                key=["stone"],
            ),
            dict(
                table="_pico_property",
                kind="update",
                key=["raft_entry_max_size"],
                ops=[["+", 1, 100]],
            ),
        ]
    )
    assert index > read_index + 1
    assert res_row_count == 1
    cluster.raft_wait_index(index, _3_SEC)
    assert cluster.instances[0].raft_read_index(_3_SEC) == index
    assert cluster.instances[0].eval('return box.execute([[select * from "some_space"]]).rows') == []
    assert value("_pico_property", "raft_entry_max_size") == 150

    with pytest.raises(ReturnError) as err:
        cluster.batch_cas(
            [
                dict(
                    table="some_space",
                    kind="insert",
                    tuple=["beverage", "wine"],
                ),
                dict(
                    table="some_space",
                    kind="delete",
                    key=["car"],
                ),
                dict(
                    table="some_space",
                    kind="insert",
                    tuple=["sport", "run"],
                ),
            ],
            index=index1,
            ranges=[CasRange(table="some_space", eq="car")],
        )
    assert err.value.args[:2] == (f"ConflictFound: found a conflicting entry at index {index1 + 1}",)


# Previous tests use stored procedure `.proc_cas_v2`, this one uses `pico.cas` lua api instead
def test_cas_lua_api(cluster: Cluster):
    def value(k: str):
        return cluster.instances[0].eval(
            """
            local tuple = box.space.some_space:get(...)
            return tuple and tuple.value
            """,
            k,
        )

    cluster.deploy(instance_count=3)

    # We cannot use `_pico_property` here as it does not have
    # a corresponding entry in `Spaces`
    cluster.create_table(
        dict(
            id=1026,
            name="some_space",
            format=[
                dict(name="key", type="string", is_nullable=False),
                dict(name="value", type="string", is_nullable=False),
            ],
            primary_key=["key"],
            distribution="global",
        )
    )

    read_index = cluster.instances[0].raft_read_index(_3_SEC)

    # Successful insert
    ret, res_row_cnt = cluster.cas("insert", "some_space", ["fruit", "apple"], index=read_index)
    assert ret == read_index + 1
    assert res_row_cnt == 1
    cluster.raft_wait_index(ret, _3_SEC)
    assert cluster.instances[0].raft_read_index(_3_SEC) == ret
    assert value("fruit") == "apple"

    # CaS rejected
    with pytest.raises(TarantoolError) as e:
        cluster.cas(
            "insert",
            "some_space",
            ["fruit", "orange"],
            index=read_index,
            ranges=[CasRange(eq="fruit")],
        )
    assert e.value.args[:2] == (
        ErrorCode.CasConflictFound,
        f"ConflictFound: found a conflicting entry at index {read_index + 1}",
    )


def test_cas_operable_table(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False, init_replication_factor=1)

    error_injection = "BLOCK_GOVERNOR_BEFORE_DDL_COMMIT"
    injection_log = f"ERROR INJECTION '{error_injection}'"
    lc = log_crawler(i1, injection_log)

    i1.env[f"PICODATA_ERROR_INJECTION_{error_injection}"] = "1"
    i1.start()
    i1.wait_online()

    with pytest.raises(TimeoutError):
        i1.sql("CREATE TABLE warehouse (id INTEGER PRIMARY KEY) DISTRIBUTED GLOBALLY;")
    lc.wait_matched()

    with pytest.raises(TarantoolError) as e1:
        i1.cas(
            "insert",
            "warehouse",
            ["1"],
        )
    assert e1.value.args[:2] == (
        ErrorCode.CasTableNotOperable,
        "TableNotOperable: " + "table warehouse cannot be modified now as DDL operation is in progress",
    )


def test_cas_raft_proposal_drop(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False, init_replication_factor=1)
    i1.start()
    i1.wait_online()
    index = i1.eval(
        """
        local index = box.space._raft_state:get("applied").value
        return index
        """
    )
    error_injection = "RAFT_PROPOSAL_DROPPED"
    i1.call("pico._inject_error", error_injection, True)

    with pytest.raises(TarantoolError) as err:
        i1.cas("insert", "_pico_property", ["foo", "420"], index=index)
    assert err.value.args[:2] == (
        ErrorCode.RaftProposalDropped,
        "raft: proposal dropped",
    )
