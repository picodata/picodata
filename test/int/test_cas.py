import pytest
from conftest import Cluster, Instance, TarantoolError, ReturnError, CasRange

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
    assert e1.value.args == (
        "ER_PROC_C",
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
    assert e2.value.args == (
        "ER_PROC_C",
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
    assert e3.value.args == (
        "ER_PROC_C",
        "compare-and-swap: EntryTermMismatch: entry at index 1 has term 1, request implies term 2",
    )

    # Wrong index (too big)
    with pytest.raises(TarantoolError) as e4:
        instance.cas(
            "insert",
            "_pico_property",
            ["foo", "420"],
            index=2048,
        )
    assert e4.value.args == (
        "ER_PROC_C",
        "compare-and-swap: NoSuchIndex: "
        + f"raft entry at index 2048 does not exist yet, the last is {index}",
    )

    # Compact the whole raft log
    assert instance.raft_compact_log() == index + 1

    # Wrong index (compacted)
    with pytest.raises(TarantoolError) as e5:
        instance.cas("insert", "_pico_property", ["foo", "420"], index=index - 1)
    assert e5.value.args == (
        "ER_PROC_C",
        "compare-and-swap: Compacted: "
        + f"raft index {index-1} is compacted at {index}",
    )

    # Prohibited spaces
    for space in ["_pico_table", "_pico_index"]:
        with pytest.raises(TarantoolError) as e5:
            instance.cas(
                "insert",
                space,
                [0],
                ranges=[CasRange(eq=0)],
            )
        assert e5.value.args == (
            "ER_PROC_C",
            f"compare-and-swap: SpaceNotAllowed: space {space} is prohibited for use "
            + "in a predicate",
        )

    # Field type error
    with pytest.raises(TarantoolError) as error:
        instance.cas(
            "insert",
            "_pico_property",
            [1, 2, 3],
        )
    assert error.value.args == (
        "ER_PROC_C",
        "box error: FieldType: Tuple field 1 (key) type does not match one required by operation: expected string, got unsigned",  # noqa: E501
    )

    # Delete of undeletable property
    with pytest.raises(TarantoolError) as error:
        instance.cas(
            "delete",
            "_pico_property",
            ["next_schema_version"],
        )
    assert error.value.args == (
        "ER_PROC_C",
        "box error: ProcLua: property next_schema_version cannot be deleted",  # noqa: E501
    )

    # Incorrect type of builtin property
    with pytest.raises(TarantoolError) as error:
        instance.cas(
            "replace",
            "_pico_property",
            ["global_schema_version", "this is not a version number"],
        )
    assert error.value.args == (
        "ER_PROC_C",
        """box error: ProcLua: incorrect type of property global_schema_version: invalid type: string "this is not a version number", expected u64""",  # noqa: E501
    )

    # Too many values for builtin property
    with pytest.raises(TarantoolError) as error:
        instance.cas(
            "replace",
            "_pico_property",
            ["password_min_length", 13, 37],
        )
    assert error.value.args == (
        "ER_PROC_C",
        "box error: ProcLua: too many fields: got 3, expected 2",
    )

    # Not enough values for builtin property
    with pytest.raises(TarantoolError) as error:
        instance.cas(
            "replace",
            "_pico_property",
            ["auto_offline_timeout"],
        )
    assert error.value.args == (
        "ER_PROC_C",
        "box error: FieldMissing: Tuple field 2 (value) required by space format is missing",  # noqa: E501
    )


def test_cas_predicate(instance: Instance):
    instance.raft_compact_log()
    read_index = instance.raft_read_index(_3_SEC)

    # Successful insert
    ret = instance.cas("insert", "_pico_property", ["fruit", "apple"], read_index)
    assert ret == read_index + 1
    instance.raft_wait_index(ret, _3_SEC)
    assert instance.raft_read_index(_3_SEC) == ret
    assert instance.pico_property("fruit") == "apple"

    # CaS rejected
    with pytest.raises(TarantoolError) as e5:
        instance.cas(
            "insert",
            "_pico_property",
            ["fruit", "orange"],
            index=read_index,
            ranges=[CasRange(eq="fruit")],
        )
    assert e5.value.args == (
        "ER_PROC_C",
        "compare-and-swap: ConflictFound: "
        + f"found a conflicting entry at index {read_index+1}",
    )

    # Stale index, yet successful insert of another key
    ret = instance.cas(
        "insert",
        "_pico_property",
        ["flower", "tulip"],
        index=read_index,
        ranges=[CasRange(eq="flower")],
    )
    assert ret == read_index + 2
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

    ret1 = cluster.batch_cas(
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
    assert ret1 == read_index + 1
    cluster.raft_wait_index(ret1, _3_SEC)
    assert cluster.instances[0].raft_read_index(_3_SEC) == ret1
    assert value("some_space", "car") == "bike"
    assert value("some_space", "tree") == "pine"
    assert value("some_space", "stone") == "diamond"
    assert value("_pico_property", "raft_entry_max_size") == 50

    ret = cluster.batch_cas(
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
    assert ret > read_index + 1
    cluster.raft_wait_index(ret, _3_SEC)
    assert cluster.instances[0].raft_read_index(_3_SEC) == ret
    assert (
        cluster.instances[0].eval(
            'return box.execute([[select * from "some_space"]]).rows'
        )
        == []
    )
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
            index=ret1,
            ranges=[CasRange(table="some_space", eq="car")],
        )
    assert err.value.args == (
        "compare-and-swap: ConflictFound: "
        + f"found a conflicting entry at index {ret1+1}",
    )


# Previous tests use stored procedure `.proc_cas`, this one uses `pico.cas` lua api instead
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
    ret = cluster.cas("insert", "some_space", ["fruit", "apple"], index=read_index)
    assert ret == read_index + 1
    cluster.raft_wait_index(ret, _3_SEC)
    assert cluster.instances[0].raft_read_index(_3_SEC) == ret
    assert value("fruit") == "apple"

    # CaS rejected
    with pytest.raises(ReturnError) as e5:
        cluster.cas(
            "insert",
            "some_space",
            ["fruit", "orange"],
            index=read_index,
            ranges=[CasRange(eq="fruit")],
        )
    assert e5.value.args == (
        "compare-and-swap: ConflictFound: "
        + f"found a conflicting entry at index {read_index+1}",
    )
    pass
