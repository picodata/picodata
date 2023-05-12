import pytest
from conftest import Instance, TarantoolError

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
        "operation request from different term 1, current term is 2",
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
        "compare-and-swap request failed: "
        + f"raft entry at index 2048 does not exist yet, the last is {index}",
    )

    # Compact the whole raft log
    assert instance.raft_compact_log() == index + 1

    # Wrong index (compacted)
    with pytest.raises(TarantoolError) as e5:
        instance.cas("insert", "_pico_property", ["foo", "420"], index=index - 1)
    assert e5.value.args == (
        "ER_PROC_C",
        "compare-and-swap request failed: "
        + f"raft index {index-1} is compacted at {index}",
    )

    # Prohibited spaces
    for space in ["_pico_space", "_pico_index"]:
        with pytest.raises(TarantoolError) as e5:
            instance.cas(
                "insert",
                space,
                [0],
                range=(
                    dict(kind="included", value=0),
                    dict(kind="included", value=0),
                ),
            )
        assert e5.value.args == (
            "ER_PROC_C",
            f"compare-and-swap request failed: space {space} is prohibited for use "
            + "in a predicate",
        )


def test_cas_predicate(instance: Instance):
    def property(k: str):
        return instance.eval(
            """
            local tuple = box.space._pico_property:get(...)
            return tuple and tuple.value
            """,
            k,
        )

    instance.raft_compact_log()
    read_index = instance.call("pico.raft_read_index", _3_SEC)

    # Successful insert
    ret = instance.cas("insert", "_pico_property", ["fruit", "apple"], read_index)
    assert ret == read_index + 1
    instance.call(".proc_sync_raft", ret, (_3_SEC, 0))
    assert instance.call("pico.raft_read_index", _3_SEC) == ret
    assert property("fruit") == "apple"

    # CaS rejected
    with pytest.raises(TarantoolError) as e5:
        instance.cas(
            "insert",
            "_pico_property",
            ["fruit", "orange"],
            index=read_index,
            range=(
                dict(kind="included", value="fruit"),
                dict(kind="included", value="fruit"),
            ),
        )
    assert e5.value.args == (
        "ER_PROC_C",
        "compare-and-swap request failed: "
        + f"comparison failed for index {read_index} "
        + f"as it conflicts with {read_index+1}",
    )

    # Stale index, yet successful insert of another key
    ret = instance.cas(
        "insert",
        "_pico_property",
        ["flower", "tulip"],
        index=read_index,
        range=(
            dict(kind="included", value="flower"),
            dict(kind="included", value="flower"),
        ),
    )
    assert ret == read_index + 2
    instance.call(".proc_sync_raft", ret, (_3_SEC, 0))
    assert instance.call("pico.raft_read_index", _3_SEC) == ret
    assert property("flower") == "tulip"
