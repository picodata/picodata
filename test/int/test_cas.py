import msgpack  # type: ignore
import pytest
from conftest import Instance, ReturnError

_3_SEC = 3


def cas_property(
    i: Instance, op: dict, key: str, read_index: int | None, read_term: int | None
) -> int:
    return i.eval(
        """
        require('fiber').name('eval_cas')
        local log =  require('log')
        local op, key, read_index, read_term = ...
        if read_index == nil then
            read_index = pico.raft_read_index(3)
        end
        if read_term == nil then
            local entry = box.space._picodata_raft_log:get(read_index)
            if entry then
                read_term = entry.term
            else
                read_term = pico.raft_status().term
            end
        end

        local sp = pico.space.property
        local predicate = {
            index = read_index,
            term = read_term,
            ranges = {
                {
                    space = sp.name,
                    index = sp.index[0].name,
                    key_min = {key},
                    key_max = {key},
                }
            },
        }

        return pico.cas(sp.index[0], op, predicate)
    """,
        op,
        key,
        read_index,
        read_term,
    )


def cas_property_insert(
    i: Instance,
    key: str,
    value: str,
    read_index: int | None = None,
    read_term: int | None = None,
) -> int:
    op = dict(kind="insert", tuple=[key, value])
    return cas_property(i, op, key, read_index, read_term)


def cas_property_delete(
    i: Instance, key: str, read_index: int | None = None, read_term: int | None = None
) -> int:
    op = dict(kind="delete", key=[key])
    return cas_property(i, op, key, read_index, read_term)


def raft_log_dml_entry(i: Instance, index: int) -> tuple:
    entry = i.call("pico.space.raft_log:get", [index])
    ctx = entry[4][1]
    assert ctx["kind"] == "dml"
    (op, args), *_ = ((k, v) for k, v in ctx.items() if k != "kind")
    return op, args[0], msgpack.loads(args[1])


def efmt(e):
    return "Binary protocol (iproto) error: service responded with error: " + e


def test_errors(instance: Instance):
    index = instance.call("pico.raft_read_index", _3_SEC)
    term = instance.call("pico.raft_status")["term"]

    # Bad requested_term == current_term + 1
    with pytest.raises(ReturnError) as e1:
        cas_property_insert(instance, "foo", "420", index, term + 1)
    assert e1.value.args == (
        efmt("operation request from different term 3, current term is 2"),
    )

    # Bad requested_term == current_term - 1
    with pytest.raises(ReturnError) as e2:
        cas_property_insert(instance, "foo", "420", index, term - 1)
    assert e2.value.args == (
        efmt("operation request from different term 1, current term is 2"),
    )

    # Wrong index (existing)
    with pytest.raises(ReturnError) as e3:
        cas_property_insert(instance, "foo", "420", read_index=0)
    assert e3.value.args == (
        efmt("operation request from different term 1, current term is 2"),
    )

    # Wrong index (too big)
    with pytest.raises(ReturnError) as e4:
        cas_property_insert(instance, "foo", "420", read_index=2**64 - 1)
    assert e4.value.args == (
        efmt(
            "compare-and-swap request failed: "
            + f"raft entry at index {2**64-1} does not exist yet, the last is {index}"
        ),
    )

    # Trim raft log
    assert instance.call("pico.raft_compact_log", 2**64 - 1) == index + 1

    # Wrong index (compacted)
    with pytest.raises(ReturnError) as e5:
        cas_property_insert(instance, "foo", "420", index - 1)
    assert e5.value.args == (
        efmt(
            "compare-and-swap request failed: "
            + f"raft index {index-1} is compacted at {index}"
        ),
    )


def test_predicate(instance: Instance):
    instance.call("pico.raft_compact_log", 2**64 - 1)
    read_index = instance.call("pico.raft_read_index", _3_SEC)

    # Successful insert
    ret = cas_property_insert(instance, "fruit", "apple", read_index)
    assert ret == read_index + 1
    instance.call(".proc_sync_raft", ret, (_3_SEC, 0))

    assert instance.call("pico.raft_read_index", 3) == ret
    assert instance.call("pico.space.property:get", "fruit")[1] == "apple"

    # Wrong index (compacted)
    with pytest.raises(ReturnError) as e5:
        cas_property_insert(instance, "fruit", "orange", read_index)
    assert e5.value.args == (
        efmt(
            "compare-and-swap request failed: "
            + f"comparison failed for index {read_index} "
            + f"as it conflicts with {read_index+1}"
        ),
    )

    # Stale index, yet successful insert of another key
    ret = cas_property_insert(instance, "flower", "tulip", read_index)
    assert ret == read_index + 2
    instance.call(".proc_sync_raft", ret, (_3_SEC, 0))

    assert instance.call("pico.raft_read_index", 3) == ret
    assert instance.call("pico.space.property:get", "flower")[1] == "tulip"
