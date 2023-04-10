from conftest import Instance


def get_raft_state(i: Instance, k: str):
    return i.eval("return pico.space.raft_state:get(...).value", k)


def test_compaction(instance: Instance):
    read_index = instance.call("pico.raft_read_index", 3)
    applied_index = get_raft_state(instance, "applied")
    assert read_index == applied_index

    assert instance.call("pico.raft_compact_log", 1) == 0

    # Trim first entry
    assert instance.call("pico.raft_compact_log", 2) == 1
    assert get_raft_state(instance, "compacted_index") == 1

    # Compact everything
    assert instance.call("pico.raft_compact_log", 2**64-1) == applied_index - 1
    assert get_raft_state(instance, "compacted_index") == applied_index

    # Check idempotency
    assert instance.call("pico.raft_compact_log", 2) == 0
