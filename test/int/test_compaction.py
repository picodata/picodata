from conftest import Instance, Retriable


def test_compaction(instance: Instance):
    def raft_state(k: str):
        return instance.eval(
            """
            local tuple = box.space._raft_state:get(...)
            return tuple and tuple.value
            """,
            k,
        )

    read_index = instance.raft_read_index(3)
    applied_index = raft_state("applied")
    assert read_index == applied_index

    assert instance.raft_compact_log(1) == 1
    assert instance.raft_first_index() == 1
    assert raft_state("compacted_index") is None
    assert raft_state("compacted_term") is None

    # Trim first entry
    assert instance.raft_compact_log(2) == 2
    assert instance.raft_first_index() == 2
    assert raft_state("compacted_index") == 1

    # Compact everything
    assert instance.raft_compact_log(2**64 - 1) == applied_index + 1
    assert instance.raft_first_index() == applied_index + 1
    assert raft_state("compacted_index") == applied_index

    # Check idempotency
    assert instance.raft_compact_log(2) == applied_index + 1
    assert raft_state("compacted_index") == applied_index


def raft_wait_commit_index(i: Instance, expected, timeout=1):
    def make_attempt():
        commit = i.eval("return box.space._raft_state:get('commit').value")
        assert commit == expected

    Retriable(timeout=timeout, rps=10).call(make_attempt)


def test_unapplied_entries_arent_compacted(instance: Instance):
    i1 = instance

    i1.call("pico._inject_error", "BLOCK_AFTER_RAFT_PERSISTS_COMMIT_INDEX", True)

    # Propose a raft log entry and wait for it to be committed, but not applied.
    index = i1.cas("insert", "_pico_property", ["dont", "compact", "me"])
    raft_wait_commit_index(i1, index)

    assert i1.call("box.space._pico_property:get", "dont") is None

    first = i1.raft_compact_log()
    assert first == index

    i1.call("pico._inject_error", "BLOCK_AFTER_RAFT_PERSISTS_COMMIT_INDEX", False)
    i1.raft_wait_index(index)
    assert i1.call("box.space._pico_property:get", "dont") == ["dont", "compact", "me"]
