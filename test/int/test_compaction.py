from conftest import Instance


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
