from conftest import Instance, Retriable, Cluster
import time


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
    index, _ = i1.cas("insert", "_pico_property", ["dont", "compact", "me"])
    raft_wait_commit_index(i1, index)

    assert i1.call("box.space._pico_property:get", "dont") is None

    first = i1.raft_compact_log()
    assert first == index

    i1.call("pico._inject_error", "BLOCK_AFTER_RAFT_PERSISTS_COMMIT_INDEX", False)
    i1.raft_wait_index(index)
    assert i1.call("box.space._pico_property:get", "dont") == ["dont", "compact", "me"]


def test_raft_log_auto_compaction_basics(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)

    def get_raft_log_count_and_size(instance: Instance) -> tuple[int, int]:
        [count, size] = instance.eval("return { box.space._raft_log:len(), box.space._raft_log:bsize() }")
        return count, size

    count, size = get_raft_log_count_and_size(i1)
    assert count > 0
    assert size > 0

    # Set the maximum raft log size to the current size. This will trigger the
    # compaction because a new entry will be added to the log which will
    # increase it's size past the newly introduced limit.
    max_size = size
    i1.sql(f"ALTER SYSTEM SET raft_wal_size_max = {max_size}")

    count, size = get_raft_log_count_and_size(i1)
    assert count == 0
    assert size == 0

    # Create a global table which will help us generate raft log entries
    i1.sql("CREATE TABLE trash (id INT PRIMARY KEY, name STRING) DISTRIBUTED GLOBALLY")

    id = 1

    def bulk_insert_until_compaction():
        nonlocal id, count, size

        start = time.time()

        prev_count, prev_size = count, size
        batch_size = 10
        name_length = 100
        while True:
            args = []
            for _ in range(batch_size):
                name = "X" * name_length
                args.append(id)
                args.append(name)
                id += 1
            placeholders = str.join(", ", ["(?, ?)"] * batch_size)

            i1.sql(f"INSERT INTO trash VALUES {placeholders}", *args)
            id += 1

            count, size = get_raft_log_count_and_size(i1)
            if count == 0:
                assert size == 0
                break
            else:
                assert size <= max_size

            prev_count, prev_size = count, size

            assert time.time() - start < 10, "just a safeguard in case of error"

        print(
            f"\x1b[36mDONE IN {time.time() - start} seconds, {id=}, {prev_count=}, {prev_size=}\x1b[0m"  # noqa: E501
        )

        return prev_count

    # Add more entries to the raft log until the log compacts again
    bulk_insert_until_compaction()

    # Make sure something was actually added to the log before it got compacted
    assert id > 1

    # Set the maximum raft log entry count.
    max_count = 2
    i1.sql(f"ALTER SYSTEM SET raft_wal_count_max = {max_count}")

    # Alter system statement results in one raft log entry
    count, size = get_raft_log_count_and_size(i1)
    assert count == 1 and count < max_count
    assert size > 0 and size < max_size

    # Another raft log entry, no compaction yet
    i1.sql("INSERT INTO trash VALUES (?, ?)", id, "no compaction just yet")
    id += 1
    count, size = get_raft_log_count_and_size(i1)
    assert count == 2 and count == max_count
    assert size > 0

    # This entry triggers compaction
    i1.sql("INSERT INTO trash VALUES (?, ?)", id, "compaction after this one")
    id += 1
    count, size = get_raft_log_count_and_size(i1)
    assert count == 0
    assert size == 0

    # Increase the maximum raft log entry count.
    max_count = 5
    i1.sql(f"ALTER SYSTEM SET raft_wal_count_max = {max_count}")

    count, size = get_raft_log_count_and_size(i1)
    assert count == 1
    assert size > 0 and size < max_size

    # Insert more entries without triggering compaction
    prev_count = count
    while count < max_count:
        name = str(id) * id
        i1.sql("INSERT INTO trash VALUES (?, ?)", id, name)
        id += 1

        count, size = get_raft_log_count_and_size(i1)
        assert count == prev_count + 1
        assert size > 0 and size < max_size

        prev_count = count

    i1.sql("INSERT INTO trash VALUES (?, ?)", id, "compaction yet again")
    id += 1

    # This last insert triggers compaction
    count, size = get_raft_log_count_and_size(i1)
    assert count == 0
    assert size == 0

    # Disable limit on number of tuples all together
    old_max_count = max_count
    i1.sql("ALTER SYSTEM SET raft_wal_count_max = 9999")

    # Insert more stuff until log compacts based on number of bytes
    prev_count = bulk_insert_until_compaction()
    assert prev_count > old_max_count


def test_raft_log_auto_compaction_preserves_finalizers(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)

    # Compact the log and make it so compaction is triggerred the moment a
    # DdlCommit entry is added to the log
    i1.sql("ALTER SYSTEM SET raft_wal_count_max = 1")

    assert i1.call("box.space._raft_log:len") == 0

    index_before = i1.raft_get_index()

    # After this 2 entries will be added to the log, tirggering the compaction,
    # but the last entry is going to be kept, because it's a DDL finalizer
    i1.sql("CREATE TABLE trash (id INT PRIMARY KEY, name STRING) DISTRIBUTED GLOBALLY")

    assert i1.raft_get_index() == index_before + 2
    assert i1.call("box.space._raft_log:len") == 1

    # But next compaction removes the finalizer, because we assume every client
    # had enough time to read it...
    i1.sql("INSERT INTO trash VALUES (1, 'foo')")

    assert i1.call("box.space._raft_log:len") == 0
