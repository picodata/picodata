from conftest import Cluster, Instance


def raft_compact_log_and_get_len(i: Instance):
    return i.eval(
        """
        local last_index = pico.space.raft_log.index[0]:max().index
        pico.raft_compact_log(last_index + 1)
        return pico.space.raft_log:len()
        """
    )


def get_raft_commit(i: Instance) -> int:
    return i.eval("return pico.space.raft_state:get('commit')")


def get_raft_log_len(i: Instance) -> int:
    return i.eval("return pico.space.raft_log:len()")


def test_reelection_after_snapshot(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)

    assert raft_compact_log_and_get_len(i1) == 0

    # Add another instance after log compaction
    i2 = cluster.add_instance()

    assert get_raft_commit(i1) == get_raft_commit(i2)
    # first 3 entries (replace peer_address, persist instance, raft conf change)
    # will be in the snapshot, and the rest will be arriving via append entries
    assert get_raft_log_len(i2) == get_raft_log_len(i1) - 3

    # Add another instance
    i3 = cluster.add_instance()

    assert get_raft_commit(i2) == get_raft_commit(i3)
    # the fresh snapshot is generated for each new instance
    assert get_raft_log_len(i3) != get_raft_log_len(i2)

    i1.terminate()

    assert raft_compact_log_and_get_len(i2) == 0
    assert raft_compact_log_and_get_len(i3) == 0

    # Add yet another instance after another log compaction
    i4 = cluster.add_instance(peers=[i2.listen])

    assert get_raft_commit(i3) == get_raft_commit(i4)
    assert get_raft_log_len(i4) != get_raft_log_len(i3)


def test_follower_restarts_after_leader_compacts(cluster: Cluster):
    i1, i2, i3 = cluster.deploy(instance_count=3)

    # Stop a follower
    i3.terminate()

    # Add more stuff to the raft log
    _ = cluster.add_instance(wait_online=True)

    # Compact log on leader
    i1.call("pico.raft_compact_log", 999)

    # raft_sync works successfully for i3
    i3.start()
    i3.wait_online()

    # i3 has received all the needed data
    assert i3.call("pico.space.instance:get", "i4") is not None

    # i3 sucessfully restarts
    i3.terminate()
    i3.start()
