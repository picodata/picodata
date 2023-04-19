from conftest import Cluster

_3_SEC = 3


def test_bootstrap_from_snapshot(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)

    ret = i1.cas("insert", "_picodata_property", ["animal", "horse"])
    i1.call(".proc_sync_raft", ret, (_3_SEC, 0))
    assert i1.call("pico.raft_read_index", _3_SEC) == ret

    # Compact the whole log
    assert i1.raft_compact_log() == ret + 1

    # Ensure i2 bootstraps from a snapshot
    i2 = cluster.add_instance(wait_online=True)
    # Whenever a snapshot is applied, all preceeding raft log is
    # implicitly compacted. Adding an instance implies appending 3
    # entries to the raft log. i2 catches them via a snapshot.
    assert i2.raft_first_index() == i1.raft_first_index() + 3

    # Ensure new instance replicates the property
    assert i2.call("pico.space.property:get", "animal") == ["animal", "horse"]


def test_catchup_by_snapshot(cluster: Cluster):
    i1, i2, i3 = cluster.deploy(instance_count=3)
    i1.assert_raft_status("Leader")
    ret = i1.cas("insert", "_picodata_property", ["animal", "tiger"])

    i3.call(".proc_sync_raft", ret, (_3_SEC, 0))
    assert i3.call("pico.space.property:get", "animal") == ["animal", "tiger"]
    assert i3.raft_first_index() == 1
    i3.terminate()

    # TODO: i1.cas("delete", "_picodata_property", ["animal"])
    i1.cas("replace", "_picodata_property", ["animal", "lion"])
    ret = i1.cas("insert", "_picodata_property", ["tree", "birch"])

    for i in [i1, i2]:
        i.call(".proc_sync_raft", ret, (_3_SEC, 0))
        assert i.raft_compact_log() == ret + 1

    # Ensure i3 is able to sync raft using a snapshot
    i3.start()
    i3.wait_online()

    assert i3.call("pico.space.property:get", "animal") == ["animal", "lion"]
    assert i3.call("pico.space.property:get", "tree") == ["tree", "birch"]

    # Since there were no cas requests since log compaction, the indexes
    # should be equal.
    assert i3.raft_first_index() == ret + 1

    # There used to be a problem: catching snapshot used to cause
    # inconsistent raft state so the second restart used to panic.
    i3.terminate()
    i3.start()
    i3.wait_online()
