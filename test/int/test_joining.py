import os
import errno
import signal
import pytest

from concurrent.futures import ThreadPoolExecutor

from conftest import (
    eprint,
    Cluster,
    Instance,
    TarantoolError,
)


@pytest.fixture
def cluster2(cluster: Cluster):
    cluster.deploy(instance_count=2)
    return cluster


@pytest.fixture
def cluster3(cluster: Cluster):
    cluster.deploy(instance_count=3)
    return cluster


def raft_join(peer: Instance, id: str, timeout: float):
    instance_id = f"{id}"
    replicaset_id = None
    # Workaround slow address resolving. Intentionally use
    # invalid address format to eliminate blocking DNS requests.
    # See https://git.picodata.io/picodata/picodata/tarantool-module/-/issues/81
    address = f"nowhere/{id}"
    is_voter = False
    return peer.call(
        ".raft_join",
        instance_id,
        replicaset_id,
        address,
        is_voter,
        timeout=timeout,
    )


def test_concurrency(cluster2: Cluster):
    i1, i2 = cluster2.instances
    i1.promote_or_fail()

    assert i2.process is not None
    # Sigstop the follower making quorum temporarily unavailable.
    os.killpg(i2.process.pid, signal.SIGSTOP)
    eprint(f"{i2} signalled with SIGSTOP")

    # First request blocks the `join_loop` until i2 is resumed.
    with pytest.raises(OSError) as e0:
        raft_join(i1, "fake-0", timeout=0.1)
    assert e0.value.errno == errno.ECONNRESET

    # Subsequent requests get batched
    executor = ThreadPoolExecutor()
    f1 = executor.submit(raft_join, i1, "fake-1", timeout=5)
    f2 = executor.submit(raft_join, i1, "fake-2", timeout=5)
    f3 = executor.submit(raft_join, i1, "fake-3", timeout=0.1)

    # Make sure all requests reach the server before resuming i2.
    with pytest.raises(OSError) as e1:
        f3.result()
    assert e1.value.errno == errno.ECONNRESET

    # Resume the follower.
    os.killpg(i2.process.pid, signal.SIGCONT)
    eprint(f"{i2} signalled with SIGCONT")

    peer1 = f1.result()[0]["peer"]
    peer2 = f2.result()[0]["peer"]
    assert peer1["instance_id"] == "fake-1"
    assert peer2["instance_id"] == "fake-2"
    # Make sure the batching works as expected
    assert peer1["commit_index"] == peer2["commit_index"]


def test_request_follower(cluster2: Cluster):
    _, i2 = cluster2.instances
    i2.assert_raft_status("Follower")

    with pytest.raises(TarantoolError) as e:
        raft_join(i2, "fake-0", timeout=1)
    assert e.value.args == ("ER_PROC_C", "not a leader")


def test_uuids(cluster2: Cluster):
    i1, i2 = cluster2.instances
    i1.assert_raft_status("Leader")

    peer_1 = i1.call(
        ".raft_join",
        i1.instance_id,
        None,  # replicaset_id
        i1.listen,  # address
        True,  # voter
    )[0]["peer"]
    assert peer_1["instance_id"] == i1.instance_id
    assert peer_1["instance_uuid"] == i1.eval("return box.info.uuid")
    assert peer_1["replicaset_uuid"] == i1.eval("return box.info.cluster.uuid")

    peer_2 = i1.call(
        ".raft_join",
        i2.instance_id,
        None,  # replicaset_id
        i2.listen,  # address
        True,  # voter
    )[0]["peer"]
    assert peer_2["instance_id"] == i2.instance_id
    assert peer_2["instance_uuid"] == i2.eval("return box.info.uuid")
    assert peer_2["replicaset_uuid"] == i2.eval("return box.info.cluster.uuid")

    # Two consequent requests must obtain same raft_id and instance_id
    fake_peer_1 = raft_join(i1, "fake", timeout=1)[0]["peer"]
    fake_peer_2 = raft_join(i1, "fake", timeout=1)[0]["peer"]
    assert fake_peer_1["instance_id"] == "fake"
    assert fake_peer_2["instance_id"] == "fake"
    assert fake_peer_1["raft_id"] == fake_peer_2["raft_id"]
    assert fake_peer_1["instance_uuid"] == fake_peer_2["instance_uuid"]


def test_discovery(cluster3: Cluster):
    i1, i2, i3 = cluster3.instances

    # make sure i1 is leader
    i1.promote_or_fail()

    # change leader
    i2.promote_or_fail()

    def req_discover(peer: Instance):
        request = dict(tmp_id="unused", peers=["test:3301"])
        request_to = peer.listen
        return peer.call(".proc_discover", request, request_to)

    # Run discovery against `--peer i1`.
    # It used to be a bootstrap leader, but now it's just a follower.
    assert req_discover(i1) == [{"Done": {"NonLeader": {"leader": i2.listen}}}]

    # add instance
    i4 = cluster3.add_instance(peers=[i1.listen])
    i4.assert_raft_status("Follower", leader_id=i2.raft_id)

    # Run discovery against `--peer i3`.
    # It has performed a rebootstrap after discovery,
    # and now has the discovery module uninitialized.
    assert req_discover(i3) == [{"Done": {"NonLeader": {"leader": i2.listen}}}]

    # add instance
    i5 = cluster3.add_instance(peers=[i3.listen])
    i5.assert_raft_status("Follower", leader_id=i2.raft_id)


def test_replication(cluster2: Cluster):
    i1, i2 = cluster2.instances

    assert i1.replicaset_uuid() == i2.replicaset_uuid()

    for instance in cluster2.instances:
        with instance.connect(1) as conn:
            raft_peer = conn.select("raft_group", [instance.raft_id])[0]
            space_cluster = conn.select("_cluster")
            cfg_replication = conn.eval("return box.cfg.replication")

        assert raft_peer[:-1] == [
            instance.raft_id,
            instance.eval("return box.info.listen"),
            True,  # voter
            instance.instance_id,
            "r1",
            instance.eval("return box.info.uuid"),
            instance.eval("return box.info.cluster.uuid"),
        ]

        assert list(space_cluster) == [
            [1, i1.instance_uuid()],
            [2, i2.instance_uuid()],
        ]

        if instance == i1:
            with pytest.raises(AssertionError):  # FIXME
                assert cfg_replication[0] == [i1.listen, i2.listen]
        else:
            assert cfg_replication[0] == [i1.listen, i2.listen]
