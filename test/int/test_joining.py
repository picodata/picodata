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


def fake_join(peer: Instance, id: str, timeout: float):
    args = (
        f"{id}",  # instance_id
        None,  # replicaset_id
        f"{id}:3301",  # address
        False,  # voter
    )
    return peer.call(".raft_join", *args, timeout=timeout)


def test_concurrency(cluster2: Cluster):
    i1, i2 = cluster2.instances
    i1.promote_or_fail()

    assert i2.process is not None
    # Sigstop the follower making quorum temporarily unavailable.
    os.killpg(i2.process.pid, signal.SIGSTOP)
    eprint(f"{i2} signalled with SIGSTOP")

    # First request blocks the `join_loop` until i2 is resumed.
    with pytest.raises(OSError) as e0:
        fake_join(i1, "fake-0", timeout=0.1)
    assert e0.value.errno == errno.ECONNRESET

    # Subsequent requests get batched
    executor = ThreadPoolExecutor(max_workers=3)
    f1 = executor.submit(fake_join, i1, "fake-1", timeout=5)
    f2 = executor.submit(fake_join, i1, "fake-2", timeout=5)
    f3 = executor.submit(fake_join, i1, "fake-3", timeout=0.1)

    # Make sure all requests reach the server before resuming i2.
    with pytest.raises(OSError) as e1:
        f3.result()
    assert e1.value.errno == errno.ECONNRESET

    # Resume the follower.
    os.killpg(i2.process.pid, signal.SIGCONT)
    eprint(f"{i2} signalled with SIGCONT")

    ret1 = f1.result()[0]["peer"]
    ret2 = f2.result()[0]["peer"]
    assert ret1["instance_id"] == "fake-1"
    assert ret2["instance_id"] == "fake-2"
    # Make sure the batching works as expected
    assert ret1["commit_index"] == ret2["commit_index"]


def test_request_follower(cluster2: Cluster):
    _, i2 = cluster2.instances
    i2.assert_raft_status("Follower")

    with pytest.raises(TarantoolError) as e:
        fake_join(i2, "fake-0", timeout=1)
    assert e.value.args == ("ER_PROC_C", "not a leader")


def test_discovery(cluster: Cluster):
    cluster.deploy(instance_count=3)
    i1, i2, i3 = cluster.instances

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
    i4 = cluster.add_instance(peers=[i1.listen])
    i4.assert_raft_status("Follower", leader_id=i2.raft_id)

    # Run discovery against `--peer i3`.
    # It has performed a rebootstrap after discovery,
    # and now has the discovery module uninitialized.
    assert req_discover(i3) == [{"Done": {"NonLeader": {"leader": i2.listen}}}]

    # add instance
    i5 = cluster.add_instance(peers=[i3.listen])
    i5.assert_raft_status("Follower", leader_id=i2.raft_id)
