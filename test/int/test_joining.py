from functools import partial
import os
import errno
import re
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


def raft_join(
    peer: Instance, cluster_id: str, instance_id: str, timeout_seconds: float | int
):
    replicaset_id = None
    # Workaround slow address resolving. Intentionally use
    # invalid address format to eliminate blocking DNS requests.
    # See https://git.picodata.io/picodata/picodata/tarantool-module/-/issues/81
    address = f"nowhere/{instance_id}"
    is_voter = False
    return peer.call(
        ".raft_join",
        cluster_id,
        instance_id,
        replicaset_id,
        address,
        is_voter,
        timeout=timeout_seconds,
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
        raft_join(
            peer=i1, cluster_id=cluster2.id, instance_id="fake-0", timeout_seconds=0.1
        )
    assert e0.value.errno == errno.ECONNRESET

    # Subsequent requests get batched
    executor = ThreadPoolExecutor()
    submit_join = partial(executor.submit, raft_join, peer=i1, cluster_id=cluster2.id)

    f1 = submit_join(instance_id="fake-1", timeout_seconds=5)
    f2 = submit_join(instance_id="fake-2", timeout_seconds=5)
    f3 = submit_join(instance_id="fake-3", timeout_seconds=0.1)

    # Make sure all requests reach the server before resuming i2.
    with pytest.raises(OSError) as e1:
        f3.result()
    assert e1.value.errno == errno.ECONNRESET

    # Resume the follower.
    os.killpg(i2.process.pid, signal.SIGCONT)
    eprint(f"{i2} signalled with SIGCONT")

    peer1 = f1.result()[0]["peer"]  # type: ignore
    peer2 = f2.result()[0]["peer"]  # type: ignore
    assert peer1["instance_id"] == "fake-1"
    assert peer2["instance_id"] == "fake-2"
    # Make sure the batching works as expected
    assert peer1["commit_index"] == peer2["commit_index"]


def test_request_follower(cluster2: Cluster):
    _, i2 = cluster2.instances
    i2.assert_raft_status("Follower")

    with pytest.raises(TarantoolError) as e:
        raft_join(
            peer=i2, cluster_id=cluster2.id, instance_id="fake-0", timeout_seconds=1
        )
    assert e.value.args == ("ER_PROC_C", "not a leader")


def test_uuids(cluster2: Cluster):
    i1, i2 = cluster2.instances
    i1.assert_raft_status("Leader")

    peer_1 = i1.call(
        ".raft_join",
        cluster2.id,
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
        cluster2.id,
        i2.instance_id,
        None,  # replicaset_id
        i2.listen,  # address
        True,  # voter
    )[0]["peer"]
    assert peer_2["instance_id"] == i2.instance_id
    assert peer_2["instance_uuid"] == i2.eval("return box.info.uuid")
    assert peer_2["replicaset_uuid"] == i2.eval("return box.info.cluster.uuid")

    def join():
        return raft_join(
            peer=i1,
            cluster_id=cluster2.id,
            instance_id="fake",
            timeout_seconds=1,
        )

    # Two consequent requests must obtain same raft_id and instance_id
    fake_peer_1 = join()[0]["peer"]
    fake_peer_2 = join()[0]["peer"]

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


def test_parallel(cluster3: Cluster):
    i1, i2, i3 = cluster3.instances

    # Make sure cluster is ready
    i1.promote_or_fail()
    i2.assert_raft_status("Follower", leader_id=i1.raft_id)
    i3.assert_raft_status("Follower", leader_id=i1.raft_id)

    # Kill i1
    i1.terminate()

    # Make sure cluster is ready
    i2.promote_or_fail()
    i3.assert_raft_status("Follower", leader_id=i2.raft_id)

    # Add instance with the first peer being i1
    i4 = cluster3.add_instance(peers=[i1.listen, i2.listen, i3.listen])
    i4.assert_raft_status("Follower", leader_id=i2.raft_id)


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


def test_cluster_id_mismatch(instance: Instance):
    wrong_cluster_id = "wrong-cluster-id"

    assert wrong_cluster_id != instance.cluster_id

    expected_error_re = re.escape(
        "cannot join the instance to the cluster: cluster_id mismatch:"
        ' cluster_id of the instance = "wrong-cluster-id",'
        f' cluster_id of the cluster = "{instance.cluster_id}"'
    )

    with pytest.raises(TarantoolError, match=expected_error_re):
        raft_join(
            peer=instance,
            cluster_id=wrong_cluster_id,
            instance_id="whatever",
            timeout_seconds=1,
        )
