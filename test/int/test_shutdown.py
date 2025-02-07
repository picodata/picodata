import os
import signal
from conftest import Cluster, Instance, log_crawler

ON_SHUTDOWN_TIMEOUT = "on_shutdown triggers failed"


def test_gl119_panic_on_shutdown(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    i2.assert_raft_status("Follower", leader_id=i1.raft_id)

    # suspend i1 (leader) and force i2 to start a new term
    assert i1.process is not None
    os.killpg(i1.process.pid, signal.SIGSTOP)
    i2.call(".proc_raft_promote")
    # it can't win the election because there is no quorum
    i2.assert_raft_status("PreCandidate")

    crawler = log_crawler(i2, ON_SHUTDOWN_TIMEOUT)

    # stopping i2 in that state still shouldn't be a problem
    assert i2.terminate() == 0

    # though on_shutdown trigger fails
    crawler.wait_matched()


def test_single(instance: Instance):
    crawler = log_crawler(instance, ON_SHUTDOWN_TIMEOUT)

    instance.terminate()
    assert not crawler.matched


def test_couple_leader_first(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    # make sure i1 is leader
    i1.assert_raft_status("Leader")
    i2.assert_raft_status("Follower", leader_id=i1.raft_id)

    c1 = log_crawler(i1, ON_SHUTDOWN_TIMEOUT)
    i1.terminate()
    assert not c1.matched

    i2.assert_raft_status("Leader")
    cluster.wait_has_states(i1, "Offline", "Offline")

    c2 = log_crawler(i2, ON_SHUTDOWN_TIMEOUT)
    i2.terminate()
    assert not c2.matched


def test_couple_follower_first(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    # make sure i1 is leader
    i1.assert_raft_status("Leader")
    i2.assert_raft_status("Follower", leader_id=i1.raft_id)

    c2 = log_crawler(i2, ON_SHUTDOWN_TIMEOUT)
    i2.terminate()
    assert not c2.matched

    cluster.wait_has_states(i2, "Offline", "Offline")

    c1 = log_crawler(i1, ON_SHUTDOWN_TIMEOUT)
    i1.terminate()
    assert not c1.matched


def test_threesome(cluster: Cluster):
    i1, i2, i3 = cluster.deploy(instance_count=3)

    # make sure i1 is leader
    i1.assert_raft_status("Leader")
    i2.assert_raft_status("Follower", leader_id=i1.raft_id)
    i3.assert_raft_status("Follower", leader_id=i1.raft_id)

    c1 = log_crawler(i1, ON_SHUTDOWN_TIMEOUT)
    i1.terminate()
    assert not c1.matched

    c2 = log_crawler(i2, ON_SHUTDOWN_TIMEOUT)
    i2.terminate()
    assert not c2.matched


def test_instance_from_falsy_tier_is_not_voter(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    tier:
        storage:
            replication_factor: 1
            can_vote: true
        router:
            replication_factor: 1
            can_vote: false
instance:
    cluster_name: my-cluster
    tier: storage
    log:
        level: verbose
"""
    )

    i1 = cluster.add_instance(wait_online=False, tier="storage")
    i1.start()
    i1.wait_online()

    i2 = cluster.add_instance(wait_online=False, tier="router")
    i2.start()
    i2.wait_online()

    c1 = log_crawler(
        i1,
        "leader is going offline and no substitution is found, voters: [1], leader_raft_id: 1",
    )

    # make sure i1 is leader
    i1.assert_raft_status("Leader")
    i2.assert_raft_status("Follower", leader_id=i1.raft_id)

    i1.terminate()

    i2.assert_raft_status("Follower", leader_id=i1.raft_id)
    c1.wait_matched()


def test_deploy_crash_with_wrong_bootstrap_leader(cluster: Cluster):
    # bootstrap leader from tier with can_vote = false

    cluster.set_config_file(
        yaml="""
cluster:
    tier:
        storage:
            replication_factor: 1
            can_vote: false
instance:
    cluster_name: my-cluster
    tier: storage
    log:
        level: verbose
"""
    )

    i1 = cluster.add_instance(wait_online=False, tier="storage")

    c1 = log_crawler(
        i1,
        "CRITICAL: invalid configuration: instance with instance_name 'storage_1_1' from tier "
        "'storage' with `can_vote = false` cannot be a bootstrap leader",
    )

    i1.fail_to_start()

    c1.wait_matched()
