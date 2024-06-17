import pytest

from conftest import (
    Cluster,
    Instance,
    Retriable,
    TarantoolError,
    log_crawler,
    ProcessDead,
)

ER_OTHER = 10000


@pytest.fixture
def cluster2(cluster: Cluster):
    cluster.deploy(instance_count=2)
    return cluster


@pytest.fixture
def cluster3(cluster: Cluster):
    cluster.deploy(instance_count=3)
    return cluster


def raft_join(
    instance: Instance,
    cluster_id: str,
    instance_id: str,
    timeout_seconds: float | int,
    failure_domain: dict[str, str] = dict(),
):
    replicaset_id = None
    # Workaround slow address resolving. Intentionally use
    # invalid address format to eliminate blocking DNS requests.
    # See https://git.picodata.io/picodata/picodata/tarantool-module/-/issues/81
    address = f"nowhere/{instance_id}"
    return instance.call(
        ".proc_raft_join",
        cluster_id,
        instance_id,
        replicaset_id,
        address,
        failure_domain,
        instance.tier if instance.tier is not None else "default",
        timeout=timeout_seconds,
    )


def replicaset_id(instance: Instance):
    return instance.eval(
        "return box.space._pico_instance:get(...).replicaset_id", instance.instance_id
    )


def test_request_follower(cluster2: Cluster):
    i1, i2 = cluster2.instances
    i2.assert_raft_status("Follower")

    actual = raft_join(
        instance=i2, cluster_id=cluster2.id, instance_id="fake-0", timeout_seconds=1
    )
    # Even though a follower is called new instance is joined successfully
    assert actual["instance"]["raft_id"] == 3


def test_discovery(cluster3: Cluster):
    i1, i2, i3 = cluster3.instances

    # make sure i1 is leader
    i1.promote_or_fail()

    # change leader
    i2.promote_or_fail()

    def req_discover(instance: Instance):
        request = dict(tmp_id="unused", peers=["test:3301"])
        request_to = instance.listen
        return instance.call(".proc_discover", request, request_to)

    # Run discovery against `--instance i1`.
    # It used to be a bootstrap leader, but now it's just a follower.
    assert req_discover(i1) == {"Done": {"NonLeader": {"leader": i2.listen}}}

    # add instance
    i4 = cluster3.add_instance(peers=[i1.listen])
    i4.assert_raft_status("Follower", leader_id=i2.raft_id)

    # Run discovery against `--instance i3`.
    # It has performed a rebootstrap after discovery,
    # and now has the discovery module uninitialized.
    assert req_discover(i3) == {"Done": {"NonLeader": {"leader": i2.listen}}}

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

    # Add instance with the first instance being i1
    i4 = cluster3.add_instance(peers=[i1.listen, i2.listen, i3.listen])
    i4.assert_raft_status("Follower", leader_id=i2.raft_id)


def test_replication(cluster: Cluster):
    cluster.deploy(
        instance_count=2, init_replication_factor=2, service_password="secret"
    )
    i1, i2 = cluster.instances

    assert i1.replicaset_uuid() == i2.replicaset_uuid()

    def check_replicated(instance):
        box_replication = instance.eval("return box.cfg.replication")
        assert set(box_replication) == set(
            (f"pico_service:secret@{addr}" for addr in [i1.listen, i2.listen])
        ), instance

    for instance in cluster.instances:
        raft_instance = instance.eval(
            "return box.space._pico_instance:get(...):tomap()", instance.instance_id
        )
        space_cluster = instance.call("box.space._cluster:select")

        expected = {
            "instance_id": instance.instance_id,
            "instance_uuid": instance.eval("return box.info.uuid"),
            "raft_id": instance.raft_id,
            "replicaset_id": "r1",
            "replicaset_uuid": instance.eval("return box.info.cluster.uuid"),
            "current_state": ["Online", 1],
            "target_state": ["Online", 1],
            "failure_domain": dict(),
            "tier": "default",
        }
        assert {k: v for k, v in raft_instance.items() if k in expected} == expected

        assert space_cluster == [
            [1, i1.instance_uuid()],
            [2, i2.instance_uuid()],
        ]

        Retriable(timeout=10, rps=2).call(check_replicated, instance)

    # It doesn't affect replication setup
    # but speeds up the test by eliminating failover.
    i1.promote_or_fail()

    i2.assert_raft_status("Follower")
    i2.restart()
    Retriable(timeout=10, rps=2).call(check_replicated, i2)

    i2.wait_online()
    i2.promote_or_fail()

    i1.assert_raft_status("Follower")
    i1.restart()
    Retriable(timeout=10, rps=2).call(check_replicated, i1)


def test_tier_replication_factor(cluster: Cluster):
    i1 = cluster.add_instance(init_replication_factor=2)
    cluster.add_instance()
    cluster.add_instance()

    replicaset_ids = i1.eval(
        """
        return box.space._pico_instance:pairs()
            :map(function(instance)
                return instance.replicaset_id
            end)
            :totable()
    """
    )

    assert set(replicaset_ids) == {"r1", "r2"}


def test_cluster_id_mismatch(instance: Instance):
    wrong_cluster_id = "wrong-cluster-id"

    assert instance.cluster_id != wrong_cluster_id

    with pytest.raises(TarantoolError) as e:
        raft_join(
            instance=instance,
            cluster_id=wrong_cluster_id,
            instance_id="whatever",
            timeout_seconds=1,
        )
    assert e.value.args == (
        ER_OTHER,
        f'cluster_id mismatch: cluster_id of the instance = "wrong-cluster-id", cluster_id of the cluster = "{instance.cluster_id}"',  # noqa: E501
    )


@pytest.mark.xfail(
    run=False,
    reason=(
        "failed reading instance with id `3`: instance with id 3 not found, "
        "thread 'main' panicked, src/traft/node.rs:1515:17"
    ),
)
def test_rebootstrap_follower(cluster3: Cluster):
    # Scenario: rebootstrap a follower in a cluster of 3+
    #   Given a cluster of 3 instances
    #   When i3 is down
    #   And i3 data dir is removed
    #   And i3 started with same command-line arguments as first time
    #   Then i3 should become a follower

    i1, i2, i3 = cluster3.instances
    i3.restart(remove_data=True)
    i3.wait_online()
    i3.assert_raft_status("Follower")

    # git.picodata.io: #114
    assert i1.terminate() == 0


def test_join_without_explicit_instance_id(cluster: Cluster):
    # Scenario: bootstrap single instance without explicitly given instance id
    #   Given no instances started
    #   When two instances starts without instance_id given
    #   Then the one of the instances became Leader with instance_id=1
    #   And the second one of the became Follower with instance_id 2

    # don't generate instance_ids so that the Leader
    # chooses ones for them when they join
    i1 = cluster.add_instance(instance_id=False)
    i2 = cluster.add_instance(instance_id=False)

    i1.assert_raft_status("Leader")
    assert i1.instance_id == "i1"
    i2.assert_raft_status("Follower")
    assert i2.instance_id == "i2"


def test_failure_domains(cluster: Cluster):
    i1 = cluster.add_instance(
        failure_domain=dict(planet="Earth"), init_replication_factor=2
    )
    i1.assert_raft_status("Leader")
    assert replicaset_id(i1) == "r1"

    assert i1.cluster_id
    with pytest.raises(TarantoolError, match="missing failure domain names: PLANET"):
        raft_join(
            instance=i1,
            cluster_id=i1.cluster_id,
            instance_id="x1",
            failure_domain=dict(os="Arch"),
            timeout_seconds=1,
        )

    i2 = cluster.add_instance(failure_domain=dict(planet="Mars", os="Arch"))
    i2.assert_raft_status("Follower", leader_id=i1.raft_id)
    assert replicaset_id(i2) == "r1"

    with pytest.raises(TarantoolError, match="missing failure domain names: OS"):
        raft_join(
            instance=i1,
            cluster_id=i1.cluster_id,
            instance_id="x1",
            failure_domain=dict(planet="Venus"),
            timeout_seconds=1,
        )

    i3 = cluster.add_instance(failure_domain=dict(planet="Venus", os="BSD"))
    i3.assert_raft_status("Follower", leader_id=i1.raft_id)
    assert replicaset_id(i3) == "r2"


def test_reconfigure_failure_domains(cluster: Cluster):
    i1 = cluster.add_instance(
        failure_domain=dict(planet="Earth"), init_replication_factor=2
    )
    i1.assert_raft_status("Leader")
    assert replicaset_id(i1) == "r1"

    i2 = cluster.add_instance(failure_domain=dict(planet="Mars"))
    assert replicaset_id(i2) == "r1"

    i2.terminate()
    # fail to start without needed domain subdivisions
    i2.failure_domain = dict(owner="Bob")
    i2.fail_to_start()

    i2.terminate()
    i2.failure_domain = dict(planet="Earth", owner="Bob")
    i2.start()
    i2.wait_online()
    # replicaset doesn't change automatically
    assert replicaset_id(i2) == "r1"

    i2.terminate()
    # fail to remove domain subdivision
    i2.failure_domain = dict(planet="Mars")
    i2.fail_to_start()


def test_fail_to_join(cluster: Cluster):
    # Check scenarios in which instances fail to join the cluster for different
    # reasons

    i1 = cluster.add_instance(failure_domain=dict(owner="Tom"))

    # Cluster has a required failure domain,
    # so instance without the required failure domain cannot join
    # and therefore exits with failure
    cluster.fail_to_add_instance(failure_domain=dict())

    # An instance with the given instance_id is already present in the cluster
    # so this instance cannot join
    # and therefore exits with failure
    assert i1.instance_id is not None
    cluster.fail_to_add_instance(
        instance_id=i1.instance_id, failure_domain=dict(owner="Jim")
    )

    # Tiers in cluster: storage
    # instance with unexisting tier cannot join and therefore exits with failure
    assert i1.instance_id is not None
    cluster.fail_to_add_instance(tier="noexistent_tier")

    joined_instances = i1.eval(
        """
        return box.space._pico_instance:pairs()
            :map(function(instance)
                return { instance.instance_id, instance.raft_id }
            end)
            :totable()
    """
    )
    assert {tuple(i) for i in joined_instances} == {(i1.instance_id, i1.raft_id)}


def test_pico_service_invalid_password(cluster: Cluster):
    password_file = f"{cluster.data_dir}/service-password.txt"
    with open(password_file, "w") as f:
        print("secret", file=f)

    i1 = cluster.add_instance(wait_online=False)
    i1.service_password_file = password_file
    i1.start()
    i1.wait_online()

    i2 = cluster.add_instance(wait_online=False)
    lc = log_crawler(i2, "User not found or supplied credentials are invalid")
    i2.start()
    # i2 blocks on the "discovery" stage, because only pico_service is allowed
    # to call .proc_discover, but i2 doesn't know the password.
    #
    # And we don't exit from "discovery" stage on error
    lc.wait_matched()
    i2.terminate()

    # Now i2 knows the password so it successfully joins
    i2.service_password_file = password_file
    i2.start()
    i2.wait_online()
    i2.terminate()

    # i2 forgets the password again, and now it does exit with error,
    # because self activation fails
    i2.service_password_file = None
    lc.matched = False
    i2.fail_to_start()
    assert lc.matched


def test_join_with_duplicate_instance_id(cluster: Cluster):
    [leader, quorum_helper] = cluster.deploy(instance_count=2)

    namesakes = []
    log_crawlers = []
    for _ in range(5):
        i = cluster.add_instance(wait_online=False, instance_id="original-name")
        lc = log_crawler(i, "`original-name` is already joined")
        log_crawlers.append(lc)
        namesakes.append(i)

    for i in namesakes:
        i.start()

    sole_survivor = None
    for i, lc in zip(namesakes, log_crawlers):
        try:
            i.wait_online()
            assert sole_survivor is None
            sole_survivor = i
        except ProcessDead:
            assert lc.matched
    assert sole_survivor

    sole_survivor.terminate()
    cluster.expel(sole_survivor, leader)

    #
    # After the instance got expelled it's ok to join a new one with the same name
    #
    namesakes = []
    log_crawlers = []
    for _ in range(5):
        i = cluster.add_instance(wait_online=False, instance_id="original-name")
        lc = log_crawler(i, "`original-name` is already joined")
        log_crawlers.append(lc)
        namesakes.append(i)

    for i in namesakes:
        i.start()

    sole_survivor = None
    for i, lc in zip(namesakes, log_crawlers):
        try:
            i.wait_online()
            assert sole_survivor is None
            sole_survivor = i
        except ProcessDead:
            assert lc.matched
    assert sole_survivor
