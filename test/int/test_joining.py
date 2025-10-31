import pytest
import uuid

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
    cluster_name: str,
    instance_name: str,
    uuid: str,
    timeout_seconds: float | int,
    failure_domain: dict[str, str] = dict(),
):
    replicaset_name = None
    # Workaround slow address resolving. Intentionally use
    # invalid address format to eliminate blocking DNS requests.
    # See https://git.picodata.io/picodata/picodata/tarantool-module/-/issues/81
    address = f"nowhere/{instance_name}"
    pg_address = f"pg_nowhere/{instance_name}"
    picodata_version = instance.call(".proc_version_info")["picodata_version"]
    return instance.call(
        ".proc_raft_join",
        cluster_name,
        instance_name,
        replicaset_name,
        address,
        pg_address,
        failure_domain,
        instance.tier if instance.tier is not None else "default",
        picodata_version,
        uuid,
        timeout=timeout_seconds,
    )


def replicaset_name(instance: Instance):
    return instance.eval("return box.space._pico_instance:get(...).replicaset_name", instance.name)


def count_instances_by_uuid(instance, uuid_str: str) -> int:
    return instance.sql('SELECT COUNT(*) FROM _pico_instance WHERE "uuid" = ?', uuid_str)[0][0]


def test_request_follower(cluster2: Cluster):
    i1, i2 = cluster2.instances
    i2.assert_raft_status("Follower")

    generated_uuid = str(uuid.uuid4())
    actual = raft_join(
        instance=i2,
        cluster_name=cluster2.id,
        instance_name="fake-0",
        uuid=generated_uuid,
        timeout_seconds=1,
    )
    # Even though a follower is called new instance is joined successfully
    assert actual["instance"]["raft_id"] == 3


def test_discovery(cluster3: Cluster):
    i1, i2, i3 = cluster3.instances

    # make sure i1 is leader
    i1.promote_or_fail()

    # change leader
    i2.promote_or_fail()

    # Wait until i1 knows that i2 is elected to reduce test flakiness
    # (proc_discover may return an error during raft leader elections).
    Retriable(timeout=5, rps=4).call(i1.assert_raft_status, "Follower", i2.raft_id)

    def req_discover(instance: Instance) -> dict:
        request = dict(tmp_id="unused", peers=["test:3301"], can_vote=True, votable_peers=[])
        request_to = instance.iproto_listen
        return instance.call(".proc_discover", request, request_to)

    # Run discovery against `--instance i1`.
    # It used to be a bootstrap leader, but now it's just a follower.
    assert req_discover(i1) == {"Done": {"NonLeader": {"leader": i2.iproto_listen}}}

    # add instance
    i4 = cluster3.add_instance(peers=[i1.iproto_listen])
    i4.assert_raft_status("Follower", leader_id=i2.raft_id)

    # Run discovery against `--instance i3`.
    # It has performed a rebootstrap after discovery,
    # and now has the discovery module uninitialized.
    assert req_discover(i3) == {"Done": {"NonLeader": {"leader": i2.iproto_listen}}}

    # add instance
    i5 = cluster3.add_instance(peers=[i3.iproto_listen])
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
    i4 = cluster3.add_instance(peers=[i1.iproto_listen, i2.iproto_listen, i3.iproto_listen])
    i4.assert_raft_status("Follower", leader_id=i2.raft_id)


def test_basic_replication_setup(cluster: Cluster):
    cluster.deploy(instance_count=2, init_replication_factor=2, service_password="secret")
    i1, i2 = cluster.instances

    assert i1.replicaset_uuid() == i2.replicaset_uuid()

    def check_replicated(instance):
        box_replication = instance.eval("return box.cfg.replication")
        assert sorted(box_replication, key=lambda d: d["uri"]) == [
            {"uri": f"pico_service:secret@{addr}"} for addr in [i1.iproto_listen, i2.iproto_listen]
        ], instance

    for instance in cluster.instances:
        raft_instance = instance.eval("return box.space._pico_instance:get(...):tomap()", instance.name)
        space_cluster = instance.call("box.space._cluster:select")

        expected = {
            "name": instance.name,
            "uuid": instance.eval("return box.info.uuid"),
            "raft_id": instance.raft_id,
            "replicaset_name": "default_1",
            "replicaset_uuid": instance.eval("return box.info.cluster.uuid"),
            "current_state": ["Online", 1],
            "target_state": ["Online", 1],
            "failure_domain": dict(),
            "tier": "default",
        }
        assert {k: v for k, v in raft_instance.items() if k in expected} == expected

        assert space_cluster == [
            [1, i1.uuid()],
            [2, i2.uuid()],
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

    replicaset_names = i1.eval(
        """
        return box.space._pico_instance:pairs()
            :map(function(instance)
                return instance.replicaset_name
            end)
            :totable()
    """
    )

    assert set(replicaset_names) == {"default_1", "default_2"}


def test_cluster_name_mismatch(instance: Instance):
    wrong_cluster_name = "wrong-cluster-name"

    assert instance.cluster_name != wrong_cluster_name

    with pytest.raises(TarantoolError) as e:
        raft_join(
            instance=instance,
            cluster_name=wrong_cluster_name,
            instance_name="whatever",
            uuid=instance.uuid(),
            timeout_seconds=1,
        )
    assert e.value.args[:2] == (
        ER_OTHER,
        f'cluster_name mismatch: cluster_name of the instance = "wrong-cluster-name", cluster_name of the cluster = "{instance.cluster_name}"',  # noqa: E501
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


def test_separate_clusters(cluster: Cluster):
    # See https://git.picodata.io/picodata/picodata/picodata/-/issues/680
    #
    # Despite two instances are added to the same cluster, we
    # intentionally clear peers so that `picodata run` is called without
    # `--peer` option specified. As a result two instances form their
    # own clusters and don't interact at all.
    #
    i1a = cluster.add_instance(wait_online=False)
    i1b = cluster.add_instance(wait_online=False)

    i1a.peers.clear()
    i1b.peers.clear()

    i1a.start()
    i1b.start()

    i1a.wait_online()
    i1b.wait_online()

    i1a_info = i1a.call(".proc_instance_info")
    i1b_info = i1b.call(".proc_instance_info")

    # We expect both instances to have raft_id 1, that means they form
    # separate clusters
    assert i1a_info["raft_id"] == 1
    assert i1b_info["raft_id"] == 1

    # See https://git.picodata.io/picodata/picodata/picodata/-/issues/390
    # Despite instance_name and replicaset_name are the same, their uuids differ

    assert i1a_info["name"] == "default_1_1"
    assert i1b_info["name"] == "default_1_1"
    assert i1a_info["uuid"] != i1b_info["uuid"]

    assert i1a_info["replicaset_name"] == "default_1"
    assert i1b_info["replicaset_name"] == "default_1"
    assert i1a_info["replicaset_uuid"] != i1b_info["replicaset_uuid"]


def test_join_without_explicit_instance_name(cluster: Cluster):
    # Scenario: bootstrap single instance without explicitly given instance name
    #   Given no instances started
    #   When two instances starts without instance_name given
    #   Then the one of the instances became Leader with instance name=1
    #   And the second one of the became Follower with instance name 2

    # don't generate instance_names so that the Leader
    # chooses ones for them when they join
    i1 = cluster.add_instance()
    i2 = cluster.add_instance()

    i1.assert_raft_status("Leader")
    assert i1.name == "default_1_1"
    i2.assert_raft_status("Follower")
    assert i2.name == "default_2_1"


def test_failure_domains(cluster: Cluster):
    i1 = cluster.add_instance(failure_domain=dict(planet="Earth"), init_replication_factor=2)
    i1.assert_raft_status("Leader")
    assert replicaset_name(i1) == "default_1"

    assert i1.cluster_name

    generated_uuid = str(uuid.uuid4())
    with pytest.raises(TarantoolError, match="missing failure domain names: PLANET"):
        raft_join(
            instance=i1,
            cluster_name=i1.cluster_name,
            instance_name="x1",
            uuid=generated_uuid,
            failure_domain=dict(os="Arch"),
            timeout_seconds=1,
        )

    i2 = cluster.add_instance(failure_domain=dict(planet="Mars", os="Arch"))
    i2.assert_raft_status("Follower", leader_id=i1.raft_id)
    assert replicaset_name(i2) == "default_1"

    generated_uuid2 = str(uuid.uuid4())
    with pytest.raises(TarantoolError, match="missing failure domain names: OS"):
        raft_join(
            instance=i1,
            cluster_name=i1.cluster_name,
            instance_name="x1",
            uuid=generated_uuid2,
            failure_domain=dict(planet="Venus"),
            timeout_seconds=1,
        )

    i3 = cluster.add_instance(failure_domain=dict(planet="Venus", os="BSD"))
    i3.assert_raft_status("Follower", leader_id=i1.raft_id)
    assert replicaset_name(i3) == "default_2"


def test_reconfigure_failure_domains(cluster: Cluster):
    i1 = cluster.add_instance(failure_domain=dict(planet="Earth"), init_replication_factor=2)
    i1.assert_raft_status("Leader")
    assert replicaset_name(i1) == "default_1"

    i2 = cluster.add_instance(failure_domain=dict(planet="Mars"))
    assert replicaset_name(i2) == "default_1"

    i2.terminate()
    # fail to start without needed domain subdivisions
    i2.failure_domain = dict(owner="Bob")
    i2.fail_to_start()

    i2.terminate()
    i2.failure_domain = dict(planet="Earth", owner="Bob")
    i2.start()
    i2.wait_online()
    # replicaset doesn't change automatically
    assert replicaset_name(i2) == "default_1"

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

    # An instance with the given instance_name is already present in the cluster
    # so this instance cannot join
    # and therefore exits with failure
    assert i1.name is not None
    cluster.fail_to_add_instance(name=i1.name, failure_domain=dict(owner="Jim"))

    # Tiers in cluster: storage
    # instance with unexisting tier cannot join and therefore exits with failure
    assert i1.name is not None
    cluster.fail_to_add_instance(tier="noexistent_tier")

    joined_instances = i1.eval(
        """
        return box.space._pico_instance:pairs()
            :map(function(instance)
                return { instance.name, instance.raft_id }
            end)
            :totable()
    """
    )
    assert {tuple(i) for i in joined_instances} == {(i1.name, i1.raft_id)}


def test_pico_service_invalid_existing_password(cluster: Cluster):
    password = "secret"
    i1 = cluster.add_instance(wait_online=False, service_password=password)
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
    i2.set_service_password(password)
    i2.start()
    i2.wait_online()
    i2.terminate()

    # # i2 forgets the password again, and now it should error during discovery,
    # # because self activation fails
    i2.set_service_password("wrongpassword")
    lc.matched = False
    i2.fail_to_start()
    assert lc.matched


def test_pico_service_invalid_requirements_password(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)

    invalid_password = ""
    i1.set_service_password(invalid_password)
    lc = log_crawler(i1, "CRITICAL: service password cannot be empty")
    i1.fail_to_start()
    lc.wait_matched()

    # manually overwrite the file with a non-ascii password
    assert i1.service_password_file
    with open(i1.service_password_file, "wb") as f:
        f.write(b"\x80")
    lc = log_crawler(i1, "CRITICAL: service password must be encoded as utf-8")
    i1.fail_to_start()
    lc.wait_matched()

    invalid_password = "\n\n"
    i1.set_service_password(invalid_password)
    lc = log_crawler(i1, "CRITICAL: service password cannot start with a newline character")
    i1.fail_to_start()
    lc.wait_matched()

    invalid_password = "\n\nnothing"
    i1.set_service_password(invalid_password)
    lc = log_crawler(i1, "CRITICAL: service password cannot start with a newline character")
    i1.fail_to_start()
    lc.wait_matched()

    invalid_password = "hello\n\nworld"
    i1.set_service_password(invalid_password)
    lc = log_crawler(i1, "CRITICAL: service password cannot be split into multiple lines")
    i1.fail_to_start()
    lc.wait_matched()

    invalid_password = "€"
    i1.set_service_password(invalid_password)
    lc = log_crawler(i1, "CRITICAL: service password characters must be within ascii range")
    i1.fail_to_start()
    lc.wait_matched()

    invalid_password = "s3cr3t@M3ss4g3"
    i1.set_service_password(invalid_password)
    lc = log_crawler(i1, "CRITICAL: service password characters must be alphanumeric")
    i1.fail_to_start()
    lc.wait_matched()


def test_join_with_duplicate_instance_name(cluster: Cluster):
    same_replicaset = "r1"
    cluster.set_service_password("secret")

    leader = cluster.add_instance(replicaset_name=same_replicaset, wait_online=False)
    # Quorum helper
    cluster.add_instance(replicaset_name=same_replicaset, wait_online=False)
    cluster.wait_online()

    namesakes = []
    log_crawlers = []
    for _ in range(5):
        i = cluster.add_instance(
            wait_online=False,
            name="original-name",
            replicaset_name=same_replicaset,
        )
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

    cluster.expel(sole_survivor, leader, force=True)

    #
    # After the instance got expelled it's ok to join a new one with the same name
    #
    namesakes = []
    log_crawlers = []
    for _ in range(5):
        i = cluster.add_instance(
            wait_online=False,
            name="original-name",
            replicaset_name=same_replicaset,
        )
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
            lc.wait_matched()
    assert sole_survivor


def test_tier_mismatch_while_joining_by_the_same_replicaset_name(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        default:
        router:
"""
    )
    _ = cluster.add_instance(replicaset_name="r1")
    _ = cluster.add_instance(replicaset_name="r2")

    instance = cluster.add_instance(replicaset_name="r2", tier="router", wait_online=False)

    msg = "tier mismatch: requested replicaset 'r2' is from tier 'default', but specified tier is 'router'"  # noqa E501

    lc = log_crawler(instance, msg)
    with pytest.raises(
        ProcessDead,
    ):
        instance.start()
        instance.wait_online()

    lc.wait_matched()


def test_proc_raft_join_is_idempotent(cluster: Cluster):
    """Calling .proc_raft_join multiple times with the same UUID should not
    create duplicates and should return the same record."""

    cluster.deploy(instance_count=1)
    leader = cluster.instances[0]

    instance = cluster.add_instance()
    instance.assert_raft_status("Follower", leader_id=leader.raft_id)

    instance_uuid = instance.uuid()

    assert count_instances_by_uuid(leader, instance_uuid) == 1

    instance.terminate()

    for i in range(3):
        result = raft_join(
            instance=leader,
            cluster_name=cluster.id,
            instance_name=str(instance.name),
            uuid=instance_uuid,
            timeout_seconds=1,
        )
        assert result["instance"]["uuid"] == instance_uuid

    assert count_instances_by_uuid(leader, instance_uuid) == 1


def test_proc_raft_join_fails_for_alive_instance(cluster: Cluster):
    """Calling .proc_raft_join for an alive instance should return an error."""

    cluster.deploy(instance_count=1)
    leader = cluster.instances[0]

    instance = cluster.add_instance()
    instance.assert_raft_status("Follower", leader_id=leader.raft_id)

    instance_uuid = instance.uuid()

    cluster.wait_has_states(instance, "Online", "Online")

    assert count_instances_by_uuid(leader, instance_uuid) == 1

    with pytest.raises(TarantoolError) as exc:
        raft_join(
            instance=leader,
            cluster_name=cluster.id,
            instance_name=str(instance.name),
            uuid=instance_uuid,
            timeout_seconds=1,
        )

    assert "is not in Offline state" in str(exc.value)

    assert count_instances_by_uuid(leader, instance_uuid) == 1


def test_retry_join_on_blocked_proc_raft_join(cluster: Cluster):
    cluster.deploy(instance_count=1)
    leader = cluster.instances[0]

    leader.call("pico._inject_error", "BLOCK_PROC_RAFT_JOIN", True)

    joining_instance = cluster.add_instance(wait_online=False)

    retry_lc = log_crawler(joining_instance, "join request timed out after")
    joining_instance.start()

    retry_lc.wait_matched()

    leader.call("pico._inject_error", "BLOCK_PROC_RAFT_JOIN", False)

    joining_instance.wait_online()
    joining_instance.assert_raft_status("Follower", leader_id=leader.raft_id)

    assert count_instances_by_uuid(leader, joining_instance.uuid()) == 1


def test_membership_inconsistency_at_raft_rejoin(cluster: Cluster):
    """
    1) make a cluster of one instance
    2) create follower candidate to be joined
    3) initiate raft join procedure
    4) after sending raft join from follower candidate, kill it before getting a response from leader
    5) check if follower candidate's instance uuid was noticed by a leader
    6) try to revive and rejoin killed follower candidate, but now successfully
    """
    leader_member_instance_name = "i_am_leader"
    leader_member = cluster.add_instance(name=leader_member_instance_name, wait_online=True)

    follower_candidate_instance_name = "i_am_candidate"
    follower_candidate = cluster.add_instance(name=follower_candidate_instance_name, wait_online=False)

    error_injection = "EXIT_AFTER_RPC_PROC_RAFT_JOIN"
    lc = log_crawler(follower_candidate, error_injection)

    follower_candidate.env[f"PICODATA_ERROR_INJECTION_{error_injection}"] = "1"
    follower_candidate.fail_to_start()

    # Make sure the error injection was triggered
    lc.wait_matched()

    current_cluster_members = leader_member.sql(
        """
        SELECT
            name
        FROM
            _pico_instance
        ORDER BY
            name
        """,
    )
    assert current_cluster_members == sorted([[leader_member_instance_name], [follower_candidate_instance_name]])

    leader_member_instance_uuid = leader_member.uuid()
    instance_uuid_select_result = leader_member.sql(
        """
        SELECT
            uuid
        FROM
            _pico_instance
        WHERE
            name=?
        """,
        follower_candidate_instance_name,
    )
    assert instance_uuid_select_result
    follower_candidate_instance_uuid = instance_uuid_select_result[0][0]
    assert follower_candidate_instance_uuid

    del follower_candidate.env[f"PICODATA_ERROR_INJECTION_{error_injection}"]

    # A corner case from https://git.picodata.io/core/picodata/-/issues/2077
    error_injection = "EXIT_AFTER_REBOOTSTRAP_BEFORE_STORAGE_INIT_IN_START_JOIN"
    lc = log_crawler(follower_candidate, error_injection)

    follower_candidate.env[f"PICODATA_ERROR_INJECTION_{error_injection}"] = "1"
    follower_candidate.fail_to_start()

    # Instance uuid was saved to the file
    instance_uuid_filepath = follower_candidate.instance_dir / "instance_uuid"
    instance_uuid_from_file = instance_uuid_filepath.open().read()
    assert follower_candidate_instance_uuid == instance_uuid_from_file

    # Make sure the error injection was triggered
    lc.wait_matched()

    # Remove all error injections, now the instance should successfully join
    del follower_candidate.env[f"PICODATA_ERROR_INJECTION_{error_injection}"]
    follower_candidate.start()
    follower_candidate.wait_online()

    current_cluster_members = leader_member.sql(
        """
        SELECT
            name, uuid
        FROM
            _pico_instance
        ORDER BY
            name
        """,
    )
    assert current_cluster_members == sorted(
        [
            [leader_member_instance_name, leader_member_instance_uuid],
            [follower_candidate_instance_name, follower_candidate_instance_uuid],
        ]
    )

    current_joining_raft_state = follower_candidate.eval(
        """
        return box.space._raft_state:get("join_state").value
        """
    )
    assert current_joining_raft_state == "confirm"

    # Instance uuid was removed automatically after successful boot
    assert not instance_uuid_filepath.exists()
