from conftest import Cluster
from conftest import Retriable
from framework.registry import get_or_make_registry
from framework.registry import Registry
from framework.util.version import base_version
from framework.util.version import ExecutableVersion
from framework.util.version import parse_version_exc
from framework.util.version import VersionAlias
from packaging.version import Version
from urllib.request import urlopen
import json
import pytest
import os
import signal


def assert_version(
    cluster: Cluster,
    version: ExecutableVersion,
    registry: Registry,
):
    executable = registry.get(version)
    assert executable is not None

    # Sometimes, a cluster version may not be clean, i.e. after an
    # upgrade, it may have a postfix component like `25.6.0.post277`.
    # This is fine in most cases, thus we do not want to raise here.
    lhs = base_version(executable.version)
    rhs = base_version(cluster.version())
    assert lhs == rhs


@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        VersionAlias.PREVIOUS_MINOR,
        VersionAlias.CURRENT,
    ]
)
def test_upgrade_from_previous_minor_to_current(cluster: Cluster, registry: Registry):
    executable = registry.get_or_skip(VersionAlias.PREVIOUS_MINOR)

    cluster.deploy(
        executable=executable,
        instance_count=4,
        init_replication_factor=2,
    )
    cluster.fill_with_data()

    assert cluster.is_healthy()

    current = registry.get(VersionAlias.CURRENT)
    assert current is not None

    cluster.change_executable(current)

    assert cluster.is_healthy()


@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        Version("25.5.9"),
        get_or_make_registry().next_version(Version("25.5.9")),
    ]
)
def test_upgrade_from_previous_major_to_current(cluster: Cluster, registry: Registry):
    from_version = Version("25.5.9")
    from_executable = registry.get(from_version)
    assert from_executable is not None
    cluster.deploy(
        executable=from_executable,
        instance_count=4,
        init_replication_factor=2,
    )
    cluster.fill_with_data()

    assert cluster.is_healthy()

    to_version = registry.next_version(from_version)
    to_executable = registry.get(to_version)
    assert to_executable is not None

    cluster.change_executable(to_executable)

    assert cluster.is_healthy()


@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        VersionAlias.BEFORELAST_MINOR,
        VersionAlias.PREVIOUS_MINOR,
        VersionAlias.CURRENT,
    ]
)
def test_node_by_node_sequential_upgrade_success(cluster: Cluster, registry: Registry):
    """
    Verifies that upgrading node by node works smoothly:
    1. Start a cluster on the BEFORELAST version.
    2. Upgrade each node one at a time to the PREVIOUS version.
    3. Confirm the cluster stays healthy after all upgrades.
    4. Upgrade each node one at a time to the CURRENT version.
    5. Confirm the cluster stays healthy after all upgrades.
    """

    # step 1

    executable = registry.get_or_skip(VersionAlias.BEFORELAST_MINOR)

    cluster.deploy(
        executable=executable,
        instance_count=4,
        init_replication_factor=2,
    )
    cluster.fill_with_data()

    # step 2

    executable = registry.get_or_skip(VersionAlias.PREVIOUS_MINOR)

    cluster.change_executable(executable)

    # step 3

    assert cluster.is_healthy()

    # step 4

    current = registry.get(VersionAlias.CURRENT)
    assert current is not None
    cluster.change_executable(current)

    # step 5

    assert cluster.is_healthy()


@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        VersionAlias.BEFORELAST_MINOR,
        VersionAlias.CURRENT,
    ]
)
def test_node_by_node_leaping_upgrade_failure(cluster: Cluster, registry: Registry):
    """
    Verifies that leap upgrading node by node fails:
    1. Start a cluster on the BEFORELAST version.
    2. Upgrade each node one at a time to the CURRENT version.
    3. Confirm the whole cluster died successfully.
    """

    # step 1

    executable = registry.get_or_skip(VersionAlias.BEFORELAST_MINOR)

    cluster.deploy(
        executable=executable,
        instance_count=4,
        init_replication_factor=2,
    )
    cluster.fill_with_data()

    # step 2

    current = registry.get(VersionAlias.CURRENT)
    assert current is not None
    cluster.change_executable(current, fail=True)

    # step 3

    assert cluster.is_ceased()


@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        VersionAlias.PREVIOUS_MINOR,
        VersionAlias.CURRENT,
    ]
)
def test_successful_rollback_on_partial_upgrade_failure(cluster: Cluster, registry: Registry):
    """
    Checks that a partial upgrade can be safely rolled back:
    1. Start a cluster on the PREVIOUS version.
    2. Shut down one node.
    3. Upgrade the other two to the CURRENT version.
    4. Bring the old node back online.
    5. Roll the upgraded nodes back to the PREVIOUS version.
    6. Confirm that no full upgrade happened, but the cluster
       stayed healthy.
    """

    # step 1

    executable = registry.get_or_skip(VersionAlias.PREVIOUS_MINOR)

    cluster.deploy(
        executable=executable,
        instance_count=4,
        init_replication_factor=2,
    )
    cluster.fill_with_data()

    # step 2

    shutdown_instance = cluster.pick_random_instance()
    shutdown_instance.terminate()
    assert cluster.is_healthy(exclude=[shutdown_instance])

    # step 3

    current = registry.get(VersionAlias.CURRENT)
    assert current is not None

    for instance in cluster.instances:
        if instance is not shutdown_instance:
            instance.change_executable(current)

    assert cluster.is_healthy(exclude=[shutdown_instance])

    # step 4

    executable = registry.get_or_skip(VersionAlias.PREVIOUS_MINOR)

    for instance in cluster.instances:
        if instance is not shutdown_instance:
            instance.change_executable(executable)

    assert cluster.is_healthy(exclude=[shutdown_instance])

    # step 5

    shutdown_instance.start_and_wait()

    # step 6

    assert cluster.is_healthy()


@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        VersionAlias.PREVIOUS_MINOR,
        VersionAlias.CURRENT,
    ]
)
def test_reject_older_node_joining_newer_cluster(cluster: Cluster, registry: Registry):
    """
    Ensure an older node cannot rejoin a newer cluster.
    1. Start a cluster on the CURRENT version.
    2. Try to restart one node using the PREVIOUS version.
    3. Confirm that node fails to start (version too old),
       but the cluster remains healthy.
    """

    # step 1

    executable = registry.get(VersionAlias.CURRENT)
    assert executable is not None

    cluster.deploy(
        executable=executable,
        instance_count=4,
        init_replication_factor=2,
    )
    cluster.fill_with_data()

    # step 2

    executable = registry.get_or_skip(VersionAlias.PREVIOUS_MINOR)

    shutdown_instance = cluster.pick_random_instance()
    shutdown_instance.change_executable(executable, fail=True)

    # step 3

    assert shutdown_instance.is_ceased()
    assert cluster.is_healthy(exclude=[shutdown_instance])


@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        VersionAlias.PREVIOUS_MINOR,
        VersionAlias.CURRENT,
    ]
)
def test_successful_upgrade_then_failed_downgrade(cluster: Cluster, registry: Registry):
    """
    Test upgrade followed by a failed downgrade.
    1. Start a cluster on the PREVIOUS version.
    2. Upgrade all nodes to the CURRENT version - confirm success.
    3. Try to restart one node as the PREVIOUS version.
    4. Confirm that the node fails to start, but the cluster stays healthy.
    """

    # step 1

    executable = registry.get_or_skip(VersionAlias.PREVIOUS_MINOR)

    cluster.deploy(
        executable=executable,
        instance_count=4,
        init_replication_factor=2,
    )
    cluster.fill_with_data()

    # step 2

    current = registry.get(VersionAlias.CURRENT)
    assert current is not None
    cluster.change_executable(current)
    assert cluster.is_healthy()

    # step 3

    executable = registry.get_or_skip(VersionAlias.PREVIOUS_MINOR)

    shutdown_instance = cluster.pick_random_instance()
    shutdown_instance.change_executable(executable, fail=True)

    # step 4

    assert cluster.is_healthy(exclude=[shutdown_instance])
    assert shutdown_instance.is_ceased()


@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        Version("25.5.1"),
        Version("25.5.9"),
    ]
)
def test_upgrade_25_5_to_25_6_check_procs(cluster: Cluster, registry: Registry):
    from_version = Version("25.5.1")
    from_executable = registry.get(from_version)
    assert from_executable is not None

    to_version = Version("25.5.9")
    to_executable = registry.get(to_version)
    assert to_executable is not None

    procedures = ["json_extract_path"]

    cluster.deploy(
        executable=from_executable,
        instance_count=4,
        init_replication_factor=2,
    )
    assert cluster.is_healthy()

    for instance in cluster.instances:
        for procedure in procedures:
            result = instance.call("box.space._func.index.name:select", [procedure])
            assert result == []

    cluster.change_executable(to_executable)
    assert cluster.is_healthy()

    for instance in cluster.instances:
        for procedure in procedures:
            result = instance.call("box.space._func.index.name:select", [procedure])
            assert result[0][2] == procedure


@pytest.mark.skip(reason="skipped temporarily after version bump to 26.2.0")
@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        Version("25.5.9"),
        get_or_make_registry().next_version(Version("25.5.9")),
    ]
)
def test_upgrade_unlogged_tables_existence(cluster: Cluster, registry: Registry):
    from_version = Version("25.5.9")
    from_executable = registry.get(from_version)
    assert from_executable is not None

    to_version = registry.next_version(from_version)
    to_executable = registry.get(to_version)
    assert to_executable is not None

    cluster.deploy(
        executable=from_executable,
        instance_count=4,
        init_replication_factor=2,
    )
    assert cluster.is_healthy()

    cluster.change_executable(to_executable)
    assert cluster.is_healthy()

    i = cluster.instances[0]
    res = i.sql("SELECT name, opts FROM _pico_table")
    for name, opts in res:
        if name == "_pico_table":
            assert opts == []
        else:
            assert opts is None

    i.sql("CREATE UNLOGGED TABLE t (a INT PRIMARY KEY)")
    res = i.sql("SELECT name, opts FROM _pico_table")
    for name, opts in res:
        if name == "_pico_table":
            assert opts == []
        elif name == "t":
            assert opts == [dict([("unlogged", [True])])]  # type: ignore
        else:
            assert opts is None


@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        VersionAlias.PREVIOUS_MINOR,
        VersionAlias.CURRENT,
    ]
)
def test_sentinel_working_during_upgrade(cluster: Cluster, registry: Registry):
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        arbiter:
            can_vote: true
        storage:
            can_vote: false
            replication_factor: 2
        """
    )

    executable = registry.get_or_skip(VersionAlias.PREVIOUS_MINOR)

    # fmt: off
    leader = cluster.add_instance(wait_online=False, name="leader", tier="arbiter", executable=executable)
    storage_A = cluster.add_instance(wait_online=False, name="storage_A", tier="storage", executable=executable)
    storage_B = cluster.add_instance(wait_online=False, name="storage_B", tier="storage", executable=executable)
    cluster.wait_online()
    # fmt: on

    # Check initial cluster version
    assert_version(cluster, VersionAlias.PREVIOUS_MINOR, registry)

    # Change auto-offline timeout to check senitnel's behaviour later
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 3")

    # Turn off one of the instances to block upgrade
    storage_B.terminate()
    leader.wait_has_states("Offline", "Offline", target=storage_B)

    # Start upgrading instances one by one
    current = registry.get(VersionAlias.CURRENT)
    assert current is not None

    for instance in [storage_A, leader]:
        instance.change_executable(current)

    # Cluster wasn't upgraded yet, because `storage_B` is not online
    assert_version(cluster, VersionAlias.PREVIOUS_MINOR, registry)

    # Make sure graceful shutdown works during upgrade
    storage_A.terminate()
    leader.wait_has_states("Offline", "Offline", target=storage_A)
    storage_A.start()
    storage_A.wait_online()

    # Make sure sentinel successfully handles non-graceful shutdown (auto-offline)
    assert storage_A.process
    os.killpg(storage_A.process.pid, signal.SIGSTOP)
    leader.wait_has_states("Offline", "Offline", target=storage_A)

    # Make sure sentinel successfully handles wake-up after auto-offline
    os.killpg(storage_A.process.pid, signal.SIGCONT)
    leader.wait_has_states("Online", "Online", target=storage_A)

    # Finish the upgrade successfully as a sanity check
    storage_B.executable = current
    storage_B.start_and_wait()

    # Now cluster is successfully upgraded
    # NOTE: Needs to be retriable because upgrade happens asynchronously after all instances become Online.
    Retriable().call(assert_version, cluster, VersionAlias.CURRENT, registry)


# This test checks upgrade from 25.5.2 or lower to 25.5.3 or greater.
# We can drop it once we don't have any clients on picodata 25.5.2 or older.
@pytest.mark.required_rolling_versions(
    versions=[
        Version("25.5.2"),
        Version("25.5.7"),
    ]
)
@pytest.mark.xdist_group(name="rolling")
def test_ddl_catchup_by_log_during_upgrade(cluster: Cluster, registry: Registry):
    initial_version = Version("25.5.2")
    future_version = Version("25.5.7")

    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        arbiter:
            can_vote: true
        storage:
            can_vote: false
            replication_factor: 3
        """
    )
    executable = registry.get(initial_version)
    assert executable is not None

    # fmt: off
    leader = cluster.add_instance(wait_online=False, name="leader", tier="arbiter", executable=executable)
    storage_A = cluster.add_instance(wait_online=False, name="storage_A", tier="storage", executable=executable)
    storage_B = cluster.add_instance(wait_online=False, name="storage_B", tier="storage", executable=executable)
    storage_C = cluster.add_instance(wait_online=False, name="storage_C", tier="storage", executable=executable)
    cluster.wait_online()
    # fmt: on

    # Check initial cluster version
    assert_version(cluster, initial_version, registry)

    # Change auto-offline timeout to check senitnel's behaviour later
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 3")

    # Turn off the replicas to block upgrade and to check how they handle
    # applying DdlCommit during catch-up
    storage_B.terminate()
    storage_C.terminate()
    leader.wait_has_states("Offline", "Offline", target=storage_B)
    leader.wait_has_states("Offline", "Offline", target=storage_C)

    # Introduce a DDL operation
    leader.sql("CREATE TABLE my_bass (id INT PRIMARY KEY, value TEXT) DISTRIBUTED GLOBALLY WAIT APPLIED LOCALLY")
    storage_A.raft_wait_index(leader.raft_get_index())

    # First upgrade the 2 up-to-date instances
    executable = registry.get(future_version)
    assert executable is not None

    for instance in [storage_A, leader]:
        instance.change_executable(executable)

    # Now let's try upgrading the lagging replicas
    for instance in [storage_B, storage_C]:
        instance.executable = executable
        instance.start_and_wait()

    # Now cluster is successfully upgraded
    # NOTE: needs to be retriable because upgrade happens asynchronously after all instances become Online
    Retriable().call(assert_version, cluster, future_version, registry)


@pytest.mark.webui
@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        VersionAlias.PREVIOUS_MINOR,
        VersionAlias.CURRENT,
    ]
)
def test_webui_during_upgrade(cluster: Cluster, registry: Registry):
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        arbiter:
            can_vote: true
        storage:
            can_vote: false
            replication_factor: 2
        """
    )

    executable = registry.get_or_skip(VersionAlias.PREVIOUS_MINOR)

    leader = cluster.add_instance(wait_online=False, name="leader", tier="arbiter", executable=executable)

    http_listen = leader.http_listen

    # fmt: off
    storage = cluster.add_instance(wait_online=False, name="storage", tier="storage", executable=executable)
    # fmt: on

    cluster.wait_online()
    assert_version(cluster, VersionAlias.PREVIOUS_MINOR, registry)

    # Disable webui authentication for simplicity
    leader.sql("ALTER SYSTEM SET jwt_secret = ''")

    # Check initial cluster version
    with urlopen(f"http://{http_listen}/api/v1/cluster") as response:
        response = json.load(response)

        webui_leader_version = parse_version_exc(response["currentInstaceVersion"])
        webui_leader_executable = registry.get(base_version(webui_leader_version))
        assert webui_leader_executable is not None

        # The `currentInstaceVersion` is the same...
        assert leader.executable.version == webui_leader_executable.version

        # ...as well as the `clusterVersion`
        if "clusterVersion" in response:  # NOTE: on older versions it may be absent.
            webui_cluster_version = parse_version_exc(response["clusterVersion"])
            assert webui_cluster_version == cluster.version()

    # Turn off one of the instances to block upgrade
    storage.terminate()
    leader.wait_has_states("Offline", "Offline", target=storage)

    # Start upgrading instances one by one
    current = registry.get(VersionAlias.CURRENT)
    assert current is not None

    leader.change_executable(current)

    # Cluster wasn't upgraded yet, because `storage` is not online
    with urlopen(f"http://{http_listen}/api/v1/cluster") as response:
        response = json.load(response)

        webui_cluster_version = parse_version_exc(response["clusterVersion"])
        webui_leader_version = parse_version_exc(response["currentInstaceVersion"])
        webui_leader_executable = registry.get(base_version(webui_leader_version))
        assert webui_leader_executable is not None

        # The `currentInstaceVersion` is different now...
        webui_leader_executable_version = base_version(webui_leader_executable.version)
        assert leader.executable.version == webui_leader_executable_version

        # ...but the `clusterVersion` is the same.
        real_cluster_version = cluster.version()
        assert webui_cluster_version == real_cluster_version

    # Finish the upgrade successfully
    storage.executable = current
    storage.start_and_wait()

    # Now cluster is successfully upgraded
    # NOTE: Needs to be retriable because upgrade happens asynchronously after all instances become Online.
    Retriable().call(assert_version, cluster, VersionAlias.CURRENT, registry)
    with urlopen(f"http://{http_listen}/api/v1/cluster") as response:
        response = json.load(response)

        webui_leader_version = parse_version_exc(response["currentInstaceVersion"])
        webui_cluster_version = parse_version_exc(response["clusterVersion"])
        webui_leader_executable = registry.get(base_version(webui_leader_version))
        assert webui_leader_executable is not None

        # The `currentInstaceVersion` is the same...
        webui_leader_executable_version = base_version(webui_leader_executable.version)
        assert leader.executable.version == webui_leader_executable.version

        # ...as well as the `clusterVersion`.
        real_cluster_version = cluster.version()
        assert webui_cluster_version == real_cluster_version


@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        VersionAlias.PREVIOUS_MINOR,
        VersionAlias.CURRENT,
    ]
)
def test_expel_working_during_upgrade(cluster: Cluster, registry: Registry):
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        arbiter:
            can_vote: true
        storage:
            can_vote: false
            replication_factor: 2
        """
    )

    executable = registry.get_or_skip(VersionAlias.PREVIOUS_MINOR)

    # fmt: off
    leader = cluster.add_instance(wait_online=False, name="leader", tier="arbiter", executable=executable)
    storage_A = cluster.add_instance(wait_online=False, name="storage_A", tier="storage", executable=executable)
    storage_B = cluster.add_instance(wait_online=False, name="storage_B", tier="storage", executable=executable)
    cluster.wait_online()
    # fmt: on

    # Check initial cluster version
    assert_version(cluster, VersionAlias.PREVIOUS_MINOR, registry)

    # Change auto-offline timeout to check senitnel's behaviour later
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 3")

    # Turn off one of the instances to block upgrade
    storage_B.terminate()
    leader.wait_has_states("Offline", "Offline", target=storage_B)

    # Start upgrading instances one by one
    current = registry.get(VersionAlias.CURRENT)
    assert current is not None

    for instance in [storage_A, leader]:
        instance.change_executable(current)

    # Cluster wasn't upgraded yet, because `storage_B` is not online
    assert_version(cluster, VersionAlias.PREVIOUS_MINOR, registry)

    # Expel the last instance which is currently blocking the upgrade
    cluster.expel(target=storage_B)

    # Now cluster is successfully upgraded
    # NOTE: Needs to be retriable because upgrade happens asynchronously after all instances become Online.
    Retriable().call(assert_version, cluster, VersionAlias.CURRENT, registry)


@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        Version("25.3.8"),
        Version("25.4.1"),
        get_or_make_registry().next_version(Version("25.4.1")),
        get_or_make_registry().next_version(Version("25.5.7")),
    ]
)
def test_25_4_1_broken_pico_tier_migration(cluster: Cluster, registry: Registry):
    # This test validates the fix for https://git.picodata.io/core/picodata/-/issues/2683
    # For this we find a picodata version before the `26.1.0` migration that ought to fix it,
    #  and modify `_pico_tier` to simulate the breakage.
    # When the bug happens, `_pico_tier` contains a single tuple that has `is_default = false`.
    # The test checks that the 26.1.0 migration will fix it and set `is_default = true`.
    # It is expected that this test will stop working after 25.5.x stops being a valid upgrade starting point.
    # NB: consider removing this test once 26.1.x branch will stop being updated

    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        tier_name:
        """
    )

    # Deploy a single-node cluster before migration introduction.
    executable = registry.get(Version("25.3.8"))
    assert executable is not None

    instance = cluster.add_instance(name="tier_name_1_1", tier="tier_name", executable=executable)
    assert_version(cluster, Version("25.3.8"), registry)

    # Upgrade to a version with the expected missing default tier bug.
    executable = registry.get(Version("25.4.1"))
    assert executable is not None

    cluster.change_executable(executable)
    assert_version(cluster, Version("25.4.1"), registry)

    # This version should have this bug.
    [[tier_is_default]] = instance.sql("SELECT is_default FROM _pico_tier")
    assert tier_is_default is False

    # Transitively upgrade to a version with a fix.
    next_version = registry.next_version(Version("25.4.1"))
    executable = registry.get(next_version)
    assert executable is not None

    cluster.change_executable(executable)
    assert_version(cluster, next_version, registry)

    next_version = registry.next_version(Version("25.5.7"))
    executable = registry.get(next_version)
    assert executable is not None

    cluster.change_executable(executable)
    assert_version(cluster, next_version, registry)

    # The migration should have fixed the missing default tier bug.
    [[tier_is_default]] = instance.sql("SELECT is_default FROM _pico_tier")
    assert tier_is_default is True
