from typing import Generator, Protocol

from conftest import Cluster, Retriable
from packaging.version import Version as AbsoluteVersion
from framework.rolling.version import VersionAlias as Version
from framework.rolling.registry import Registry
from urllib.request import urlopen
import json

import pytest
import os
import signal


class Factory(Protocol):
    def __call__(self, of: Version, fill: bool = True) -> Cluster: ...


MINIMAL_CLUSTER_SIZE = 3
"""
Standard cluster size for rolling upgrade tests.
Three nodes provide sufficient coverage for most scenarios:
- One node can be offline while maintaining quorum.
- Mixed version states are testable.
- Common failure patterns are reproducible (heuristic).
"""


@pytest.fixture(scope="session")
def registry():
    return Registry()


@pytest.fixture(scope="class")
def factory(
    registry: Registry,
    cluster_factory,
) -> Generator[Factory, None, None]:
    created_cluster = None

    def _cluster(of: Version, fill: bool = True) -> Cluster:
        runtime = registry.get(of)

        print(
            f"upgrade_rolling: deploying cluster [version={runtime.absolute_version} ({runtime.relative_version})], data fill? {fill}"
        )

        cluster: Cluster = cluster_factory()

        cluster.registry = registry
        cluster.runtime = runtime

        cluster.deploy(instance_count=MINIMAL_CLUSTER_SIZE)

        if fill:
            cluster.pick_random_instance().fill_with_data()

        nonlocal created_cluster
        created_cluster = cluster

        return cluster

    yield _cluster

    if created_cluster is not None:
        created_cluster.kill()


@pytest.mark.xdist_group(name="rolling")
def test_node_by_node_sequential_upgrade_success(factory: Factory):
    """
    Verifies that upgrading node by node works smoothly:
    1. Start a cluster on the BEFORELAST version.
    2. Upgrade each node one at a time to the PREVIOUS version.
    3. Confirm the cluster stays healthy after all upgrades.
    4. Upgrade each node one at a time to the CURRENT version.
    5. Confirm the cluster stays healthy after all upgrades.
    """

    # step 1

    cluster = factory(of=Version.BEFORELAST_MINOR)

    # step 2

    cluster.change_version(to=Version.PREVIOUS_MINOR)

    # step 3

    assert cluster.is_healthy()

    # step 4

    cluster.change_version(to=Version.CURRENT)

    # step 5

    assert cluster.is_healthy()


@pytest.mark.xdist_group(name="rolling")
def test_node_by_node_leaping_upgrade_failure(factory: Factory):
    """
    Verifies that leap upgrading node by node fails:
    1. Start a cluster on the BEFORELAST version.
    2. Upgrade each node one at a time to the CURRENT version.
    3. Confirm the whole cluster died successfully.
    """

    # step 1

    cluster = factory(of=Version.BEFORELAST_MINOR)

    # step 2

    cluster.change_version(to=Version.CURRENT, fail=True)

    # step 3

    assert cluster.is_ceased()


@pytest.mark.xdist_group(name="rolling")
def test_successful_rollback_on_partial_upgrade_failure(factory: Factory):
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

    cluster = factory(of=Version.PREVIOUS_MINOR)

    # step 2

    shutdown_instance = cluster.pick_random_instance()
    shutdown_instance.terminate()
    assert cluster.is_healthy(exclude=[shutdown_instance])

    # step 3

    cluster.change_version(to=Version.CURRENT, exclude=[shutdown_instance])
    assert cluster.is_healthy(exclude=[shutdown_instance])

    # step 4

    shutdown_instance.start_and_wait()
    assert cluster.is_healthy()

    # step 5

    cluster.change_version(to=Version.PREVIOUS_MINOR, exclude=[shutdown_instance])

    # step 6

    assert cluster.is_healthy()


@pytest.mark.xdist_group(name="rolling")
def test_reject_older_node_joining_newer_cluster(factory: Factory):
    """
    Ensure an older node cannot rejoin a newer cluster.
    1. Start a cluster on the CURRENT version.
    2. Try to restart one node using the PREVIOUS version.
    3. Confirm that node fails to start (version too old),
       but the cluster remains healthy.
    """

    # step 1

    cluster = factory(of=Version.CURRENT)

    # step 2

    shutdown_instance = cluster.pick_random_instance()
    shutdown_instance.change_version(to=Version.PREVIOUS_MINOR, fail=True)

    # step 3

    assert shutdown_instance.is_ceased()
    assert cluster.is_healthy(exclude=[shutdown_instance])


@pytest.mark.xdist_group(name="rolling")
def test_successful_upgrade_then_failed_downgrade(factory: Factory):
    """
    Test upgrade followed by a failed downgrade.
    1. Start a cluster on the PREVIOUS version.
    2. Upgrade all nodes to the CURRENT version - confirm success.
    3. Try to restart one node as the PREVIOUS version.
    4. Confirm that the node fails to start, but the cluster stays healthy.
    """

    # step 1

    cluster = factory(of=Version.PREVIOUS_MINOR)

    # step 2

    cluster.change_version(to=Version.CURRENT)
    assert cluster.is_healthy()

    # step 3

    shutdown_instance = cluster.pick_random_instance()
    shutdown_instance.change_version(to=Version.PREVIOUS_MINOR, fail=True)

    # step 4

    assert cluster.is_healthy(exclude=[shutdown_instance])
    assert shutdown_instance.is_ceased()


@pytest.mark.xdist_group(name="rolling")
def test_upgrade_25_5_to_25_6_check_procs(factory: Factory):
    cluster = factory(of=Version.PREVIOUS_MINOR)
    cluster.change_version(to=Version.CURRENT)

    assert cluster.is_healthy()

    proc_names = ["_pico_bucket", "json_extract_path"]
    for i in cluster.instances:
        for p in proc_names:
            res = i.call("box.space._func.index.name:select", [p])
            assert res[0][2] == p


@pytest.mark.xdist_group(name="rolling")
def test_upgrade_25_5_to_25_6_check_opts(factory: Factory):
    cluster = factory(of=Version.PREVIOUS_MINOR)
    cluster.change_version(to=Version.CURRENT)

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
def test_sentinel_working_during_upgrade(
    registry: Registry,
    cluster: Cluster,
):
    cluster.registry = registry
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

    runtime_v1 = cluster.registry.get(Version.PREVIOUS_MINOR)
    cluster.runtime = runtime_v1
    leader = cluster.add_instance(wait_online=False, name="leader", tier="arbiter")
    storage_A = cluster.add_instance(wait_online=False, name="storage_A", tier="storage")
    storage_B = cluster.add_instance(wait_online=False, name="storage_B", tier="storage")
    cluster.wait_online()

    # Initial cluster version
    [[cluster_version]] = leader.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
    assert base_version(cluster_version) == str(runtime_v1.absolute_version)

    # Change auto-offline timeout to check senitnel's behaviour later
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 3")

    # Turn off one of the instances to block upgrade
    storage_B.terminate()
    leader.wait_has_states("Offline", "Offline", target=storage_B)

    # Start upgrading instances one by one
    runtime_v2 = cluster.registry.get(Version.CURRENT)

    storage_A.terminate()
    storage_A.runtime = runtime_v2
    storage_A.start()
    storage_A.wait_online()

    leader.terminate()
    leader.runtime = runtime_v2
    leader.start()
    leader.wait_online()

    # Cluster wasn't upgraded yet, because `storage_B` is not online
    [[cluster_version]] = leader.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
    assert base_version(cluster_version) == str(runtime_v1.absolute_version)

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
    storage_B.runtime = runtime_v2
    storage_B.start()
    storage_B.wait_online()

    # Now cluster is successfully upgraded
    def check():
        [[cluster_version]] = leader.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
        assert base_version(cluster_version) == str(runtime_v2.absolute_version)

    # Note: needs to be retriable because upgrade happens asynchronously after all instances become Online
    Retriable().call(check)


def base_version(v: str) -> str:
    parts = v.split("-", maxsplit=1)
    if parts:
        return parts[0]
    return v


# This test checks upgrade from 25.5.2 or lower to 25.5.3 or greater.
# We can drop it once we don't have any clients on picodata 25.5.2 or older.
@pytest.mark.xdist_group(name="rolling")
def test_ddl_catchup_by_log_during_upgrade(
    registry: Registry,
    cluster: Cluster,
):
    cluster.registry = registry
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

    runtime_v1 = cluster.registry.get(Version.PREVIOUS_MINOR)
    cluster.runtime = runtime_v1
    leader = cluster.add_instance(wait_online=False, name="leader", tier="arbiter")
    storage_A = cluster.add_instance(wait_online=False, name="storage_A", tier="storage")
    storage_B = cluster.add_instance(wait_online=False, name="storage_B", tier="storage")
    storage_C = cluster.add_instance(wait_online=False, name="storage_C", tier="storage")
    cluster.wait_online()

    # Initial cluster version
    [[cluster_version]] = leader.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
    assert base_version(cluster_version) == str(runtime_v1.absolute_version)

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

    # Start upgrading instances one by one
    runtime_v2 = cluster.registry.get(Version.CURRENT)

    # First upgrade the 2 up-to-date instances
    storage_A.terminate()
    storage_A.runtime = runtime_v2
    storage_A.start()
    storage_A.wait_online()

    leader.terminate()
    leader.runtime = runtime_v2
    leader.start()
    leader.wait_online()

    # Now let's try upgrading the lagging replicas
    storage_B.runtime = runtime_v2
    storage_C.runtime = runtime_v2
    storage_B.start()
    storage_C.start()

    storage_B.wait_online()
    storage_C.wait_online()

    # Now cluster is successfully upgraded
    def check():
        [[cluster_version]] = leader.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
        assert base_version(cluster_version) == str(runtime_v2.absolute_version)

    # Note: needs to be retriable because upgrade happens asynchronously after all instances become Online
    Retriable().call(check)


@pytest.mark.webui
@pytest.mark.xdist_group(name="rolling")
def test_webui_during_upgrade(
    registry: Registry,
    cluster: Cluster,
):
    cluster.registry = registry
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

    runtime_v1 = cluster.registry.get(Version.PREVIOUS_MINOR)
    cluster.runtime = runtime_v1
    leader = cluster.add_instance(wait_online=False, name="leader", tier="arbiter")

    http_listen = f"{cluster.base_host}:{cluster.port_distributor.get()}"
    leader.env["PICODATA_HTTP_LISTEN"] = http_listen

    storage = cluster.add_instance(wait_online=False, name="storage", tier="storage")
    cluster.wait_online()

    # Disable webui authentication for simplicity
    leader.sql("ALTER SYSTEM SET jwt_secret = ''")

    # Initial cluster version
    with urlopen(f"http://{http_listen}/api/v1/cluster") as response:
        response = json.load(response)
        assert base_version(response["currentInstaceVersion"]) == str(runtime_v1.absolute_version)

        if "clusterVersion" in response:
            assert base_version(response["clusterVersion"]) == str(runtime_v1.absolute_version)
        else:
            # On older versions it may be absent
            pass

    # Turn off one of the instances to block upgrade
    storage.terminate()
    leader.wait_has_states("Offline", "Offline", target=storage)

    # Start upgrading instances one by one
    runtime_v2 = cluster.registry.get(Version.CURRENT)

    leader.terminate()
    leader.runtime = runtime_v2
    leader.start()
    leader.wait_online()

    # Cluster wasn't upgraded yet, because `storage` is not online
    with urlopen(f"http://{http_listen}/api/v1/cluster") as response:
        response = json.load(response)
        # currentInstaceVersion is different now
        assert base_version(response["currentInstaceVersion"]) == str(runtime_v2.absolute_version)
        # but currentVersion is the same
        assert base_version(response["clusterVersion"]) == str(runtime_v1.absolute_version)

    # Finish the upgrade successfully
    storage.runtime = runtime_v2
    storage.start()
    storage.wait_online()

    # Now cluster is successfully upgraded
    def check():
        [[cluster_version]] = leader.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
        assert base_version(cluster_version) == str(runtime_v2.absolute_version)

    # Note: needs to be retriable because upgrade happens asynchronously after all instances become Online
    Retriable().call(check)

    # Cluster wasn't upgraded yet, because `storage` is not online
    with urlopen(f"http://{http_listen}/api/v1/cluster") as response:
        response = json.load(response)
        # currentInstaceVersion is different now
        assert base_version(response["currentInstaceVersion"]) == str(runtime_v2.absolute_version)
        # but currentVersion is the same
        assert base_version(response["clusterVersion"]) == str(runtime_v2.absolute_version)


@pytest.mark.xdist_group(name="rolling")
def test_expel_working_during_upgrade(
    registry: Registry,
    cluster: Cluster,
):
    cluster.registry = registry
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

    runtime_v1 = cluster.registry.get(Version.PREVIOUS_MINOR)
    cluster.runtime = runtime_v1
    leader = cluster.add_instance(wait_online=False, name="leader", tier="arbiter")
    storage_A = cluster.add_instance(wait_online=False, name="storage_A", tier="storage")
    storage_B = cluster.add_instance(wait_online=False, name="storage_B", tier="storage")
    cluster.wait_online()

    # Initial cluster version
    [[cluster_version]] = leader.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
    assert base_version(cluster_version) == str(runtime_v1.absolute_version)

    # Change auto-offline timeout to check senitnel's behaviour later
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 3")

    # Turn off one of the instances to block upgrade
    storage_B.terminate()
    leader.wait_has_states("Offline", "Offline", target=storage_B)

    # Start upgrading instances one by one
    runtime_v2 = cluster.registry.get(Version.CURRENT)

    storage_A.terminate()
    storage_A.runtime = runtime_v2
    storage_A.start()
    storage_A.wait_online()

    leader.terminate()
    leader.runtime = runtime_v2
    leader.start()
    leader.wait_online()

    # Cluster wasn't upgraded yet, because `storage_B` is not online
    [[cluster_version]] = leader.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
    assert base_version(cluster_version) == str(runtime_v1.absolute_version)

    # Expel the last instance which is currently blocking the upgrade
    cluster.expel(target=storage_B)

    # Now cluster is successfully upgraded
    def check():
        [[cluster_version]] = leader.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
        assert base_version(cluster_version) == str(runtime_v2.absolute_version)

    # Note: needs to be retriable because upgrade happens asynchronously after all instances become Online
    Retriable().call(check)


@pytest.mark.xdist_group(name="rolling")
def test_25_4_1_broken_pico_tier_migration(registry: Registry, cluster: Cluster):
    # This test validates the fix for https://git.picodata.io/core/picodata/-/issues/2683
    # For this we find a picodata version before the `26.1.0` migration that ought to fix it,
    #  and modify `_pico_tier` to simulate the breakage.
    # When the bug happens, `_pico_tier` contains a single tuple that has `is_default = false`.
    # The test checks that the 26.1.0 migration will fix it and set `is_default = true`.
    # It is expected that this test will stop working after 25.5.x stops being a valid upgrade starting point.
    # NB: consider removing this test once 26.1.x branch will stop being updated

    cluster.registry = registry
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        tier_name:
        """
    )

    # FIXME: refactor code after <https://git.picodata.io/core/picodata/-/merge_requests/2677> gets merged
    # Find a version before the 26.1.0 fix
    runtime_v1 = None
    for version in [Version.PREVIOUS_MINOR, Version.BEFORELAST_MINOR]:
        registry_item = cluster.registry.get(version)
        if registry_item.absolute_version < AbsoluteVersion("26.0.0"):
            runtime_v1 = registry_item
            break
    if runtime_v1 is None:
        pytest.skip(
            "Could not find a version which doesn't have the migration fix "
            "between versions available in the version registry via aliases. Can't test this"
        )
    print(f"Found version to use: {runtime_v1.relative_version} ({runtime_v1.absolute_version})")

    runtime_v2 = cluster.registry.get(Version.CURRENT)

    if runtime_v2.absolute_version >= AbsoluteVersion("26.2.0"):
        pytest.skip("This test is only relevant when updating from 25.5.x to 26.1.x. Current version is >= 26.2.0")

    cluster.runtime = runtime_v1
    instance = cluster.add_instance(name="tier_name_1_1", tier="tier_name")

    [[cluster_version]] = instance.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
    assert base_version(cluster_version) == str(runtime_v1.absolute_version)

    # This version should not have the bug
    [[tier_is_default]] = instance.sql("SELECT is_default FROM _pico_tier")
    assert tier_is_default is True

    # Simulate the broken catalog by modifying `_pico_tier`.
    # The same result can be obtained by creating a cluster in 25.3.8, and then updating it to 25.4.4 and to 25.5.6,
    #  but is not possible with our current upgrade testing framework.
    instance.sql("UPDATE _pico_tier SET is_default = false")

    # Now we should have the broken catalog, as if after applying the buggy 25.4.1 migration
    [[tier_is_default]] = instance.sql("SELECT is_default FROM _pico_tier")
    assert tier_is_default is False

    instance.terminate()

    instance.runtime = runtime_v2
    instance.start()
    instance.wait_online()

    [[cluster_version]] = instance.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
    assert base_version(cluster_version) == str(runtime_v2.absolute_version)

    # The migration should have fixed the missing default tier
    [[tier_is_default]] = instance.sql("SELECT is_default FROM _pico_tier")
    assert tier_is_default is True
