from conftest import Cluster
from conftest import Instance
from conftest import log_crawler
from conftest import Retriable
from framework.registry import get_or_make_registry
from framework.registry import Registry
from framework.util.version import base_version
from framework.util.version import ExecutableVersion
from framework.util.version import parse_version_exc
from framework.util.version import VersionAlias
from framework.util import ExpectedError
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
    cluster.check_health()

    current = registry.get(VersionAlias.CURRENT)
    assert current is not None

    cluster.change_executable(current)

    cluster.check_health()


@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        Version("25.5.9"),
        get_or_make_registry().next_version(Version("25.5.9"), skip_on_gap=False),
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
    cluster.check_health()

    to_version = registry.next_version(from_version)
    to_executable = registry.get(to_version)
    assert to_executable is not None

    cluster.change_executable(to_executable)

    cluster.check_health()


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
    cluster.check_health()

    # step 2

    executable = registry.get_or_skip(VersionAlias.PREVIOUS_MINOR)

    cluster.change_executable(executable)
    # step 3

    cluster.check_health()

    # step 4

    current = registry.get(VersionAlias.CURRENT)
    assert current is not None
    cluster.change_executable(current)
    # step 5

    cluster.check_health()


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

    2. Upgrade all but the last node to the CURRENT version
       and verify that the old-version leader rejects them.

    3. Upgrade the last old-version node and verify that it
       cannot activate without any live peers.

    4. Confirm the whole cluster died successfully.
    """

    executable = registry.get_or_skip(VersionAlias.BEFORELAST_MINOR)

    cluster.deploy(
        executable=executable,
        instance_count=4,
        init_replication_factor=2,
    )
    cluster.check_health()

    current = registry.get(VersionAlias.CURRENT)
    assert current is not None

    # Keep the last old-version instance alive so that every instance
    # upgraded in this loop can reach a peer which rejects the leap
    # upgrade with a version mismatch. Each rejected current-version
    # process exits and leaves one fewer live peer in the cluster.
    mismatch_pattern = "mismatches the leader's version"
    version_mismatch = ExpectedError(log_pattern=mismatch_pattern)
    for instance in cluster.instances[:-1]:
        # `Instance.fail_to_start` accepts any quick non-zero process exit
        # without checking its `ExpectedError` log pattern, so verify the
        # actual reason for the rejection with a dedicated crawler.
        mismatch_crawler = log_crawler(instance, mismatch_pattern)
        instance.change_executable(current, version_mismatch)
        mismatch_crawler.wait_matched()

    # Only one old-version instance remains alive. Once it is stopped for the
    # upgrade, there is no peer left to reject it with a version mismatch.
    # The restarted process instead keeps trying to activate through dead
    # peers; `Instance.fail_to_start` records the connection error, waits
    # for its startup timeout, and then kills the process.
    no_live_peers = ExpectedError(log_pattern="failed to activate myself: failed to connect")
    cluster.instances[-1].change_executable(current, no_live_peers)

    # All current-version processes either exited after the version
    # rejection or were killed after failing to reach a live peer.
    running_instances = [
        f"{instance.name} (pid={instance.process.pid})"
        for instance in cluster.instances
        if instance.process is not None and instance.process.poll() is None
    ]
    assert not running_instances, f"expected the whole cluster to stop, still running: {running_instances}"


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
    cluster.check_health()

    # step 2

    shutdown_instance = cluster.pick_random_instance()
    shutdown_instance.terminate()

    cluster.check_health(exclude=[shutdown_instance])

    # step 3

    current = registry.get(VersionAlias.CURRENT)
    assert current is not None

    for instance in cluster.instances:
        if instance is not shutdown_instance:
            instance.change_executable(current)

    cluster.check_health(exclude=[shutdown_instance], homogeneous=False)

    # step 4

    executable = registry.get_or_skip(VersionAlias.PREVIOUS_MINOR)

    for instance in cluster.instances:
        if instance is not shutdown_instance:
            instance.change_executable(executable)

    cluster.check_health(exclude=[shutdown_instance])

    # step 5

    shutdown_instance.start_and_wait()

    # step 6

    cluster.check_health()


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
    cluster.check_health()

    # step 2

    executable = registry.get_or_skip(VersionAlias.PREVIOUS_MINOR)

    error = ExpectedError(log_pattern="mismatches the leader's version")
    shutdown_instance = cluster.pick_random_instance()
    shutdown_instance.change_executable(executable, error)

    # step 3

    cluster.check_health(exclude=[shutdown_instance])


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
    cluster.check_health()

    # step 2

    current = registry.get(VersionAlias.CURRENT)
    assert current is not None

    cluster.change_executable(current)

    cluster.check_health()
    # step 3

    executable = registry.get_or_skip(VersionAlias.PREVIOUS_MINOR)

    error = ExpectedError(log_pattern="mismatches the leader's version")
    shutdown_instance = cluster.pick_random_instance()
    shutdown_instance.change_executable(executable, error)

    # step 4

    cluster.check_health(exclude=[shutdown_instance])


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


# Changes to the bootstrap schema and data must have matching upgrade
# operations. Otherwise, an upgraded cluster may silently end up with
# a different state than a cluster bootstrapped directly on the same
# version, which might lead to an unpredictable cluster behavior.
#
# See <https://git.picodata.io/core/picodata/issues/2998>.
@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        VersionAlias.PREVIOUS_MINOR,
        VersionAlias.CURRENT,
    ]
)
def test_upgraded_schema_matches_fresh_cluster(
    cluster: Cluster,
    registry: Registry,
):
    def dump_bootstrap_state(instance: Instance):
        """
        Dump schema catalogs and non-topology bootstrap rows. The test captures
        then this state after an upgrade and a fresh bootstrap. After that, it
        compares each catalog to detect bootstrap changes without matching upgrades.
        """
        return instance.eval(
            """
            -- Define the catalogs to dump and the fields
            -- which uniquely identify a row in each catalog.
            local catalog_key_fields = {
                _space = { "name" },
                _index = { "space_id", "name" },
                _func = { "name" },
                _pico_table = { "name" },
                _pico_index = { "table_id", "name" },
                _pico_routine = { "name" },
                _pico_property = { "key" },
                _pico_db_config = { "key", "scope" },
                _pico_user = { "name" },
                _pico_privilege = {
                    "grantee_id", "object_type", "object_id", "privilege",
                },
            }

            -- Define fields to remove because they depend on
            -- creation order or the path taken to the state.
            local path_dependent_fields = {
                _func = { id = true, created = true, last_altered = true },
                _pico_table = { schema_version = true },
                _pico_index = { schema_version = true },
                _pico_routine = { schema_version = true },
                _pico_user = { schema_version = true },
                _pico_privilege = { schema_version = true },
            }

            -- Define generated or path-dependent values to remove while
            -- retaining their rows, so the presence of entries are checked.
            local keys_to_remove = {
                _pico_property = {
                    global_schema_version = true,
                    next_schema_version = true,
                },
                _pico_db_config = { jwt_secret = true },
            }
            local bootstrap_state = {}

            -- Convert every tuple to a map and normalize
            -- the fields and values defined above.
            for catalog_name, key_field_names in pairs(catalog_key_fields) do
                local catalog_rows = {}
                local fields_to_remove = path_dependent_fields[catalog_name] or {}
                for _, tuple in box.space[catalog_name]:pairs() do
                    local normalized_row = tuple:tomap({ names_only = true })
                    for field_name in pairs(fields_to_remove) do
                        normalized_row[field_name] = nil
                    end

                    local catalog_value_keys = keys_to_remove[catalog_name] or {}
                    if catalog_value_keys[normalized_row.key] then
                        normalized_row.value = nil
                    end

                    -- Build a stable key from the catalog's unique fields,
                    -- allowing the resulting maps to be compared in Python.
                    local key_parts = {}
                    for _, field_name in ipairs(key_field_names) do
                        table.insert(key_parts, tostring(normalized_row[field_name]))
                    end
                    local row_key = table.concat(key_parts, ":")
                    catalog_rows[row_key] = normalized_row
                end
                bootstrap_state[catalog_name] = catalog_rows
            end

            return bootstrap_state
            """
        )

    previous = registry.get_or_skip(VersionAlias.PREVIOUS_MINOR)
    current = registry.get(VersionAlias.CURRENT)
    assert current is not None

    # XXX: `Cluster.deploy` overrides `sql_ddl_timeout` with the `pytest`'s timeout.
    # That would make both clusters equal even if a changed bootstrap default had no
    # matching upgrade operation, so keep the bootstrap value intact in both clusters.
    cluster.pytest_timeout = None

    # Bootstrap on the previous version and capture the schema after upgrade.
    cluster.deploy(executable=previous, instance_count=1)
    cluster.change_executable(current)
    upgraded_state = dump_bootstrap_state(cluster.leader())

    # Reuse the cluster fixture to bootstrap a clean cluster on the current version.
    cluster.reset()
    os.makedirs(cluster.data_dir)  # XXX: `Cluster.reset` removes the data directory.
    cluster.deploy(executable=current, instance_count=1)
    fresh_state = dump_bootstrap_state(cluster.leader())

    # Check catalogs separately so pytest reports the mismatching catalog.
    for catalog_name in upgraded_state:
        assert upgraded_state[catalog_name] == fresh_state[catalog_name], catalog_name


def read_parameter(instance: Instance, key: str):
    """
    Read a global-scope parameter from `_pico_db_config`, or `None` if the
    cluster has no row for it.
    """
    res = instance.sql("SELECT value FROM _pico_db_config WHERE key = ?", key)
    if not res:
        return None
    assert len(res) == 1, res
    return res[0][0]


# The 26.3.1 catalog upgrade adds `governor_ddl_rpc_timeout` and raises the
# default of `governor_common_rpc_timeout` from 3.0 to 7.0.
@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        VersionAlias.PREVIOUS_MINOR,
        VersionAlias.CURRENT,
    ]
)
def test_upgrade_governor_rpc_timeouts(cluster: Cluster, registry: Registry):
    previous = registry.get_or_skip(VersionAlias.PREVIOUS_MINOR)
    current = registry.get(VersionAlias.CURRENT)
    assert current is not None

    cluster.deploy(executable=previous, instance_count=1)
    i1 = cluster.leader()

    # A cluster bootstrapped before the upgrade has no row for the ddl timeout
    # and holds the old common timeout default.
    assert read_parameter(i1, "governor_ddl_rpc_timeout") is None
    assert read_parameter(i1, "governor_common_rpc_timeout") == 3.0

    cluster.change_executable(current)
    i1 = cluster.leader()

    assert read_parameter(i1, "governor_ddl_rpc_timeout") == 30.0
    assert read_parameter(i1, "governor_common_rpc_timeout") == 7.0

    # Now the same upgrade on a cluster whose common timeout was tuned away from
    # the old default. Only the exact old default counts as untouched, so the
    # tuned value must survive.
    cluster.reset()
    os.makedirs(cluster.data_dir)  # XXX: `Cluster.reset` removes the data directory.
    cluster.deploy(executable=previous, instance_count=1)
    i1 = cluster.leader()

    i1.sql("ALTER SYSTEM SET governor_common_rpc_timeout = 5", sudo=True)

    cluster.change_executable(current)
    i1 = cluster.leader()

    assert read_parameter(i1, "governor_ddl_rpc_timeout") == 30.0
    assert read_parameter(i1, "governor_common_rpc_timeout") == 5.0
