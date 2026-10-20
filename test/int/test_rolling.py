from conftest import Cluster, log_crawler
from conftest import Instance
from conftest import Retriable
from framework.registry import get_or_make_registry
from framework.registry import Registry
from framework.util.version import base_version
from framework.util.version import ExecutableVersion
from framework.util.version import parse_version_exc
from framework.util.version import VersionAlias
from packaging.version import Version
from test_http_server import (
    PASSWORD,
    USERNAME,
    assert_refresh_revoked,
    authorize,
    decode_jwt_claims,
    get_authorized,
    logout,
    set_jwt_enabled,
)
from urllib.request import urlopen
import json
import pytest
import os
import signal
import subprocess


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
    mismatch_error = "mismatches the leader's version"
    for instance in cluster.instances[:-1]:
        instance.change_executable(current, mismatch_error)

    # Only one old-version instance remains alive. Once it is stopped for the
    # upgrade, there is no peer left to reject it with a version mismatch.
    # The restarted process instead keeps trying to activate through dead
    # peers; `Instance.fail_to_start` reaches timeout while waiting for
    # process to die and then kills it, while the log_crawler records
    # the logged error.
    no_live_peers_error = "failed to activate myself: failed to connect"
    no_live_peers_lc = log_crawler(cluster.instances[-1], no_live_peers_error)
    with pytest.raises(subprocess.TimeoutExpired):
        cluster.instances[-1].change_executable(current, no_live_peers_error)
    no_live_peers_lc.wait_matched()

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
@pytest.mark.xfail(
    reason="picodata 26.2.1 panics when reading 26.3 catalogue. https://git.picodata.io/core/picodata/-/work_items/3163"
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

    error = "mismatches the leader's version"
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
@pytest.mark.xfail(
    reason="picodata 26.2.1 panics when reading 26.3 catalogue. https://git.picodata.io/core/picodata/-/work_items/3163"
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

    error = "mismatches the leader's version"
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
        Version("26.2.1"),
        get_or_make_registry().next_version(Version("26.2.1"), skip_on_gap=False),
    ]
)
def test_jwt_revocation_during_upgrade(cluster: Cluster, registry: Registry):
    previous = registry.get_or_skip(Version("26.2.1"))
    target = registry.get(registry.next_version(Version("26.2.1")))
    assert target is not None

    instances = cluster.deploy(executable=previous, instance_count=3)
    upgraded = instances[0]
    set_jwt_enabled(upgraded, True)
    tokens, status = authorize(upgraded, USERNAME, PASSWORD)
    assert status == 200
    cluster.raft_wait_index(upgraded.raft_get_index())

    upgraded.change_executable(target)
    assert_version(cluster, previous.version, registry)
    assert upgraded.eval("return box.space._pico_revoked_jwt == nil") is True

    # Before catalog migration, logout remains a no-op and existing refresh
    # tokens continue working, including on the instance running the new binary.
    for instance in instances:
        with get_authorized(f"http://{instance.http_listen}/api/v1/session", tokens["refresh"]) as response:
            assert response.status == 200
            refreshed = json.load(response)
            assert refreshed["auth"] and refreshed["refresh"]
    logout(upgraded, f"Bearer {tokens['refresh']}")
    with get_authorized(f"http://{upgraded.http_listen}/api/v1/session", tokens["refresh"]) as response:
        assert response.status == 200
    assert upgraded.eval("return box.space._pico_revoked_jwt == nil") is True

    for instance in instances[1:]:
        instance.change_executable(target)
    cluster.wait_governor_status("idle")
    Retriable().call(assert_version, cluster, target.version, registry)

    # Tokens issued before the upgrade survive migration. Once all instances
    # are upgraded, logout on a follower must prevent refresh on every instance.
    for instance in instances:
        assert instance.sql('SELECT * FROM "_pico_revoked_jwt"') == []
        with get_authorized(f"http://{instance.http_listen}/api/v1/session", tokens["refresh"]) as response:
            assert response.status == 200

    follower = next(instance for instance in instances if instance.raft_id != instance.raft_leader_id())
    logout(follower, f"Bearer {tokens['refresh']}")
    for instance in instances:
        assert_refresh_revoked(instance, tokens["refresh"])
        assert instance.sql('SELECT jwt, retain_until FROM "_pico_revoked_jwt"') == [
            [tokens["refresh"], decode_jwt_claims(tokens["refresh"])["exp"] + 60]
        ]
        with get_authorized(f"http://{instance.http_listen}/api/v1/tiers", tokens["auth"]) as response:
            assert response.status == 200
    logout(follower, f"Bearer {tokens['refresh']}")
    cluster.check_health()


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
        VersionAlias.PREVIOUS_MINOR,
        VersionAlias.CURRENT,
    ]
)
def test_mixed_binary_data_decoding_compat_during_upgrade(cluster: Cluster, registry: Registry):
    """
    CURRENT switched the tarantool `binary_data_decoding` compat option from
    'old' to 'new' (see `set_tarantool_compat_options` in src/lib.rs). Each
    instance applies this compat setting locally based on its own binary, so
    during a rolling upgrade some instances decode MP_BIN as a plain Lua
    string ('old', on PREVIOUS_MINOR) while others decode it as `varbinary`
    cdata ('new', on CURRENT). This affects the raft network Lua bridge
    (src/traft/network.rs), so verify that SQL and raft leadership keep
    working correctly while the cluster is genuinely mixed:
    1. Deploy a cluster on PREVIOUS_MINOR ('old' compat everywhere).
    2. Upgrade half the instances to CURRENT ('new' compat), leaving the
       rest on PREVIOUS_MINOR - a genuinely mixed-compat cluster.
    3. Force a leader re-election so raft messages get routed through
       instances with both compat settings.
    4. Confirm SQL DML keeps working regardless of which instance serves it.
    5. Finish the upgrade and confirm the cluster is healthy.
    """

    # step 1

    executable = registry.get_or_skip(VersionAlias.PREVIOUS_MINOR)
    current = registry.get(VersionAlias.CURRENT)
    assert current is not None

    cluster.deploy(
        executable=executable,
        instance_count=4,
        init_replication_factor=2,
    )
    cluster.check_health()

    # DDL must happen while the cluster is still homogeneous.
    cluster.instances[0].sql(
        "CREATE TABLE compat_check (a INT PRIMARY KEY, b STRING) DISTRIBUTED BY (a) WAIT APPLIED GLOBALLY"
    )

    upgraded, not_upgraded = cluster.instances[:2], cluster.instances[2:]
    for instance in upgraded:
        instance.change_executable(current)

    cluster.check_health(homogeneous=False)

    # step 3

    old_leader = Retriable().call(cluster.leader)
    observer = cluster.pick_random_instance(exclude=[old_leader])
    old_leader.terminate()
    observer.wait_has_states("Offline", "Offline", target=old_leader)

    new_leader = Retriable().call(cluster.leader)
    assert new_leader is not old_leader

    old_leader.start_and_wait()

    # step 4

    for instance in cluster.instances:
        instance.sql("INSERT INTO compat_check VALUES (?, ?)", instance.raft_id, instance.name)

    rows = cluster.instances[0].sql("SELECT a, b FROM compat_check ORDER BY a")
    assert len(rows) == len(cluster.instances)

    # step 5

    for instance in not_upgraded:
        instance.change_executable(current)

    cluster.check_health()

    rows = cluster.instances[0].sql("SELECT a, b FROM compat_check ORDER BY a")
    assert len(rows) == len(cluster.instances)


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

                    -- normalize is_nullable representation in index fields
                    if catalog_name == "_index" then
                        local format = box.space[normalized_row.id]:format()
                        for _, part in ipairs(normalized_row.parts) do
                            -- Detect the legacy array representation, such as {0, "string"}.
                            if part.field == nil then
                                part.field, part.type = part[1], part[2]
                                part[1], part[2] = nil, nil
                                local field = format[part.field + 1]
                                part.is_nullable = field ~= nil and field.is_nullable == true
                            elseif part.is_nullable == nil then
                                part.is_nullable = false
                            end
                        end
                    end

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


# The 26.3.1 catalog upgrade adds `governor_ddl_rpc_timeout`, raises the
# default of `governor_common_rpc_timeout` from 3.0 to 7.0 and adds the JWT
# token lifetimes.
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
    assert read_parameter(i1, "jwt_auth_token_lifetime") is None
    assert read_parameter(i1, "jwt_refresh_token_lifetime") is None

    cluster.change_executable(current)
    i1 = cluster.leader()

    assert read_parameter(i1, "governor_ddl_rpc_timeout") == 30.0
    assert read_parameter(i1, "governor_common_rpc_timeout") == 7.0
    assert read_parameter(i1, "jwt_auth_token_lifetime") == 1800
    assert read_parameter(i1, "jwt_refresh_token_lifetime") == 432000

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


@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        Version("26.2.1"),
        get_or_make_registry().next_version(Version("26.2.1"), skip_on_gap=False),
    ]
)
def test_upgrade_from_26_2_1_synchronous_spaces(cluster: Cluster, registry: Registry):
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        arbiter:
            can_vote: true
        storage:
            can_vote: true
            replication_factor: 2
            replication_mode: sync
        """
    )

    from_version = Version("26.2.1")
    from_executable = registry.get(from_version)
    assert from_executable is not None

    a1 = cluster.add_instance(wait_online=False, tier="arbiter", executable=from_executable)
    i1 = cluster.add_instance(wait_online=False, tier="storage", executable=from_executable)
    i2 = cluster.add_instance(wait_online=False, tier="storage", executable=from_executable)
    cluster.wait_online()

    for i in [a1, i1, i2]:
        assert i.eval("return box.space._space.is_sync") is False

    to_version = registry.next_version(from_version)
    to_executable = registry.get(to_version)
    assert to_executable is not None

    cluster.change_executable(to_executable)

    # Since 26.3 the system spaces are synchronous in every tier, the async
    # arbiter tier included.
    for i in [a1, i1, i2]:
        assert i.eval("return box.space._space.is_sync") is True


@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        Version("26.2.1"),
        get_or_make_registry().next_version(Version("26.2.1"), skip_on_gap=False),
    ]
)
def test_upgrade_from_26_2_1_elections_on_every_tier(cluster: Cluster, registry: Registry):
    """
    Since 26.3 every tier runs elections, whatever its `replication_mode` is.
    An async tier switches to them once every instance runs the new version,
    see `ELECTIONS_ON_EVERY_TIER_SINCE`. Until then it works like in 26.2.
    """
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

    from_version = Version("26.2.1")
    from_executable = registry.get(from_version)
    assert from_executable is not None

    to_version = registry.next_version(from_version)
    to_executable = registry.get(to_version)
    assert to_executable is not None

    a1 = cluster.add_instance(wait_online=False, tier="arbiter", executable=from_executable)
    s1 = cluster.add_instance(wait_online=False, tier="storage", executable=from_executable)
    s2 = cluster.add_instance(wait_online=False, tier="storage", executable=from_executable)
    cluster.wait_online()

    master_name = s1.replicaset_master_name()
    old_master, new_master = (s1, s2) if master_name == s1.name else (s2, s1)

    def check_master(i: Instance, name: str):
        assert i.replicaset_master_name() == name

    def check_writable(i: Instance):
        assert not i.eval("return box.info.ro")

    def check_no_elections(i: Instance):
        assert i.eval("return box.cfg.election_mode") == "off"
        # Nobody has ever been promoted.
        assert i.eval("return box.info.synchro.queue.owner") == 0

    def check_elections(i: Instance, quorum: int):
        assert i.eval("return box.cfg.election_mode") == "manual"
        assert i.eval("return box.cfg.replication_synchro_quorum") == quorum

    def check_leader(i: Instance):
        assert i.eval("return box.info.election.state") == "leader"
        assert not i.eval("return box.info.ro")
        assert i.eval("return box.info.synchro.queue.owner") == i.eval("return box.info.id")

    def check_follower(i: Instance):
        assert i.eval("return box.info.election.state") == "follower"
        assert i.eval("return box.info.ro_reason") == "election"

    a1.sql("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) DISTRIBUTED BY (id) IN TIER storage")
    a1.sql("INSERT INTO t VALUES (1, 'before upgrade')")

    def check_row_count(i: Instance, count: int):
        assert i.eval("return box.space.t:count()") == count

    Retriable().call(check_row_count, new_master, 1)

    #
    # Upgrade the raft leader, so that the governor runs the new version, and
    # the replica. The cluster is still on the old version, so the async tiers
    # don't run elections yet, even on the restarted instances.
    #
    a1.change_executable(to_executable)
    new_master.change_executable(to_executable)
    assert_version(cluster, from_version, registry)

    for i in [a1, old_master, new_master]:
        check_no_elections(i)
    check_writable(old_master)
    assert new_master.eval("return box.info.ro_reason") == "config"

    #
    # A failover works like in 26.2: the replica is made writable directly,
    # not promoted in an election.
    #
    old_master.terminate()
    Retriable().call(check_master, new_master, new_master.name)
    cluster.wait_governor_status("idle")
    Retriable().call(check_writable, new_master)
    check_no_elections(new_master)
    a1.sql("INSERT INTO t VALUES (2, 'failover during upgrade')")

    #
    # An instance which joins during the upgrade doesn't run elections either.
    #
    # NOTE: It joins the existing replicaset. The first instance of a new
    # replicaset would create the system tables of the new version on its own,
    # e.g. `_pico_revoked_jwt`, and then the catalog upgrade would fail to
    # create them.
    #
    # NOTE: The join response lists the old master too, which is down, and
    # `box.cfg` waits for it for `replication_connect_timeout` (30s) before it
    # bootstraps the instance. Hence the longer timeout.
    #
    s3 = cluster.add_instance(
        wait_online=True,
        tier="storage",
        replicaset_name=new_master.replicaset_name,
        executable=to_executable,
        timeout=60,
    )
    Retriable().call(check_row_count, s3, 2)
    check_no_elections(s3)
    assert s3.eval("return box.info.ro_reason") == "config"

    #
    # Finish the upgrade. The old master comes back as a replica.
    #
    old_master.executable = to_executable
    old_master.start()
    old_master.wait_online()

    # NOTE: Needs to be retriable because upgrade happens asynchronously after all instances become Online.
    Retriable().call(assert_version, cluster, to_version, registry)
    cluster.wait_governor_status("idle")

    #
    # Every tier runs elections now, without a restart. The governor tells the
    # instances to run them when it configures the replication, and the
    # masters promote themselves.
    #
    # The quorum is the majority of the replicaset members: the arbiter
    # replicaset has a single member, the storage one has 3.
    for i, quorum in [(a1, 1), (old_master, 2), (new_master, 2), (s3, 2)]:
        Retriable().call(check_elections, i, quorum)
    for i in [a1, new_master]:
        Retriable().call(check_leader, i)
    for i in [old_master, s3]:
        Retriable().call(check_follower, i)

    # The catalog upgrade, which needs the writable masters, has finished.
    assert a1.sql("SELECT op, status, status_description FROM _pico_governor_queue WHERE status != 'done'") == []
    for i in [a1, old_master, new_master, s3]:
        assert i.eval("return box.space._space.is_sync") is True

    # DDL needs the synchro quorum of every replicaset.
    a1.sql("CREATE TABLE t2 (id INTEGER PRIMARY KEY) DISTRIBUTED BY (id) IN TIER storage")
    a1.sql("INSERT INTO t VALUES (3, 'after upgrade')")
    Retriable().call(check_row_count, old_master, 3)

    # The replicaset is back to 2 members.
    cluster.expel(s3, force=True)
    cluster.wait_has_states(s3, "Expelled", "Expelled")
    cluster.wait_governor_status("idle")
    for i in [old_master, new_master]:
        Retriable().call(check_elections, i, 2)
    Retriable().call(check_leader, new_master)

    #
    # A failover is an election now. The replicaset has 2 members, and the
    # governor breaks the tie: the remaining one is the quorum.
    #
    new_master.terminate()
    Retriable().call(check_master, old_master, old_master.name)
    cluster.wait_governor_status("idle")
    Retriable().call(check_elections, old_master, 1)
    Retriable().call(check_leader, old_master)
    a1.sql("INSERT INTO t VALUES (4, 'failover after upgrade')")

    # A restarted instance runs elections right away, and the quorum is back.
    new_master.start()
    new_master.wait_online()
    for i in [old_master, new_master]:
        Retriable().call(check_elections, i, 2)
    Retriable().call(check_leader, old_master)
    Retriable().call(check_follower, new_master)
    Retriable().call(check_row_count, new_master, 4)

    #
    # An instance which joins after the upgrade runs elections too, and
    # follows the elected leader. The replicaset has 3 members now.
    #
    # NOTE: It joins the existing replicaset. The first instance of a new
    # replicaset creates the system tables of the new version on its own, e.g.
    # `_pico_revoked_jwt`, and then fails to replay the `CREATE TABLE` of the
    # catalog upgrade from the raft log.
    #
    s4 = cluster.add_instance(
        wait_online=True, tier="storage", replicaset_name=old_master.replicaset_name, executable=to_executable
    )
    for i in [old_master, new_master, s4]:
        Retriable().call(check_elections, i, 2)
    Retriable().call(check_leader, old_master)
    for i in [new_master, s4]:
        Retriable().call(check_follower, i)
    Retriable().call(check_row_count, s4, 4)


@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        Version("26.2.1"),
        get_or_make_registry().next_version(Version("26.2.1"), skip_on_gap=False),
    ]
)
def test_upgrade_from_26_2_1_sync_tier_quorum(cluster: Cluster, registry: Registry):
    """
    In 26.2 a sync tier runs with the synchro quorum of the replication factor
    alone. Since 26.3 it's the majority of the replicaset members if there are
    more of them than the replication factor. Once every instance runs the new
    version, the governor corrects the quorum, even if nothing else changes in
    the replicaset then.
    """
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        arbiter:
            can_vote: true
        storage:
            can_vote: false
            replication_factor: 1
            replication_mode: sync
        """
    )

    from_version = Version("26.2.1")
    from_executable = registry.get(from_version)
    assert from_executable is not None

    to_version = registry.next_version(from_version)
    to_executable = registry.get(to_version)
    assert to_executable is not None

    a1 = cluster.add_instance(wait_online=False, tier="arbiter", executable=from_executable)
    s1 = cluster.add_instance(wait_online=False, tier="storage", executable=from_executable)
    cluster.wait_online()
    # More members than the replication factor.
    s2 = cluster.add_instance(
        wait_online=True, tier="storage", replicaset_name=s1.replicaset_name, executable=from_executable
    )

    def check_quorum(i: Instance, quorum: int):
        assert i.eval("return box.cfg.replication_synchro_quorum") == quorum

    for i in [s1, s2]:
        check_quorum(i, 1)

    cluster.change_executable(to_executable)

    # NOTE: Needs to be retriable because upgrade happens asynchronously after all instances become Online.
    Retriable().call(assert_version, cluster, to_version, registry)
    cluster.wait_governor_status("idle")

    # The quorum is the majority of the 2 members, so neither of them can be
    # elected without the other.
    for i in [s1, s2]:
        Retriable().call(check_quorum, i, 2)

    # The replicaset is still writable with both members alive.
    a1.sql("CREATE TABLE t (id INTEGER PRIMARY KEY) DISTRIBUTED BY (id) IN TIER storage")
    a1.sql("INSERT INTO t VALUES (1)")
