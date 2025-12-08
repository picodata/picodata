from typing import Generator, Protocol

from conftest import Cluster
from framework.rolling.version import RelativeVersion as Version
from framework.rolling.registry import Registry

import pytest


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
