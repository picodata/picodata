import time

from framework.log import log
from conftest import Cluster


def test_restart_timing(cluster: Cluster):
    """
    Test that checks timings of picodata restart.
    Proactive sending of `proc_update_instance` during `postjoin` should make it fast & predictable.
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
"""
    )

    arbiter1 = cluster.add_instance(tier="arbiter", wait_online=False)
    _arbiter2 = cluster.add_instance(tier="arbiter", wait_online=False)
    _arbiter3 = cluster.add_instance(tier="arbiter", wait_online=False)
    storage = cluster.add_instance(tier="storage", wait_online=False)
    cluster.wait_online()

    arbiter1.promote_or_fail()

    # now restart the storage node 10 times and see how long each restart takes
    for _ in range(10):
        storage.terminate()

        start = time.time()

        storage.start()
        storage.wait_online()

        elapsed = time.time() - start

        log.info(f"Restart completed in {elapsed} seconds")

        # In my testing, after implementation of proactive `proc_update_instance`, `elapsed` is consistently sitting at 2.01 seconds
        # While before enabling proactive `proc_update_instance` it sits around 4-6 seconds
        # Use a slightly higher threshold to reduce flaky false positives
        assert elapsed < 5
