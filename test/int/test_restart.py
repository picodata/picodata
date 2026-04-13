import time
import threading

import pytest

from framework.log import log
from conftest import Cluster, TarantoolError, log_crawler


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


@pytest.mark.flaky(reruns=3)
def test_wait_vshard_storage(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_name="default_1", init_replication_factor=2)
    _i1 = cluster.add_instance(replicaset_name="default_1")
    i2 = cluster.add_instance(replicaset_name="default_2")
    _i2 = cluster.add_instance(replicaset_name="default_2")

    i2.sql("""CREATE TABLE IF NOT EXISTS test_table (
        ID BIGINT NOT NULL,
        TP_FID BIGINT NOT NULL,
        PRIMARY KEY (ID))
        USING vinyl DISTRIBUTED BY (TP_FID)
        IN TIER "default"
        """)

    dml = i2.sql("INSERT INTO test_table VALUES(1, 2) ON CONFLICT DO REPLACE")
    assert dml["row_count"] == 1

    # i2 is the router, i1 is the storage
    assert i2.eval("return #box.space.test_table:select()") == 0

    # _i1 is the master after restart
    vshard_block = "BLOCK_BEFORE_NOTIFY_VSHARD_CONFIGURED"
    i1.restart()
    i1.wait_online()

    assert i1.replicaset_master_name() == _i1.name

    # force i1 to became master
    i1.call("pico._inject_error", vshard_block, True)
    lc_vshard = log_crawler(i1, f"ERROR INJECTION '{vshard_block}': BLOCKING")
    index, _ = cluster.cas(
        "update",
        "_pico_replicaset",
        key=["default_1"],
        ops=[("=", "target_master_name", i1.name)],
    )
    cluster.raft_wait_index(index)

    # i1 is a master now, locked before notify happens
    lc_vshard.wait_matched()

    dml = None

    def run_dml():
        nonlocal dml
        try:
            dml = i2.sql("INSERT INTO test_table VALUES(1, 2) ON CONFLICT DO REPLACE", timeout=90)
        except Exception as err:
            dml = err

    t = threading.Thread(target=run_dml)
    t.start()

    # unblock notify, dml must succeed
    lc_vshard_unblock = log_crawler(i1, f"ERROR INJECTION '{vshard_block}': UNBLOCKING")
    i1.call("pico._inject_error", vshard_block, False)
    lc_vshard_unblock.wait_matched()

    t.join()
    if isinstance(dml, Exception):
        raise dml
    assert dml and dml["row_count"] == 1


def test_no_wait_on_master_switchover(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_name="default_1", init_replication_factor=2)
    i2 = cluster.add_instance(replicaset_name="default_1")
    i3 = cluster.add_instance(replicaset_name="default_2")

    i3.sql("CREATE TABLE t0 (a INT PRIMARY KEY, b INT)")
    i3.sql("INSERT INTO t0 VALUES(1, 1)")

    # i1 is the current master of default_1
    assert i1.replicaset_master_name() == i1.name

    # i1 must not accept i2 as master
    vshard_block = "BLOCK_BEFORE_VSHARD_RECONFIGURATION"
    i1.call("pico._inject_error", vshard_block, True)
    lc_vshard = log_crawler(i1, f"ERROR INJECTION '{vshard_block}': BLOCKING")

    # switch master from i1 to i2
    index, _ = cluster.cas(
        "update",
        "_pico_replicaset",
        key=["default_1"],
        ops=[("=", "target_master_name", i2.name)],
    )
    cluster.raft_wait_index(index)
    i2.replicaset_master_name()  # wait for switchover to complete

    lc_vshard.wait_matched()

    # we should get an error instead of waiting
    with pytest.raises(TarantoolError, match=r"DML was sent to the replica during master switchover"):
        i1.sql("INSERT INTO t0 VALUES(2, 2)", timeout=60)

    i1.call("pico._inject_error", vshard_block, False)
