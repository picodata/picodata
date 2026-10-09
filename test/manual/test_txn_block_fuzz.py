"""
Randomized consistency fuzz for transactional blocks (DO $$ ... $$).

A Jepsen-style consistency test. Several worker threads run transactional
blocks against every instance of the cluster at once, while an optional
nemesis kills, pauses and restarts instances underneath them.

Every block works on sharded rows, grouped by the sharding key: every
statement of a block stays in one group, so in one bucket. Blocks writing
DISTRIBUTED GLOBALLY tables are out of scope, because DML on a global table
inside a block is not supported.

The cluster is always deployed on a synchronous tier. A block's atomicity
across a change of replicaset master is guaranteed only there, so an
asynchronous run would be measuring a configuration the database does not
claim to support, and every invariant it broke would be expected rather
than a finding. The mode is fixed at bootstrap and cannot be changed
afterwards, which is why the harness deploys through a cluster config
instead of `init_replication_factor`.

This file is the orchestration only; the pieces live in `txn_block_fuzz/`:

    config    every knob, read from the environment at import
    workload  the blocks to run
    runner    the client threads and the nemesis
    schema    cluster bring-up and the synchronous-tier check
    verify    what is checked once the cluster is quiet again
    stats     counters, anomalies, expected vs unexpected errors
    report    the summary printed at the end

One workload so far, selected with FUZZ_WORKLOAD:

  * bank -- transfers and snapshot reads. Every read block checks the bank
    invariants against the rows it saw: money is conserved, no account is
    negative, no account is missing. Those hold no matter which transfers
    committed, so an operation whose outcome is unknown does not weaken the
    check. They only catch a half-applied block, though: write skew, or a
    transfer lost or applied twice, keeps the total intact.

CI does not run this file: test/manual is outside the pytest.ini testpaths.
Run it by hand:

    CI=1 uv run pytest test/manual/test_txn_block_fuzz.py -s
    FUZZ_NEMESIS=all FUZZ_DURATION=60 \
        CI=1 uv run pytest test/manual/test_txn_block_fuzz.py -s

Knobs, all environment variables:

    FUZZ_DURATION            seconds to run the workload           (default 20)
    FUZZ_WORKERS             concurrent client threads             (default 8)
    FUZZ_INSTANCES           cluster size                          (default 3)
    FUZZ_RF                  replication factor                    (default 3)
    FUZZ_WORKLOAD            comma separated list of [bank]        (default bank)
    FUZZ_ACCOUNTS            bank accounts per group               (default 6)
    FUZZ_GROUPS              groups in the sharded table           (default 3)
    FUZZ_SEED                rng seed, printed on every run        (default random)
    FUZZ_NEMESIS             none, all, or a comma separated list of
                             [kill, pause, restart, leader, compact] (default none)
    FUZZ_NEMESIS_PERIOD      seconds between faults                (default 5)
"""

import threading
import time
from pathlib import Path

import pytest

from conftest import Cluster, ProcessDead
from txn_block_fuzz import config
from txn_block_fuzz.report import report
from txn_block_fuzz.runner import check_alive, run_nemesis, run_worker
from txn_block_fuzz.schema import assert_synchronous_replication, setup
from txn_block_fuzz.stats import Stats
from txn_block_fuzz.verify import final_check


@pytest.mark.timeout(3600)
def test_txn_block_fuzz(cluster: Cluster, tmp_path: Path):
    faults = config.parse_faults()

    # `--init-replication-factor` cannot be combined with `--config`, and
    # `replication_mode` can only be set in the config, so the replication factor
    # moves into the tier definition too. The tier is named `default` so that no
    # statement in this file needs an `IN TIER` clause.
    cluster.set_config_file(
        yaml=f"""
cluster:
    name: {cluster.name}
    tier:
        default:
            replication_factor: {config.RF}
            replication_mode: sync
"""
    )
    cluster.deploy(instance_count=config.INSTANCES)
    cluster.wait_until_buckets_balanced()

    endpoints = setup(cluster)
    assert_synchronous_replication(cluster)
    stop = threading.Event()
    stats = Stats(stop=stop)
    print(f"run directory: {tmp_path}", flush=True)

    threads: list[threading.Thread] = [
        threading.Thread(
            target=run_worker,
            args=(i, endpoints, config.SEED + i, stop, stats),
            name=f"fuzz-worker-{i}",
            # On a sync tier `replication_synchro_timeout` is INFINITY, so a write
            # which reached the limbo before the replicaset was fenced never
            # returns. Daemon threads let the run finish and report instead of
            # hanging at interpreter exit.
            daemon=True,
        )
        for i in range(config.WORKERS)
    ]
    nemesis: threading.Thread | None = None
    if faults:
        nemesis = threading.Thread(
            target=run_nemesis,
            args=(cluster, faults, stop, stats, tmp_path / "faults.jsonl"),
            name="fuzz-nemesis",
            daemon=True,
        )

    started = time.monotonic()
    for thread in threads:
        thread.start()
    if nemesis is not None:
        nemesis.start()

    try:
        stop.wait(config.DURATION)
    finally:
        stop.set()
        if nemesis is not None:
            nemesis.join(timeout=180)
        for thread in threads:
            thread.join(timeout=60)
    elapsed = time.monotonic() - started

    # Heal whatever the nemesis left behind, then check the quiet cluster.
    # Without a nemesis, a crash during the run is first seen here.
    try:
        check_alive(cluster, stats)
        # An instance the nemesis restarted keeps a replication config that
        # points at a peer the nemesis killed, and the governor auto-offlines it
        # for the broken replication. Recovery is the instance reattaching its
        # appliers one by one while its own sentinel sets the target state back
        # to Online, which took about 90 seconds after a storm of 23 faults.
        # `wait_online` retries that state and resets its deadline on every
        # state change, so this budget only has to cover a recovery already
        # under way: the stock 30 seconds gave up seconds before the last
        # applier reattached. It does not cover a cluster where nothing moves at
        # all -- the wait is bounded so that such a cluster is reported instead
        # of hung on.
        expected_faults = config.DURATION / config.NEMESIS_PERIOD if faults else 0
        try:
            cluster.wait_online(timeout=min(600, round(30 + 10 * expected_faults)))
        except ProcessDead:
            raise
        except Exception as e:
            if not faults:
                raise
            # The faults left an instance Offline and nothing brought it back:
            # the governor reports nothing to do and the instance's own sentinel
            # does not wake it. The checks below read from the cluster, so they
            # would measure a crippled one; the run is skipped instead, after
            # the asserts that need no healthy cluster. Without a nemesis the
            # same state is a bug, so it is raised rather than skipped.
            stats.record_not_healed(str(e).strip().splitlines()[0] if str(e).strip() else repr(e))
        if stats.not_healed is None and "bank" in config.WORKLOADS:
            final_check(cluster, stats)
    finally:
        report(stats, faults, elapsed, [])

    assert not stats.anomalies, f"{len(stats.anomalies)} consistency anomalies, see the report above"
    assert not stats.unexpected_samples, "unexpected errors, see the report above"
    if stats.not_healed is not None:
        pytest.skip(f"the cluster did not come back after the faults: {stats.not_healed}")
    for op in config.OPS:
        assert stats.ok[op] > 0, f"{op} never succeeded -- the workload did not really run"
