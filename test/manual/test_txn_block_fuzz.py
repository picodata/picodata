"""
Randomized consistency fuzz for transactional blocks (DO $$ ... $$).

A small Jepsen-style consistency test. Several worker threads run
transactional blocks against every instance of the cluster at once, while an
optional nemesis kills, pauses and restarts instances underneath them.

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
    workload  the blocks to run, and the keys they work on
    history   the operations, in the form elle-cli reads
    runner    the client threads and the nemesis
    schema    cluster bring-up and the synchronous-tier check
    verify    what is checked once the cluster is quiet again
    elle      the serializability check and its negative controls
    stats     counters, anomalies, expected vs unexpected errors
    report    the summary printed at the end

Two workloads, selected with FUZZ_WORKLOAD:

  * bank -- transfers and snapshot reads. Every read block checks the bank
    invariants against the rows it saw: money is conserved, no account is
    negative, no account is missing. Those hold no matter which transfers
    committed, so an operation whose outcome is unknown does not weaken the
    check. They only catch a half-applied block, though: write skew, or a
    transfer lost or applied twice, keeps the total intact.
  * append -- Jepsen's list-append. Each row holds a list; a block reads some
    lists and appends unique elements to some. Every block is recorded in a
    history that Elle checks for serializability after the run. A read of a
    list reveals the order of every append before it, so Elle can rebuild
    which transaction depended on which and find cycles (write skew, read
    skew), duplicated and aborted appends, and diverging orders. It cannot
    see an acknowledged append that no read ever shows: such an append is
    placed after everything observed, which is always serializable.

The history is checked by elle-cli (https://github.com/ligurio/elle-cli), a
standalone jar that runs Elle. It is downloaded on first use into
~/.cache/elle-cli and needs Java 21; ELLE_CLI_JAR points at a jar of your own
instead. Without java the append workload fails before the cluster is
deployed. Three corrupted copies of every
history are checked as well, and the run fails unless each is flagged -- a
check that silently stopped working would otherwise look exactly like a pass.

CI does not run this file: test/manual is outside the pytest.ini testpaths.
Run it by hand:

    CI=1 uv run pytest test/manual/test_txn_block_fuzz.py -s
    FUZZ_WORKLOAD=bank,append FUZZ_NEMESIS=all FUZZ_DURATION=60 \
        CI=1 uv run pytest test/manual/test_txn_block_fuzz.py -s

Knobs, all environment variables:

    FUZZ_DURATION            seconds to run the workload           (default 20)
    FUZZ_WORKERS             concurrent client threads             (default 8)
    FUZZ_INSTANCES           cluster size                          (default 3)
    FUZZ_RF                  replication factor                    (default 3)
    FUZZ_WORKLOAD            comma separated list of [bank, append] (default bank)
    FUZZ_ACCOUNTS            bank accounts per table and per group (default 6)
    FUZZ_GROUPS              groups in the sharded tables          (default 3)
    FUZZ_KEYS                pre-created append keys per group
                             (default 16 per second of FUZZ_DURATION, at least 1000)
    FUZZ_MAX_WRITES_PER_KEY  appends before a key is retired       (default 64)
    FUZZ_SEED                rng seed, printed on every run        (default random)
    FUZZ_NEMESIS             none, all, or a comma separated list of
                             [kill, pause, restart, leader, compact] (default none)
    FUZZ_NEMESIS_PERIOD      seconds between faults                (default 5)
    ELLE_CLI_JAR             elle-cli standalone jar      (default: downloaded on first use)
    FUZZ_ELLE_HEAP           JVM heap for elle-cli                 (default 4g)
"""

import threading
import time
from pathlib import Path

import pytest

from conftest import Cluster, ProcessDead
from txn_block_fuzz import config
from txn_block_fuzz.elle import check_history, elle_cli_jar
from txn_block_fuzz.history import History
from txn_block_fuzz.report import report
from txn_block_fuzz.runner import check_alive, run_nemesis, run_worker
from txn_block_fuzz.schema import assert_synchronous_replication, setup
from txn_block_fuzz.stats import Stats
from txn_block_fuzz.verify import final_append_read, final_check
from txn_block_fuzz.workload import KeyPool


@pytest.mark.timeout(3600)
def test_txn_block_fuzz(cluster: Cluster, tmp_path: Path):
    faults = config.parse_faults()
    elle_jar = elle_cli_jar() if "append" in config.WORKLOADS else None

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

    pool = KeyPool() if "append" in config.WORKLOADS else None
    endpoints = setup(cluster, pool)
    assert_synchronous_replication(cluster)
    stop = threading.Event()
    stats = Stats(expect_faults=bool(faults), stop=stop)
    history = History(tmp_path / "history.json") if pool is not None else None
    print(f"run directory: {tmp_path}", flush=True)

    threads: list[threading.Thread] = [
        threading.Thread(
            target=run_worker,
            args=(i, endpoints, config.SEED + i, stop, stats, pool, history),
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
            args=(cluster, faults, stop, stats, history, tmp_path / "faults.jsonl"),
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
        if history is not None:
            history.stop_workers()
    elapsed = time.monotonic() - started

    # Heal whatever the nemesis left behind, then check the quiet cluster.
    # Without a nemesis, a crash during the run is first seen here.
    history_lines: list[str] = []
    try:
        if nemesis is not None and nemesis.is_alive():
            # The stuck fault still controls its victim; healing it here would race.
            stats.record_not_healed("a fault did not finish in 180 seconds")
        else:
            check_alive(cluster, stats)
            # `wait_online` resets its deadline on every state change, so the
            # budget only has to cover a recovery that is already under way.
            expected_faults = config.DURATION / config.NEMESIS_PERIOD if faults else 0
            try:
                cluster.wait_online(timeout=min(600, round(30 + 10 * expected_faults)))
            except ProcessDead:
                raise
            except Exception as e:
                if not faults:
                    raise
                # The checks that read from the cluster are skipped; the history check does not need it.
                stats.record_not_healed(str(e).strip().splitlines()[0] if str(e).strip() else repr(e))
        if stats.not_healed is None and "bank" in config.WORKLOADS:
            final_check(cluster, stats)
        if pool is not None and history is not None:
            if stats.not_healed is None:
                final_append_read(cluster, pool, history, stats)
            history.close()
            history_lines = [f"history: {history.path} ({history.count} records)"]
            assert elle_jar is not None
            history_lines += check_history(history.path, tmp_path, stats, elle_jar)
    finally:
        # Also when the final checks failed: a history without its closing
        # bracket cannot be replayed through elle-cli.
        if history is not None:
            history.close()
        report(stats, faults, elapsed, history_lines)

    assert stats.stopped_early is None, stats.stopped_early
    assert not stats.anomalies, f"{len(stats.anomalies)} consistency anomalies, see the report above"
    assert not stats.unexpected_samples, "unexpected errors, see the report above"
    if stats.not_healed is not None:
        pytest.skip(f"the cluster did not come back after the faults: {stats.not_healed}")
    for op in config.OPS:
        assert stats.ok[op] > 0, f"{op} never succeeded -- the workload did not really run"
