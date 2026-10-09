"""The two things that run while the workload is in flight: the client
threads, and the nemesis that disturbs them."""

import json
import os
import random
import signal
import threading
import time
from pathlib import Path

import psycopg

from conftest import Cluster, Instance, ProcessDead
from framework.log import log

from . import config
from .history import History, outcome_of
from .stats import Anomaly, Stats
from .workload import Append, KeyPool, KeyPoolExhausted, build_op


def run_worker(
    process: int,
    endpoints: list[tuple[str, str]],
    seed: int,
    stop: threading.Event,
    stats: Stats,
    pool: KeyPool | None,
    history: History | None,
):
    """One client: pick a coordinator, run blocks against it, reconnect when it dies."""
    rng = random.Random(seed)
    conn = None
    node = "?"

    while not stop.is_set():
        if conn is None:
            node, conn_info = rng.choice(endpoints)
            try:
                conn = psycopg.connect(conn_info, autocommit=True, connect_timeout=5)
            except Exception as e:
                stats.record_error("connect", e, node, "")
                stop.wait(0.2)
                continue

        try:
            op = build_op(rng, pool)
        except KeyPoolExhausted as e:
            # The run cannot go on as configured; stop it and say so.
            stats.record_stop(str(e))
            stop.set()
            break
        if isinstance(op, Append):
            assert history is not None
            if not history.begin(process, op, node):
                break
        try:
            with conn.cursor() as cur:
                broken = op.run(cur)
            if isinstance(op, Append):
                history.finish(process, "ok", op.completed)  # type: ignore[union-attr]
            stats.record_ok(op.name)
            for detail in broken:
                stats.record_anomaly(Anomaly("invariant", op.name, detail, node, op.sql, op.params))
        except (psycopg.OperationalError, psycopg.InterfaceError) as e:
            # The connection is gone -- reconnect, maybe to a different node.
            if isinstance(op, Append):
                history.finish(process, outcome_of(e), op.invoke_value())  # type: ignore[union-attr]
            stats.record_error(op.name, e, node, op.sql, op.params)
            try:
                conn.close()
            except Exception:
                pass
            conn = None
        except Exception as e:
            if isinstance(op, Append):
                history.finish(process, outcome_of(e), op.invoke_value())  # type: ignore[union-attr]
            stats.record_error(op.name, e, node, op.sql, op.params)

    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass

    log.info(f"worker {process} finished")


def check_alive(cluster: Cluster, stats: Stats) -> list[Instance]:
    """The instances whose process is running.

    A process that exited when no fault asked it to is a finding: it is recorded
    as an anomaly and started again, so that the run keeps its full cluster."""
    alive = []
    for instance in cluster.instances:
        if instance.process is None:
            continue
        try:
            instance.check_process_alive()
        except ProcessDead as e:
            stats.record_anomaly(Anomaly("crash", "process", str(e), instance.name or "?", ""))
            # start() is a no-op while the exited process is still attached.
            instance.kill()
            instance.start()
            continue
        alive.append(instance)
    return alive


def apply_fault(cluster: Cluster, fault: str, rng: random.Random, stop: threading.Event, stats: Stats) -> str:
    alive = check_alive(cluster, stats)
    if not alive:
        return "no instance alive"

    if fault in ("kill", "restart", "pause"):
        victim: Instance = rng.choice(alive)
        log.info(f"NEMESIS {fault} {victim.name}")
        if fault == "kill":
            victim.kill()
            stop.wait(rng.uniform(0.5, 2.0))
            victim.start()
        elif fault == "restart":
            victim.restart()
        else:
            os.killpg(victim.process.pid, signal.SIGSTOP)  # type: ignore[union-attr]
            try:
                stop.wait(rng.uniform(0.5, 2.0))
            finally:
                os.killpg(victim.process.pid, signal.SIGCONT)  # type: ignore[union-attr]
        victim.wait_online()
        return victim.name or "?"

    if fault == "leader":
        leader = cluster.leader()
        others = [i for i in alive if i is not leader]
        if not others:
            return "no other instance"
        target = rng.choice(others)
        log.info(f"NEMESIS leader {leader.name} -> {target.name}")
        raft_id = leader.instance_info(target)["raft_id"]
        leader.call(".proc_raft_transfer_leader", raft_id)
        # Ask once and wait a bounded time: the target may itself be killed by
        # the next fault, and a transfer that never happens is not an error.
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if leader.call(".proc_raft_info")["state"] != "Leader":
                break
            stop.wait(0.2)
        return f"{leader.name} -> {target.name}"

    if fault == "compact":
        leader = cluster.leader()
        log.info(f"NEMESIS compact raft log on {leader.name}")
        leader.raft_compact_log()
        return leader.name or "?"

    raise AssertionError(f"unknown fault {fault}")


def run_nemesis(
    cluster: Cluster,
    faults: list[str],
    stop: threading.Event,
    stats: Stats,
    history: History | None,
    faults_log: Path,
):
    """Apply faults one at a time. Each is written to faults_log with the number
    of history records so far, so an anomaly can be placed against the fault
    that preceded it. The file belongs to this thread: the main thread stops
    waiting for a fault that hangs, and must not close the file under it."""
    rng = random.Random(config.SEED ^ 0x5EED)

    with faults_log.open("w") as out:

        def note(fault: str, phase: str, target: str = ""):
            record = {"time": time.monotonic_ns(), "fault": fault, "phase": phase, "target": target}
            record["history_records"] = history.size() if history is not None else None
            out.write(json.dumps(record) + "\n")
            out.flush()

        while not stop.is_set():
            if stop.wait(config.NEMESIS_PERIOD):
                break
            fault = rng.choice(faults)
            note(fault, "start")
            try:
                target = apply_fault(cluster, fault, rng, stop, stats)
                stats.record_fault(fault)
                note(fault, "end", target)
            except Exception as e:
                # A fault that could not be applied (or healed in time) is worth
                # seeing, but it is not a consistency violation by itself.
                log.warning(f"nemesis {fault} failed: {e}")
                stats.record_fault(f"{fault}(failed)")
                note(fault, "failed", str(e))
