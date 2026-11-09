"""What is checked once the workload has stopped and the cluster is quiet."""

import time

from conftest import Cluster, Instance, Retriable

from . import config
from .history import History
from .stats import Anomaly, Stats
from .workload import APPEND_TABLE, KeyPool, check_bank, parse_list


def read_accounts(instance: Instance, sql: str) -> list[tuple]:
    # Retried while the cluster still settles after the last fault; a dead
    # instance is fatal at once. The cluster is already online here, so an
    # error that lasts longer than this is reported, not waited out.
    retriable = Retriable()
    retriable.deadline = min(retriable.deadline, time.monotonic() + 120)
    rows = retriable.call(instance.sql, sql, sudo=True)
    return [tuple(row) for row in rows]


SHARDED_READ = "DO $$ BEGIN RETURN QUERY SELECT id, balance FROM sacc WHERE grp = {grp}; END $$;"


def read_groups(instance: Instance) -> dict[int, list[tuple]]:
    """Every group's accounts. Read inside blocks: a plain SELECT carries no
    transactional guarantee, so it could show a snapshot taken mid-transfer and
    accuse a healthy database."""
    return {grp: read_accounts(instance, SHARDED_READ.format(grp=grp)) for grp in range(config.GROUPS)}


def settle(instance: Instance, rounds: int = 10, delay: float = 3.0) -> dict[int, list[str]]:
    """Read every group repeatedly on a quiet cluster until the bank invariants hold.

    Anything an in-flight transfer could explain must disappear once the
    workload has stopped: the writers are joined, so no transfer is in flight,
    and every extra round gives any straggler more time to land. An invariant
    still broken after this is durable, not a snapshot artifact.
    """
    broken: dict[int, list[str]] = {}
    for attempt in range(rounds):
        groups = read_groups(instance)
        broken = {grp: check_bank(rows) for grp, rows in groups.items()}
        totals = {grp: sum(row[1] for row in rows) for grp, rows in groups.items()}
        wrong = any(broken.values())
        print(f"  settle round {attempt + 1}: totals {totals}" + ("" if wrong else "  (all correct)"), flush=True)
        if not wrong:
            break
        if attempt < rounds - 1:
            time.sleep(delay)
    return broken


def final_check(cluster: Cluster, stats: Stats):
    """Check the bank invariants on the quiet cluster after the workload stopped."""
    admin = cluster.instances[0]

    print("\nfinal check (workload stopped, cluster healed):", flush=True)
    for grp, details in settle(admin).items():
        for detail in details:
            stats.record_anomaly(
                Anomaly(
                    "final",
                    f"sharded grp={grp}",
                    f"{detail}, read inside a block on a quiet cluster",
                    admin.name or "?",
                    SHARDED_READ.format(grp=grp),
                )
            )


def final_append_read(cluster: Cluster, pool: KeyPool, history: History, stats: Stats):
    """Read every key that was ever handed out, on the quiet cluster.

    This exposes the order of appends no worker read afterwards. A key whose
    row is gone is reported and left out of the read: an UPDATE of a missing
    row succeeds, so Elle alone would never see the loss."""
    admin = cluster.instances[0]
    admin.raft_read_index()
    for group in pool.groups:
        sql = f"DO $$ BEGIN RETURN QUERY SELECT k, v FROM {APPEND_TABLE} WHERE grp = {group.grp}; END $$;"
        lists = {row[0]: parse_list(row[1]) for row in read_accounts(admin, sql)}
        issued = group.keys[: group.issued]
        missing = [key for key in issued if key not in lists]
        if missing:
            detail = f"{len(missing)} of {len(issued)} pre-created rows are gone, first {missing[:10]}"
            stats.record_anomaly(Anomaly("lost-row", f"{APPEND_TABLE} grp={group.grp}", detail, admin.name or "?", sql))
        value = [["r", key, lists[key]] for key in issued if key in lists]
        history.record_read(config.WORKERS, value, admin.name or "?")
