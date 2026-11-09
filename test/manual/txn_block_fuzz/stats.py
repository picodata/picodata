"""What the run observed: counters, anomalies, and the rule that separates
the errors a fault is expected to cause from the errors worth a report."""

import re
import threading
from collections import Counter
from dataclasses import dataclass, field

from framework.log import log

# Concurrent blocks on the same rows abort each other; that happens with or
# without faults.
CONTENTION_ERRORS = re.compile(r"\bconflict|\baborted\b", re.IGNORECASE)

# A block that cannot run right now because of a fault is not a bug: the
# coordinator was killed, the raft leader is being re-elected, a bucket is
# being moved by the rebalancer. Only a run with a nemesis expects these.
# The patterns are whole words or phrases taken from the messages of psycopg,
# raft, tarantool and vshard: a bare fragment such as `cas` or `bucket` would
# also swallow a planner error that happens to contain it.
FAULT_ERRORS = re.compile(
    # the connection, as psycopg reports it
    r"\bconnection\b|server closed|reset by peer|peer closed|broken pipe|\beof\b|end of file|io error|"
    r"\brefused\b|\btimeout\b|timed out|\bdisconnect|query and params failed|"
    # raft
    r"not a leader|leader is unknown|leader id is unknown|term mismatch|"
    # the instance: starting, read-only, or without a quorum
    r"not online|\bunavailable\b|is not available|bootstrap hasn't finished|\bnot.ready\b|"
    r"\bread.only\b|\belection\b|\bquorum\b|\bsynchro|not writable|"
    # vshard: buckets in motion, replicasets without a master
    r"\bbucket \d+\b|buckets are not discovered|transaction on all buckets|rebalancing in progress|"
    r"is not a master|master is not configured|master switchover|no active replicas|is unreachable|"
    r"replicaset is locked|is in backoff|out of sync|isn't active|replicaset sync|"
    r"storage is disabled|router is disabled|configuration of the (?:storage|router)|"
    r"is outdated after module reload|"
    # compare-and-swap on the raft log
    r"cas rejected|conflictfound|entrytermmismatch|\bretries\b|"
    r"single node|one node|data motion",
    re.IGNORECASE,
)


@dataclass
class Anomaly:
    """A checker verdict: the database returned something impossible."""

    kind: str
    op: str
    detail: str
    node: str
    sql: str
    params: tuple = ()

    def __str__(self):
        text = f"[{self.kind}] {self.op} on {self.node}: {self.detail}\n{self.sql.strip()}"
        if self.params:
            text += f"\nparams: {self.params}"
        return text


@dataclass
class Stats:
    # Without a nemesis only CONTENTION_ERRORS are expected.
    expect_faults: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)
    ok: Counter = field(default_factory=Counter)
    expected: Counter = field(default_factory=Counter)
    unexpected: Counter = field(default_factory=Counter)
    faults: Counter = field(default_factory=Counter)
    anomalies: list[Anomaly] = field(default_factory=list)
    unexpected_samples: list[Anomaly] = field(default_factory=list)
    not_healed: str | None = None
    # Set by the first anomaly or unexpected error: the workers and the nemesis
    # wind down, so a finding is reported instead of the errors it goes on to
    # cause. The checks made afterwards do not consult it and still run.
    stop: threading.Event = field(default_factory=threading.Event)
    stopped_early: str | None = None

    def record_ok(self, op: str):
        with self.lock:
            self.ok[op] += 1

    def record_error(self, op: str, error: Exception, node: str, sql: str, params: tuple = ()):
        text = str(error) or repr(error)
        with self.lock:
            if CONTENTION_ERRORS.search(text) or (self.expect_faults and FAULT_ERRORS.search(text)):
                self.expected[op] += 1
                return
            self.unexpected[op] += 1
            self.unexpected_samples.append(Anomaly("unexpected-error", op, text, node, sql, params))
        self.stop.set()

    def record_anomaly(self, anomaly: Anomaly):
        with self.lock:
            self.anomalies.append(anomaly)
        self.stop.set()
        log.error(f"ANOMALY {anomaly}")

    def record_stop(self, reason: str):
        with self.lock:
            if self.stopped_early is None:
                self.stopped_early = reason

    def record_not_healed(self, reason: str):
        with self.lock:
            self.not_healed = reason

    def record_fault(self, fault: str):
        with self.lock:
            self.faults[fault] += 1
