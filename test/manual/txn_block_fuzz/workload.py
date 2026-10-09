"""What the workers run: the blocks themselves, and the keys they work on."""

import random
import threading
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

import psycopg

from . import config


def parse_list(text: str) -> list[int]:
    """A list is stored as its elements joined with a leading comma: ",3,7"."""
    return [int(element) for element in text.split(",") if element]


def check_bank(rows: list[tuple]) -> list[str]:
    """The bank invariants, against the rows a read block returned."""
    broken = []
    if len(rows) != config.ACCOUNTS:
        broken.append(f"saw {len(rows)} accounts, expected {config.ACCOUNTS}: {rows}")
    total = sum(balance for _, balance in rows)
    if rows and total != config.TOTAL:
        broken.append(f"total is {total}, expected {config.TOTAL}: {rows}")
    negative = [row for row in rows if row[1] < 0]
    if negative:
        broken.append(f"negative balances {negative}")
    return broken


@dataclass
class Transfer:
    """Move money between two accounts. Nothing to check: a transfer that
    committed and one that was refused by its own IF guard look the same from
    here, and either way the reads are what catch a broken database."""

    name: str
    sql: str
    params: tuple

    def run(self, cur: psycopg.Cursor) -> list[str]:
        cur.execute(self.sql, self.params)
        return []


@dataclass
class Read:
    """Read every account of a table, or of one group, and check what came back."""

    name: str
    sql: str
    params: tuple

    def run(self, cur: psycopg.Cursor) -> list[str]:
        cur.execute(self.sql, self.params)
        return check_bank(cur.fetchall())


@dataclass
class Append:
    """One list-append transaction: read some lists, then append to some.

    Reads come first because a block may not read after its first DML."""

    name: str
    sql: str
    params: tuple
    reads: list[int]
    appends: list[tuple[int, int]]
    completed: list[list] = field(default_factory=list)

    def invoke_value(self) -> list[list]:
        return [["r", key, None] for key in self.reads] + [["append", key, e] for key, e in self.appends]

    def run(self, cur: psycopg.Cursor) -> list[str]:
        cur.execute(self.sql, self.params)
        lists = {key: parse_list(text) for key, text in (cur.fetchall() if cur.description else [])}
        # A pre-created row that did not come back is lost. Elle still gets an
        # empty read, so the completion matches the invoke; the loss itself is
        # reported here, because an UPDATE of a missing row succeeds and Elle
        # would place the appends after everything observed.
        self.completed = [["r", key, lists.get(key, [])] for key in self.reads]
        self.completed += [["append", key, e] for key, e in self.appends]
        return [f"pre-created row k={key} is gone" for key in self.reads if key not in lists]


Op = Transfer | Read | Append

APPEND_TABLE = "ls"
KEY_SPAN = 1_000_000
ACTIVE_KEYS = 8


class KeyPoolExhausted(Exception):
    """Every pre-created key of a group reached MAX_WRITES_PER_KEY."""


@dataclass
class KeySpace:
    """The pre-created append keys of one group of the append table."""

    grp: int
    keys: list[int]
    issued: int = 0
    active: list[int] = field(default_factory=list)
    writes: Counter = field(default_factory=Counter)


class KeyPool:
    """Hands out keys and elements to the append workload.

    Elle orders a key's appends by the longest list any read returned, so lists
    stay short: a key retires after MAX_WRITES_PER_KEY appends and the next
    pre-created key takes its place. Once no key is left, the run stops instead
    of letting the last lists grow without bound, which would slow every UPDATE
    and Elle's ordering of the key. Elements come from one counter, so they are
    unique per key and never reused by another attempt -- a repeated element
    crashes elle-cli.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.element = 0
        # Key ids are unique across every group, so one history holds them all.
        self.groups = [KeySpace(g, [g * KEY_SPAN + j for j in range(config.KEYS)]) for g in range(config.GROUPS)]
        for group in self.groups:
            self._refill(group)

    def _refill(self, space: KeySpace):
        while len(space.active) < ACTIVE_KEYS and space.issued < len(space.keys):
            space.active.append(space.keys[space.issued])
            space.issued += 1

    def pick(self, rng: random.Random, space: KeySpace, count: int) -> list[int]:
        with self.lock:
            if not space.active:
                raise KeyPoolExhausted(f"every append key of group {space.grp} is retired, raise FUZZ_KEYS")
            return rng.sample(space.active, min(count, len(space.active)))

    def claim_element(self, space: KeySpace, key: int) -> int:
        with self.lock:
            self.element += 1
            space.writes[key] += 1
            if space.writes[key] == config.MAX_WRITES_PER_KEY and key in space.active:
                space.active.remove(key)
                self._refill(space)
            return self.element


def build_append(rng: random.Random, pool: KeyPool, kind: str) -> Append:
    """1-4 micro-operations; a key is read at most once and appended at most once.

    Most appends go to a key the same block read first: Elle's lost-update and
    read-skew checks only see a transaction that read the version it wrote over.
    """
    group = rng.choice(pool.groups)
    keys = pool.pick(rng, group, rng.randint(1, 4))

    reads: list[int] = []
    appends: list[tuple[int, int]] = []
    for key in keys:
        role = rng.choices(["read", "read_append", "append"], weights=[35, 50, 15])[0]
        if role != "append":
            reads.append(key)
        if role != "read":
            appends.append((key, pool.claim_element(group, key)))

    statements: list[str] = []
    params: list[Any] = []
    if reads:
        statements.append(
            f"RETURN QUERY SELECT k, v FROM {APPEND_TABLE} WHERE grp = %s AND k IN ({', '.join(['%s'] * len(reads))});"
        )
        params += [group.grp] + reads
    for key, element in appends:
        statements.append(f"UPDATE {APPEND_TABLE} SET v = v || %s::text WHERE grp = %s AND k = %s;")
        params += [f",{element}", group.grp, key]

    sql = "DO $$ BEGIN\n    " + "\n    ".join(statements) + "\nEND $$;"
    return Append(kind, sql, tuple(params), reads, appends)


def build_op(rng: random.Random, pool: KeyPool | None) -> Op:
    """Pick the next operation and build the block that performs it.

    Bank amounts are inlined and the group and account ids are bound, so a
    block exercises both the literal and the parameter path. A report prints
    the block together with its params.
    """
    kind = rng.choices(config.OPS, weights=config.WEIGHTS)[0]

    if kind == "append_sharded":
        assert pool is not None
        return build_append(rng, pool, kind)

    if kind == "sharded_transfer":
        grp = rng.randrange(config.GROUPS)
        src, dst = rng.sample(range(config.ACCOUNTS), 2)
        amount = rng.randint(1, config.MAX_TRANSFER)
        return Transfer(
            kind,
            f"""
            DO $$ BEGIN
                LET src_balance = (SELECT balance FROM sacc WHERE grp = %s AND id = %s);
                IF src_balance >= {amount} THEN
                    UPDATE sacc SET balance = balance - {amount} WHERE grp = %s AND id = %s;
                    UPDATE sacc SET balance = balance + {amount} WHERE grp = %s AND id = %s;
                END IF;
            END $$;
            """,
            (grp, src, grp, src, grp, dst),
        )

    if kind == "sharded_read":
        grp = rng.randrange(config.GROUPS)
        return Read(
            kind,
            """
            DO $$ BEGIN
                RETURN QUERY SELECT id, balance FROM sacc WHERE grp = %s;
            END $$;
            """,
            (grp,),
        )

    raise AssertionError(f"unknown op {kind}")
