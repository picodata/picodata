"""What the workers run: the blocks themselves."""

import random
from dataclasses import dataclass

import psycopg

from . import config


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
    """Read every account of one group, and check what came back."""

    name: str
    sql: str
    params: tuple

    def run(self, cur: psycopg.Cursor) -> list[str]:
        cur.execute(self.sql, self.params)
        return check_bank(cur.fetchall())


Op = Transfer | Read


def build_op(rng: random.Random) -> Op:
    """Pick the next operation and build the block that performs it.

    Bank amounts are inlined and the group and account ids are bound, so a
    block exercises both the literal and the parameter path. A report prints
    the block together with its params.
    """
    kind = rng.choices(config.OPS, weights=config.WEIGHTS)[0]

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
