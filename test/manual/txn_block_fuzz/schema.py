"""Bringing the cluster up: the user, the tables, and the check that the tier
really did come up synchronous."""

import psycopg

from conftest import Cluster

from . import config
from .workload import APPEND_TABLE, KeyPool


def insert_rows(cur: psycopg.Cursor, table: str, rows: list[tuple], chunk: int = 500):
    for start in range(0, len(rows), chunk):
        part = rows[start : start + chunk]
        placeholders = ", ".join("(" + ", ".join(["%s"] * len(row) + ["''"]) + ")" for row in part)
        cur.execute(f"INSERT INTO {table} VALUES {placeholders};", [value for row in part for value in row])


def setup(cluster: Cluster, pool: KeyPool | None) -> list[tuple[str, str]]:
    """Create the user and the tables, return one pgproto endpoint per instance."""
    leader = cluster.instances[0]
    leader.sql(f"CREATE USER {config.USER} WITH PASSWORD '{config.PASSWORD}'", sudo=True)
    leader.sql(f"GRANT CREATE TABLE TO {config.USER}", sudo=True)

    endpoints = [
        (i.name or str(i.port), f"host={i.pg_host} port={i.pg_port} user={config.USER} password={config.PASSWORD}")
        for i in cluster.instances
    ]

    with psycopg.connect(endpoints[0][1], autocommit=True) as conn:
        with conn.cursor() as cur:
            if "bank" in config.WORKLOADS:
                cur.execute(
                    "CREATE TABLE sacc (grp INT, id INT, balance INT, PRIMARY KEY (grp, id)) DISTRIBUTED BY (grp);"
                )
                for account in range(config.ACCOUNTS):
                    for grp in range(config.GROUPS):
                        cur.execute("INSERT INTO sacc VALUES (%s, %s, %s);", (grp, account, config.INITIAL))

            if pool is not None:
                cur.execute(
                    f"CREATE TABLE {APPEND_TABLE} "
                    "(grp INT, k INT, v TEXT NOT NULL, PRIMARY KEY (grp, k)) DISTRIBUTED BY (grp);"
                )
                for group in pool.groups:
                    insert_rows(cur, APPEND_TABLE, [(group.grp, key) for key in group.keys])
                # An UPDATE of a missing row succeeds and changes nothing, which
                # would look like an append that was acknowledged and lost.
                expected = config.KEYS * config.GROUPS
                cur.execute(f"SELECT count(*) FROM {APPEND_TABLE};")
                row = cur.fetchone()
                assert row is not None and row[0] == expected, f"{APPEND_TABLE} has {row} rows, expected {expected}"

    return endpoints


def assert_synchronous_replication(cluster: Cluster):
    """Confirm the tier came up synchronous: the harness has meaning only there
    (see the test module's docstring), and a cluster that silently came up
    asynchronous would look exactly like a clean pass."""
    sharded = "sacc" if "bank" in config.WORKLOADS else APPEND_TABLE
    instance = cluster.instances[0]
    sharded_is_sync = instance.eval(f"return box.space.{sharded}.is_sync") or False
    quorum = instance.eval("return box.cfg.replication_synchro_quorum")
    states = {i.name: i.eval("return box.info.election.state") for i in cluster.instances}

    print(
        f"\n{sharded}.is_sync={sharded_is_sync} synchro_quorum={quorum} election={states}",
        flush=True,
    )

    assert sharded_is_sync, "sharded table is not synchronous -- the tier did not come up in sync mode"
    assert quorum == config.RF // 2 + 1, f"unexpected synchro quorum {quorum} for rf {config.RF}"
    assert "leader" in states.values(), f"no election leader in the replicaset: {states}"
