"""Bringing the cluster up: the user, the tables, and the check that the tier
really did come up synchronous."""

import psycopg

from conftest import Cluster

from . import config


def setup(cluster: Cluster) -> list[tuple[str, str]]:
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

    return endpoints


def assert_synchronous_replication(cluster: Cluster):
    """Confirm the tier came up synchronous: the harness has meaning only there
    (see the test module's docstring), and a cluster that silently came up
    asynchronous would look exactly like a clean pass."""
    instance = cluster.instances[0]
    sharded_is_sync = instance.eval("return box.space.sacc.is_sync") or False
    quorum = instance.eval("return box.cfg.replication_synchro_quorum")
    states = {i.name: i.eval("return box.info.election.state") for i in cluster.instances}

    print(
        f"\nsacc.is_sync={sharded_is_sync} synchro_quorum={quorum} election={states}",
        flush=True,
    )

    assert sharded_is_sync, "sharded table is not synchronous -- the tier did not come up in sync mode"
    assert quorum == config.RF // 2 + 1, f"unexpected synchro quorum {quorum} for rf {config.RF}"
    assert "leader" in states.values(), f"no election leader in the replicaset: {states}"
