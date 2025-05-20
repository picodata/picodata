import pg8000.native as pg  # type: ignore
from conftest import Postgres


def test_pgbench_queries(postgres: Postgres):
    user = "Бен Бенчич"
    password = "P@ssw0rd"

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)
    conn = pg.Connection(user, password=password, host=postgres.host, port=postgres.port)

    conn.run(
        """
        CREATE TABLE pgbench_branches (
            bid int PRIMARY KEY,
            bbalance int,
            filler varchar(88)
        );
        """
    )
    conn.run(
        """
        CREATE TABLE pgbench_tellers (
            tid int PRIMARY KEY,
            bid int,
            tbalance int,
            filler varchar(84)
        );
        """
    )

    conn.run(
        """
        CREATE TABLE pgbench_accounts (
            aid int PRIMARY KEY,
            bid int,
            abalance int,
            filler varchar(84)
        );
        """
    )

    conn.run(
        """
        CREATE TABLE pgbench_history (
            tid int,
            bid int,
            aid int,
            delta int,
            mtime datetime,
            filler varchar(22),
            PRIMARY KEY(tid, bid, aid, delta)
        );
        """
    )

    # The following queries can be found at https://www.postgresql.org/docs/current/pgbench.html

    conn.run("BEGIN;")
    conn.run("UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;", delta=1, aid=2)
    conn.run("SELECT abalance FROM pgbench_accounts WHERE aid = :aid;", aid=3)
    conn.run("UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;", delta=4, tid=5)
    conn.run("UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;", delta=6, bid=7)
    conn.run(
        "INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) \
            VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);",
        tid=8,
        bid=9,
        aid=10,
        delta=11,
    )

    conn.run("END;")
