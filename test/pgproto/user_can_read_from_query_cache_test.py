from conftest import Postgres
import psycopg


def test_user_can_read_from_query_cache(postgres: Postgres):
    # non-admin user
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(
        f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5"
    )
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)
    conn = psycopg.connect(
        f"user = {user} password={password} host={host} port={port} sslmode=disable"
    )
    conn.autocommit = True

    conn.execute(
        """
        create table "t" (
            "id" integer not null,
            primary key ("id")
        )
        using memtx distributed by ("id")
        option (timeout = 3);
    """
    )
    conn.execute(
        """
        INSERT INTO "t" VALUES (1), (2);
        """,
    )

    # put this query in the query cache
    cur = conn.execute(
        """
        SELECT * FROM "t";
        """,
    )
    assert sorted(cur.fetchall()) == [(1,), (2,)]

    # get this query from the cache
    cur = conn.execute(
        """
        SELECT * FROM "t";
        """,
    )
    assert sorted(cur.fetchall()) == [(1,), (2,)]
