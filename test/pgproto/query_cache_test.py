import psycopg
from conftest import Postgres, Retriable

# test that cached queries executed through pgproto and iproto are cross-compatible
# https://git.picodata.io/core/picodata/-/issues/2061
# https://git.picodata.io/core/picodata/-/issues/2010

query = "SELECT MAX(t1.a) FROM t t1 JOIN t t2 ON TRUE"


def prepare(postgres: Postgres) -> psycopg.connection.Connection:
    user = "postgres"
    password = "Passw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'", sudo=True)
    postgres.instance.sql(f'GRANT READ TABLE TO "{user}"', sudo=True)
    postgres.instance.sql("CREATE TABLE t (a INT PRIMARY KEY, b INT)", sudo=True)

    conn = psycopg.connect(f"postgres://{user}:{password}@{host}:{port}", autocommit=True)

    return conn


def test_cache_compatibility_iproto_to_pgproto(postgres: Postgres):
    conn = prepare(postgres)

    # put a query into cache through iproto...
    postgres.instance.sql(query)
    # ...and now use the cached plan through pgproto
    conn.execute(query)


def test_cache_compatibility_pgproto_to_iproto(postgres: Postgres):
    conn = prepare(postgres)

    # put a query into cache through pgproto...
    conn.execute(query)
    # ...and now use the cached plan through iproto
    postgres.instance.sql(query)


def test_user_can_read_from_query_cache(postgres: Postgres):
    # non-admin user
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)
    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    conn.execute(
        """
        create table "t" (
            "id" integer not null,
            primary key ("id")
        )
        using memtx distributed by ("id");
    """
    )
    conn.execute(
        """
        INSERT INTO "t" VALUES (1), (2);
        """,
    )

    def select_returns_inserted():
        cur = conn.execute(
            """
        SELECT * FROM "t";
        """,
        )
        assert sorted(cur.fetchall()) == [(1,), (2,)]

    # put this query in the query cache
    # we have to retry because of vshard rebalancing problems
    # see https://git.picodata.io/core/sbroad/-/issues/848
    Retriable().call(select_returns_inserted)

    # get this query from the cache
    cur = conn.execute(
        """
        SELECT * FROM "t";
        """,
    )
    assert sorted(cur.fetchall()) == [(1,), (2,)]
