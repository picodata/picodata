import psycopg
from conftest import Postgres

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
