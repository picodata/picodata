from conftest import Postgres, Retriable
import pg8000.native as pg8000  # type: ignore
import pytest
import os


def setup_pg8000_env(postgres: Postgres) -> pg8000.Connection:
    user = "postgres"
    password = "Passw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    os.environ["PGSSLMODE"] = "disable"
    conn = pg8000.Connection(
        user,
        password=password,
        host=host,
        port=port,
    )

    # Note: We don't use conn.run because it will create an unnamed statement that cannot
    # be closed in pg8000 (conn.close_prepared_statement won't help)
    ps = conn.prepare(
        """
        create table "t" (
            "id" integer not null,
            primary key ("id")
        )
        using memtx distributed by ("id")
        """
    )
    ps.run()
    ps.close()

    return conn


def test_statement_storage_config_limit(postgres: Postgres):
    conn = setup_pg8000_env(postgres)

    max_pg_statements = 32
    postgres.instance.sql(
        f"ALTER SYSTEM SET pg_statement_max = {max_pg_statements}",
        sudo=True,
    )

    statements = []
    # make the storage full
    for _ in range(max_pg_statements):
        statements.append(conn.prepare(""" insert into "t" values (cast (:p as int)) """))

    # test that an insertion in a full storage fails
    with pytest.raises(
        pg8000.Error,
        match=f'Statement storage is full. Current size limit: {max_pg_statements}. \
Please, increase storage limit using: \
ALTER SYSTEM SET "pg_statement_max" TO <new-limit>',
    ):
        statements.append(conn.prepare(""" insert into "t" values (cast (:p as int)) """))

    # increase storage capacity
    postgres.instance.sql(
        f"ALTER SYSTEM SET pg_statement_max={max_pg_statements + 1}",
        sudo=True,
    )

    # test that we can insert a statement in a storage after increasing the limit
    statements.append(conn.prepare(""" insert into "t" values (cast (:p as int)) """))


def test_statement_storage_limit_is_shared_by_clients(postgres: Postgres):
    conn1 = setup_pg8000_env(postgres)
    conn2 = pg8000.Connection("postgres", password="Passw0rd", host=postgres.host, port=postgres.port)

    max_pg_statements = 4
    postgres.instance.sql(
        f"ALTER SYSTEM SET pg_statement_max = {max_pg_statements}",
        sudo=True,
    )

    # the first client fills the storage of all clients
    statements = []
    for _ in range(max_pg_statements):
        statements.append(conn1.prepare(""" insert into "t" values (cast (:p as int)) """))

    with pytest.raises(pg8000.Error, match="Statement storage is full"):
        conn2.prepare(""" insert into "t" values (cast (:p as int)) """)

    # the server frees the statements of a client after it disconnects
    conn1.close()
    Retriable().call(lambda: conn2.prepare(""" insert into "t" values (cast (:p as int)) """))
