from conftest import Postgres
import pg8000.native as pg8000  # type: ignore
import pytest
import os


def setup_pg8000_env(postgres: Postgres) -> pg8000.Connection:
    user = "postgres"
    password = "Passw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(
        f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5"
    )
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
        'UPDATE "_pico_property" SET "value" = ? \
            WHERE "key" = \'max_pg_statements\'',
        max_pg_statements,
        sudo=True,
    )

    statements = []
    # make the storage full
    for _ in range(max_pg_statements):
        statements.append(
            conn.prepare(""" insert into "t" values (cast (:p as int)) """)
        )

    # test that an insertion in a full storage fails
    with pytest.raises(
        pg8000.Error,
        match=f'Statement storage is full. Current size limit: {max_pg_statements}. \
Please, increase storage limit using: \
UPDATE "_pico_property" SET "value" = <new-limit> WHERE "key" = \\\\\'max_pg_statements\\\\\'',
    ):
        statements.append(
            conn.prepare(""" insert into "t" values (cast (:p as int)) """)
        )

    # increase storage capacity
    postgres.instance.sql(
        'UPDATE "_pico_property" SET "value" = ? \
            WHERE "key" = \'max_pg_statements\'',
        max_pg_statements + 1,
        sudo=True,
    )

    # test that we can insert a statement in a storage after increasing the limit
    statements.append(conn.prepare(""" insert into "t" values (cast (:p as int)) """))
