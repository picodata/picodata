import pg8000.dbapi as pg  # type: ignore
from conftest import Postgres


def test_pico_stmt_invalidation(postgres: Postgres):
    password = "Passw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"postgres\" WITH PASSWORD '{password}'")
    postgres.instance.sql("CREATE TABLE t (a INTEGER NOT NULL PRIMARY KEY, b TEXT NULL);")

    conn = pg.connect(
        user="postgres",
        password=password,
        host=host,
        port=port,
        startup_params={"options": "pico_stmt_invalidation=true"},
    )
    conn.autocommit = True

    query = "insert into t values ($1::integer, $2::text);"
    ps_name, cols, funcs = conn.prepare_statement(query, [])
    postgres.instance.sql("ALTER TABLE t ADD COLUMN c INT;")

    try:
        conn.execute_named(
            statement_name_bin=ps_name,
            columns=cols,
            input_funcs=funcs,
            params=(
                "1",
                "t",
            ),
            statement="",
        )
    except pg.Error as e:
        assert e.args[0]["C"] == "42999"
        assert e.args[0]["M"] == "prepared statement pg8000_statement_0 has been invalidated"
    finally:
        conn.close()
