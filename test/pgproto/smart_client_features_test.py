import pg8000.dbapi as pg  # type: ignore
from pg8000.dbapi import Connection
import json
from conftest import Postgres


def check_query_metadata(conn: Connection, query: str, sharding_indexes):
    conn.prepare_statement(query, [])

    # Server will send a notice with metadata before ParseComplete msg
    assert len(conn.notices) == 1

    sharding_key_metadata = json.loads(conn.notices.popleft()[b"M"].decode("UTF-8"))

    assert sharding_key_metadata["query"] == query
    assert sharding_key_metadata["dk_meta"] == sharding_indexes


def test_pico_query_metadata(postgres: Postgres):
    password = "Passw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER postgres WITH PASSWORD '{password}'")
    postgres.instance.sql("GRANT CREATE TABLE TO postgres", sudo=True)

    conn = pg.connect(
        user="postgres",
        password=password,
        host=host,
        port=port,
        startup_params={
            "pico_query_metadata": "true",
            "options": "pico_query_metadata=false",
        },
    )
    conn.autocommit = True

    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE t (a DECIMAL, b TEXT NULL, c INT, d DOUBLE, e UUID,"
        "f DATETIME, g BOOLEAN, PRIMARY KEY (f, a, g, c));"
    )

    check_query_metadata(
        conn,
        query="insert into t (c, a, g, f) values ($1::integer, $2::decimal, $3::boolean, $4::datetime);",
        sharding_indexes=[[4, 1184], [2, 1700], [3, 16], [1, 20]],
    )

    check_query_metadata(
        conn, query="insert into t (a, g, f, b, c) values ($1::decimal, true, $2, $3::text, $4);", sharding_indexes=[]
    )

    check_query_metadata(
        conn,
        query="insert into t (a, g, f, c) values ($1, $1::boolean, $2, $1::int);",
        sharding_indexes=[],
    )

    check_query_metadata(conn, query="insert into t (a, g, f, c) values ($1, true, $2, $1::int);", sharding_indexes=[])

    check_query_metadata(
        conn,
        query="insert into t (f, a, g, c) values ($1::datetime, $2, $3, $4::int);",
        sharding_indexes=[[1, 1184], [2, 1700], [3, 16], [4, 20]],
    )

    check_query_metadata(
        conn,
        query="insert into t values ($1, $2, $3, $4, $5, $6, $7);",
        sharding_indexes=[[6, 1184], [1, 1700], [7, 16], [3, 20]],
    )

    postgres.instance.sql("DROP TABLE t;")
    postgres.instance.sql(
        "CREATE TABLE t (a DECIMAL, b TEXT, c INT, d DOUBLE, e UUID,f DATETIME, g BOOLEAN, PRIMARY KEY (e, b, d));"
    )

    check_query_metadata(
        conn,
        query="insert into t (b, e, d) values ($1::text, $1::uuid, $1::double);",
        sharding_indexes=[],
    )

    check_query_metadata(
        conn,
        query="insert into t (b, e, d) values ($2::text, $3::uuid, $1::double);",
        sharding_indexes=[[3, 2950], [2, 25], [1, 701]],
    )

    check_query_metadata(
        conn,
        query="insert into t (b, e, d) values ($2::text, '9035910e-8342-4460-8c25-25e5f10d9876'::uuid, $1::double);",
        sharding_indexes=[],
    )

    check_query_metadata(
        conn,
        query="insert into t (e, b, d) values ($1::uuid, $2::text, $3::double), ($4::uuid, $5::text, $6::double);",
        sharding_indexes=[],
    )

    check_query_metadata(
        conn,
        query="insert into t values ($1::decimal, $2::text, $3::int, $4::double, $5::uuid, $6::datetime, $7::boolean);",
        sharding_indexes=[[5, 2950], [2, 25], [4, 701]],
    )

    postgres.instance.sql("DROP TABLE t;")
    postgres.instance.sql("CREATE TABLE t (a INT PRIMARY KEY, b TEXT, c INT) DISTRIBUTED BY (c, b);")

    check_query_metadata(
        conn,
        query="insert into t(a, b) values ($1, $2);",
        sharding_indexes=[],
    )

    check_query_metadata(
        conn,
        query="insert into t(a, b, c) values ($1, $2, $3);",
        sharding_indexes=[[3, 20], [2, 25]],
    )

    check_query_metadata(
        conn,
        query="insert into t(a, b, c) values (1, '', 2), ($1, $2, $3);",
        sharding_indexes=[],
    )

    check_query_metadata(
        conn,
        query="insert into t(a, b, c) select a + 1, b, c from t where b = $1 and c = $2;",
        sharding_indexes=[],
    )

    check_query_metadata(
        conn,
        query="select * from t where b = $1 and c = $2;",
        sharding_indexes=[],
    )

    conn.close()


def test_pico_stmt_invalidation(postgres: Postgres):
    password = "Passw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER postgres WITH PASSWORD '{password}'")
    postgres.instance.sql("GRANT CREATE TABLE to postgres", sudo=True)
    postgres.instance.sql("GRANT ALTER TABLE to postgres", sudo=True)

    conn = pg.connect(
        user="postgres",
        password=password,
        host=host,
        port=port,
        startup_params={"options": "pico_stmt_invalidation=true"},
    )
    conn.autocommit = True

    cursor = conn.cursor()
    cursor.execute("CREATE TABLE t (a INT PRIMARY KEY, b TEXT)")

    query = "insert into t values ($1::integer, $2::text);"
    ps_name, cols, funcs = conn.prepare_statement(query, [])
    cursor.execute("ALTER TABLE t ADD COLUMN c INT;")

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
