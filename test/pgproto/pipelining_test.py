import psycopg
from conftest import Postgres
import pytest

# pg8000 lets us send protocol messages one by one.
import pg8000.native as pg  # type: ignore
from pg8000.core import NULL_BYTE, Context  # type: ignore
from pg8000.exceptions import DatabaseError  # type: ignore


def test_pipelined_messages(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)
    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    queries = [
        "create table test (id integer not null, primary key (id))\
            using memtx distributed by (id);",
        "insert into test values (1);",
        "insert into test values (2);",
    ]
    # run queries in a pipeline
    with conn.pipeline():
        for query in queries:
            conn.execute(bytes(query, "utf-8"))

        # let's read some rows during the pipeline
        cur = conn.execute("select * from test")
        results = cur.fetchall()
        assert sorted(results) == [(1,), (2,)]

    # verify the results
    cur = conn.execute(" SELECT * FROM test;")
    results = cur.fetchall()
    assert sorted(results) == [(1,), (2,)]


def test_pipelined_messages_with_error(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)
    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    queries = [
        "create table test (id integer not null, primary key (id))\
            using memtx distributed by (id);",
        "insert into test values (1);",
        "insert into test values (1);",  # this will cause duplicate key error during the pipeline
        "insert into test values (2);",  # so this message should not be executed
    ]

    sent = 0
    with pytest.raises(psycopg.InternalError, match="Duplicate key"):
        # run queries in a pipeline
        with conn.pipeline():
            for query in queries:
                conn.execute(bytes(query, "utf-8"))
                sent += 1

    # verify that we've sent all the messages
    assert sent == len(queries)

    # verify the results, the table should not have 2 due to the error
    cur = conn.execute(" SELECT * FROM test;")
    results = cur.fetchall()
    assert sorted(results) == [(1,)]


def test_pipeline_restores_after_error(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)
    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    # start the pipeline
    with conn.pipeline() as pipeline:
        # 1st group of messages
        conn.execute(
            "create table test (id integer not null, primary key (id))\
            using memtx distributed by (id);"
        )
        conn.execute("insert into test values (1);")
        pipeline.sync()

        # 2nd group of messages
        conn.execute("insert into test values (2)")
        # this will cause duplicate key error during the pipeline
        conn.execute("insert into test values (1)")
        # so this shouldn't be executed
        conn.execute("insert into test values (1487)")
        # NOTE: In contrast to postgres, the insertion of 2 will not be rolled
        # back due to the lack of proper transaction support.
        with pytest.raises(psycopg.InternalError, match="Duplicate key"):
            pipeline.sync()

        # 3rd group of messages
        conn.execute("insert into test values (3)")
        pipeline.sync()

    # verify the results
    cur = conn.execute(" SELECT * FROM test;")
    results = cur.fetchall()
    assert sorted(results) == [(1,), (2,), (3,)]


def test_failed_query_in_unsynced_pipeline(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)
    conn = pg.Connection(user, password=password, host=postgres.host, port=postgres.port)
    conn.run("create table t (id integer not null, primary key (id)) distributed by (id)")
    # Fail with a timeout and not hang if ReadyForQuery does not come.
    conn._usock.settimeout(10)

    # Parse, Bind and Execute with no Sync, then a failing Query.
    # Query ends the extended query, so the server must not wait for Sync
    # after the error and must send ReadyForQuery.
    conn.send_PARSE(NULL_BYTE, "insert into t values (1)")
    conn.send_BIND(NULL_BYTE, ())
    conn.send_EXECUTE()
    conn.send_QUERY("select * from no_such_table")
    conn._sock.flush()
    with pytest.raises(DatabaseError, match="no_such_table"):
        conn.handle_messages(Context(None))

    assert conn.run("select * from t") == [[1]]
