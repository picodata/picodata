import psycopg
from conftest import Postgres
import pytest


def test_pipelined_messages(postgres: Postgres):
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

    postgres.instance.sql(
        f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5"
    )
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)
    conn = psycopg.connect(
        f"user = {user} password={password} host={host} port={port} sslmode=disable"
    )
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

    postgres.instance.sql(
        f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5"
    )
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)
    conn = psycopg.connect(
        f"user = {user} password={password} host={host} port={port} sslmode=disable"
    )
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
