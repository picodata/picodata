import pytest
import os
from conftest import Postgres

# We use psycopg when we want the client to send parameters types explicitly.
import psycopg
from psycopg.pq import ExecStatus
from psycopg import RawCursor

# We use pg8000 when we want to prepare statements explicitly or when we don't want the client
# to send parameters types, which is useful when we test parameter types inference.
import pg8000.native as pg  # type: ignore
from pg8000.exceptions import DatabaseError  # type: ignore


def test_extended_query(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}'")

    os.environ["PGSSLMODE"] = "disable"
    conn = pg.Connection(user, password=password, host=postgres.host, port=postgres.port)

    ps = conn.prepare(
        """
        create table "tall" (
            "id" integer not null,
            "str" string,
            "bool" boolean,
            "real" double,
            primary key ("id")
        )
        using memtx distributed by ("id")
        option (timeout = 3);
    """
    )

    # statement is prepared, but not executed yet
    with pytest.raises(DatabaseError, match='table with name "TALL" not found'):
        conn.run(""" select * from "TALL" """)

    ps.run()

    ps = conn.prepare(
        """
        INSERT INTO "tall" VALUES
            (1, 'one', true, 0.1),
            (2, 'to', false, 0.2),
            (4, 'for', true, 0.4);
    """
    )
    ps.run()

    ps = conn.prepare(
        """
        SELECT * FROM "tall";
    """
    )

    tuples = ps.run()
    assert [1, "one", True, 0.1] in tuples
    assert [2, "to", False, 0.2] in tuples
    assert [4, "for", True, 0.4] in tuples
    assert len(tuples) == 3

    # rerun the same statement
    tuples = ps.run()
    assert [1, "one", True, 0.1] in tuples
    assert [2, "to", False, 0.2] in tuples
    assert [4, "for", True, 0.4] in tuples
    assert len(tuples) == 3

    ps.close()

    # run a closed statement
    with pytest.raises(DatabaseError, match="Couldn't find statement"):
        ps.run()

    # let's check that we are fine after some error handling
    ps = conn.prepare(
        """
        SELECT * FROM "tall";
    """
    )

    tuples = ps.run()
    assert [1, "one", True, 0.1] in tuples
    assert [2, "to", False, 0.2] in tuples
    assert [4, "for", True, 0.4] in tuples
    assert len(tuples) == 3


def test_parameterized_queries(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}'")

    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    conn.execute(
        """
        create table "tall" (
            "id" integer not null,
            "str" string,
            "bool" boolean,
            "real" double,
            primary key ("id")
        )
        using memtx distributed by ("id")
        option (timeout = 3);
    """
    )

    # NOTE: psycopg sends parameter types for everything except strings,
    # so they need to be specified via cast (%s -> %s::text).

    params1 = (-1, "string", True, 3.141592)
    # text parameters
    conn.execute(
        """
        INSERT INTO "tall" VALUES (%t, %t::text, %t, %t);
        """,
        params1,
        binary=False,
    )
    cur = conn.execute(
        """
        SELECT * FROM "tall";
    """
    )
    assert cur.fetchall() == [params1]

    params2 = (3000000, "Россия!", False, -2.71)
    # binary parameters
    conn.execute(
        """
        INSERT INTO "tall" VALUES (%b, %b::text, %b, %b);
        """,
        params2,
        binary=False,
    )
    cur = conn.execute(
        """
        SELECT * FROM "tall";
    """
    )
    rows = cur.fetchall()
    assert sorted([params1, params2]) == sorted(rows)

    cur = conn.execute(
        """
        SELECT * FROM "tall";
    """,
        binary=True,
    )

    rows = cur.fetchall()
    assert sorted([params1, params2]) == sorted(rows)


def test_empty_queries(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}'")

    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    cur = conn.execute("  ", prepare=True)
    assert cur.pgresult is not None
    assert cur.pgresult.status == ExecStatus.EMPTY_QUERY

    cur = conn.execute(" ; ", prepare=True)
    assert cur.pgresult is not None
    assert cur.pgresult.status == ExecStatus.EMPTY_QUERY


def test_deallocate(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}'")

    os.environ["PGSSLMODE"] = "disable"
    conn = pg.Connection(user, password=password, host=postgres.host, port=postgres.port)
    conn.autocommit = True

    # Remove unprepared statement
    statement_name = "not_existing_name"
    ps = conn.prepare(f"DEALLOCATE {statement_name}")
    with pytest.raises(DatabaseError, match=f"prepared statement {statement_name} does not exist."):
        ps.run()

    # Remove statement with .close()
    ps = conn.prepare("SELECT 1")
    ps.run()

    ps.close()

    # run a closed statement
    with pytest.raises(DatabaseError, match="Couldn't find statement"):
        ps.run()

    # Remove statement with DEALLOCATE statement_name
    ps = conn.prepare("SELECT 1")
    ps.run()

    # Decode the binary name to a string
    statement_name = ps.name_bin.decode("utf-8").strip("\x00")
    # Use the decoded name in the DEALLOCATE statement
    ps_deallocate = conn.prepare(f"DEALLOCATE {statement_name}")

    # check that prepare ps_deallocate doesn't actually deallocate statement
    ps.run()

    ps_deallocate.run()

    # run a closed statement
    with pytest.raises(DatabaseError, match="Couldn't find statement"):
        ps.run()

    # Remove statements with DEALLOCATE ALL
    ps1 = conn.prepare("SELECT 1")
    ps1.run()

    ps2 = conn.prepare("SELECT 1")
    ps2.run()

    ps_deallocate = conn.prepare("DEALLOCATE ALL")

    ps1.run()
    ps2.run()

    ps_deallocate.run()

    # run a closed statement1
    with pytest.raises(DatabaseError, match="Couldn't find statement"):
        ps1.run()

    # run a closed statement2
    with pytest.raises(DatabaseError, match="Couldn't find statement"):
        ps2.run()

    conn.close()


def test_tcl(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port
    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}'")

    conn = psycopg.connect(f"user={user} password={password} host={host} port={port} sslmode=disable")
    # With autocommit
    conn.autocommit = True

    cur = conn.execute("CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT);")

    cur = conn.execute("INSERT INTO test_table (id, name) VALUES (1,'Alice'), (2,'Bob');")

    cur = conn.execute("SELECT * FROM test_table;")
    rows = cur.fetchall()
    assert rows == [(1, "Alice"), (2, "Bob")]

    cur = conn.execute("DROP TABLE test_table;")

    # Without autocommit
    conn.autocommit = False

    cur = conn.execute("BEGIN;", prepare=True)
    assert cur.pgresult is not None
    assert cur.pgresult.status == ExecStatus.COMMAND_OK

    cur = conn.execute("CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT);")

    cur = conn.execute("INSERT INTO test_table (id, name) VALUES (1,'Alice'), (2,'Bob');")

    cur = conn.execute("SELECT * FROM test_table;")
    rows = cur.fetchall()
    assert rows == [(1, "Alice"), (2, "Bob")]

    cur = conn.execute("ROLLBACK;", prepare=True)
    assert cur.pgresult is not None
    assert cur.pgresult.status == ExecStatus.COMMAND_OK

    cur = conn.execute("SELECT * FROM test_table;")
    rows = cur.fetchall()
    assert rows == [(1, "Alice"), (2, "Bob")]

    cur = conn.execute("DROP TABLE test_table;")

    cur = conn.execute("BEGIN;", prepare=True)
    assert cur.pgresult is not None
    assert cur.pgresult.status == ExecStatus.COMMAND_OK

    cur = conn.execute("CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT);")

    cur = conn.execute("INSERT INTO test_table (id, name) VALUES (1,'Alice'), (2,'Bob');")

    cur = conn.execute("COMMIT;", prepare=True)
    assert cur.pgresult is not None
    cur.pgresult.status == ExecStatus.COMMAND_OK

    cur = conn.execute("SELECT * FROM test_table;")
    rows = cur.fetchall()
    assert rows == [(1, "Alice"), (2, "Bob")]

    cur = conn.execute("DROP TABLE test_table;")


def test_create_schema(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port
    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}'")

    conn = psycopg.connect(f"user={user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    cur = conn.execute("CREATE SCHEMA test_schema;", prepare=True)
    assert cur.pgresult is not None
    assert cur.pgresult.status == ExecStatus.COMMAND_OK
    assert cur.statusmessage == "CREATE SCHEMA"


def test_drop_schema(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port
    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}'")

    conn = psycopg.connect(f"user={user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    cur = conn.execute("DROP SCHEMA test_schema;", prepare=True)
    assert cur.pgresult is not None
    assert cur.pgresult.status == ExecStatus.COMMAND_OK
    assert cur.statusmessage == "DROP SCHEMA"


def test_wrong_number_of_params(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    dsn = f"host={postgres.host} port={postgres.port} user={user} password={password}"
    with psycopg.connect(dsn) as conn:
        with RawCursor(conn) as cur:
            # correct params
            cur.execute("SELECT $1, $2", [4, 5], prepare=True)
            assert cur.fetchone() == (4, 5)

            # insufficient params
            with pytest.raises(
                psycopg.errors.ProtocolViolation,
                match="bind message supplies 0 parameters, but prepared statement .* requires 1",
            ):
                cur.execute("SELECT $1", [], prepare=True)

            # insufficient params
            with pytest.raises(
                psycopg.errors.ProtocolViolation,
                match="bind message supplies 2 parameters, but prepared statement .* requires 4",
            ):
                cur.execute("SELECT $1, $2, $3, $4", ["Hello", "4"], prepare=True)

            # excessive params
            with pytest.raises(
                psycopg.errors.ProtocolViolation,
                match="bind message supplies 1 parameters, but prepared statement .* requires 0",
            ):
                cur.execute("SELECT 1", ["hello"], prepare=True)

            # excessive params
            with pytest.raises(
                psycopg.errors.ProtocolViolation,
                match="bind message supplies 3 parameters, but prepared statement .* requires 2",
            ):
                cur.execute("SELECT $1, $2", [2, 1, 4], prepare=True)


def test_large_literals_cant_fit_into_target_type(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}'")

    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    max_value = 9223372036854775807
    value_above_max = 9223372036854775808

    params = (max_value,)
    # text parameters
    cur = conn.execute(
        """
        SELECT %t::int;
        """,
        params,
    )
    assert cur.fetchall() == [params]

    # binary parameters
    cur = conn.execute(
        """
        SELECT %b::int;
        """,
        params,
    )
    assert cur.fetchall() == [params]

    # large literals without cast will be interpreted as numeric
    params = (value_above_max,)
    # text parameters
    cur = conn.execute(
        """
        SELECT %t;
        """,
        params,
    )

    assert cur.description is not None
    type_oid = cur.description[0].type_code
    assert type_oid == 1700
    assert cur.fetchall() == [params]

    # binary parameters
    cur = conn.execute(
        """
        SELECT %b;
        """,
        params,
    )

    assert cur.description is not None
    type_oid = cur.description[0].type_code
    assert type_oid == 1700
    assert cur.fetchall() == [params]

    with pytest.raises(
        psycopg.errors.InternalError_, match="encoding error: out of range integral type conversion attempted"
    ):
        cur.execute(""" SELECT %t::int """, params)

    with pytest.raises(
        psycopg.errors.InternalError_, match="encoding error: out of range integral type conversion attempted"
    ):
        cur.execute(""" SELECT %b::int """, params)

    with RawCursor(conn) as cur:
        with pytest.raises(
            psycopg.errors.InternalError_, match="encoding error: out of range integral type conversion attempted"
        ):
            cur.execute("SELECT $1::int", [value_above_max], prepare=True)

        with pytest.raises(
            psycopg.errors.InternalError_, match="encoding error: out of range integral type conversion attempted"
        ):
            cur.execute("SELECT $1::int * 2", [max_value], prepare=True)
