import pytest
import pg8000.dbapi as pg  # type: ignore
import os
from conftest import Postgres
from decimal import Decimal
import psycopg
from psycopg.pq import ExecStatus
import pg8000.native as pg2  # type: ignore
from pg8000.exceptions import DatabaseError  # type: ignore


def test_simple_query_flow_errors(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}'")

    os.environ["PGSSLMODE"] = "disable"
    conn = pg.Connection(user, password=password, host=postgres.host, port=postgres.port)
    conn.autocommit = True
    cur = conn.cursor()

    with pytest.raises(pg.DatabaseError, match="rule parsing error"):
        cur.execute(
            """
            CREATE TEMPORARY TABLE book (id SERIAL, title TEXT);
        """
        )

    with pytest.raises(pg.DatabaseError, match="space BOOK not found"):
        cur.execute(
            """
            INSERT INTO "BOOK" VALUES (1, 2);
        """
        )


def test_simple_flow_session(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}'")

    os.environ["PGSSLMODE"] = "disable"
    conn = pg.Connection(user, password=password, host=postgres.host, port=postgres.port)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
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

    cur.execute(
        """
        INSERT INTO "tall" VALUES
            (1, 'one', true, 0.1),
            (2, 'to', false, 0.2),
            (4, 'for', true, 0.4);
    """
    )

    cur.execute(
        """
        SELECT * FROM "tall";
    """
    )

    tuples = cur.fetchall()
    assert [1, "one", True, 0.1] in tuples
    assert [2, "to", False, 0.2] in tuples
    assert [4, "for", True, 0.4] in tuples

    cur.execute(
        """
        DROP TABLE "tall";
    """
    )


def test_explain(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}'")

    os.environ["PGSSLMODE"] = "disable"
    conn = pg.Connection(user, password=password, host=postgres.host, port=postgres.port)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
        """
        create table "explain" (
            "id" integer not null,
            primary key ("id")
        )
        using memtx distributed by ("id")
        option (timeout = 3);
    """
    )

    query = """
        insert into "explain" values (0);
    """
    cur.execute("explain " + query)
    plan = cur.fetchall()
    assert 'insert "explain" on conflict: fail' in plan[0]
    assert '    motion [policy: segment([ref("COLUMN_1")]), program: ReshardIfNeeded]' in plan[1]
    assert "        values" in plan[2]
    assert "            value row (data=ROW(0::int))" in plan[3]
    assert "execution options:" in plan[4]

    cur.execute(query)
    cur.execute("explain " + query)
    plan = cur.fetchall()
    assert 'insert "explain" on conflict: fail' in plan[0]
    assert '    motion [policy: segment([ref("COLUMN_1")]), program: ReshardIfNeeded]' in plan[1]
    assert "        values" in plan[2]
    assert "            value row (data=ROW(0::int))" in plan[3]
    assert "execution options:" in plan[4]

    query = """
        select * from "explain";
    """
    cur.execute("explain " + query)
    plan = cur.fetchall()
    assert 'projection ("explain"."id"::int -> "id")' in plan[0]
    assert '    scan "explain"' in plan[1]
    assert "execution options:" in plan[2]

    cur.execute(query)
    cur.execute("explain " + query)
    plan = cur.fetchall()
    assert 'projection ("explain"."id"::int -> "id")' in plan[0]
    assert '    scan "explain"' in plan[1]
    assert "execution options:" in plan[2]

    cur.execute('drop table "explain";')


# test that we can handle aggregate functions returning decimals
def test_aggregates(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}'")

    os.environ["PGSSLMODE"] = "disable"
    conn = pg.Connection(user, password=password, host=postgres.host, port=postgres.port)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
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

    cur.execute(
        """
        INSERT INTO "tall" ("id", "str") VALUES (1, 'apple')
        """
    )

    # integer decimal
    cur.execute(
        """
        SELECT SUM("id") FROM "tall";
    """
    )
    assert cur.fetchall() == ([Decimal(1)],)

    cur.execute(
        """
        INSERT INTO "tall" ("id", "str") VALUES (2, 'banana')
        """
    )
    # floating decimal
    cur.execute(
        """
        SELECT AVG("id") FROM "tall";
    """
    )
    assert cur.fetchall() == ([Decimal(1.5)],)

    cur.execute(
        """
        INSERT INTO "tall" ("id", "str") VALUES (3, 'cherry')
        """
    )

    # Test group_concat
    cur.execute(
        """
        SELECT group_concat("str", ',') FROM "tall";
        """
    )
    group_concat_result = cur.fetchall()[0][0]

    # Test string_agg
    cur.execute(
        """
        SELECT string_agg("str", ',') FROM "tall";
        """
    )
    string_agg_result = cur.fetchall()[0][0]

    # Verify both functions produce identical results
    assert group_concat_result == string_agg_result
    assert group_concat_result == "apple,banana,cherry"

    # Test with GROUP BY
    cur.execute(
        """
        SELECT "bool", group_concat("str", ',') FROM "tall" GROUP BY "bool";
        """
    )
    group_concat_grouped = cur.fetchall()

    cur.execute(
        """
        SELECT "bool", string_agg("str", ',') FROM "tall" GROUP BY "bool";
        """
    )
    string_agg_grouped = cur.fetchall()

    # Verify grouped results are identical
    assert group_concat_grouped == string_agg_grouped


def test_empty_queries(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}'")

    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    cur = conn.execute("  ", prepare=False)
    assert cur.pgresult is not None
    assert cur.pgresult.status == ExecStatus.EMPTY_QUERY

    cur = conn.execute(" ; ", prepare=False)
    assert cur.pgresult is not None
    assert cur.pgresult.status == ExecStatus.EMPTY_QUERY


def test_deallocate(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}'")

    os.environ["PGSSLMODE"] = "disable"
    conn = pg2.Connection(user, password=password, host=postgres.host, port=postgres.port)
    conn.autocommit = True

    # Remove unprepared statement
    statement_name = "not_existing_name"
    with pytest.raises(DatabaseError, match=f"prepared statement {statement_name} does not exist."):
        conn.run(f"DEALLOCATE {statement_name}")

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
    conn.run(f"DEALLOCATE {statement_name}")

    # run a closed statement
    with pytest.raises(DatabaseError, match="Couldn't find statement"):
        ps.run()

    # Remove statements with DEALLOCATE ALL
    ps1 = conn.prepare("SELECT 1")
    ps1.run()

    ps2 = conn.prepare("SELECT 1")
    ps2.run()

    conn.run("DEALLOCATE ALL")

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
    assert sorted(rows, key=lambda x: x[0]) == [(1, "Alice"), (2, "Bob")]

    cur = conn.execute("DROP TABLE test_table;")

    # Without autocommit
    conn.autocommit = False

    cur = conn.execute("BEGIN;", prepare=False)
    assert cur.pgresult is not None
    assert cur.pgresult.status == ExecStatus.COMMAND_OK

    cur = conn.execute("CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT);")

    cur = conn.execute("INSERT INTO test_table (id, name) VALUES (1,'Alice'), (2,'Bob');")

    cur = conn.execute("SELECT * FROM test_table;")
    rows = cur.fetchall()
    assert sorted(rows, key=lambda x: x[0]) == [(1, "Alice"), (2, "Bob")]

    cur = conn.execute("ROLLBACK;", prepare=False)
    assert cur.pgresult is not None
    assert cur.pgresult.status == ExecStatus.COMMAND_OK

    # ROLLBACK does nothing, the table is still created and is populated
    cur = conn.execute("SELECT * FROM test_table;")
    rows = cur.fetchall()
    assert sorted(rows, key=lambda x: x[0]) == [(1, "Alice"), (2, "Bob")]

    cur = conn.execute("DROP TABLE test_table;")

    cur = conn.execute("BEGIN;", prepare=False)
    assert cur.pgresult is not None
    assert cur.pgresult.status == ExecStatus.COMMAND_OK

    cur = conn.execute("CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT);")

    cur = conn.execute("INSERT INTO test_table (id, name) VALUES (1,'Alice'), (2,'Bob');")

    cur = conn.execute("COMMIT;", prepare=False)
    assert cur.pgresult is not None
    assert cur.pgresult.status == ExecStatus.COMMAND_OK

    cur = conn.execute("SELECT * FROM test_table;")
    rows = cur.fetchall()
    assert sorted(rows, key=lambda x: x[0]) == [(1, "Alice"), (2, "Bob")]

    cur = conn.execute("DROP TABLE test_table;")

    # now test the same, but using END instead of COMMIT
    cur = conn.execute("BEGIN;", prepare=False)
    assert cur.pgresult is not None
    assert cur.pgresult.status == ExecStatus.COMMAND_OK

    cur = conn.execute("CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT);")

    cur = conn.execute("INSERT INTO test_table (id, name) VALUES (1,'Alice'), (2,'Bob');")

    cur = conn.execute("END;", prepare=False)
    assert cur.pgresult is not None
    assert cur.pgresult.status == ExecStatus.COMMAND_OK

    cur = conn.execute("SELECT * FROM test_table;")
    rows = cur.fetchall()
    assert sorted(rows, key=lambda x: x[0]) == [(1, "Alice"), (2, "Bob")]

    cur = conn.execute("DROP TABLE test_table;")


def test_create_schema(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port
    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}'")

    conn = psycopg.connect(f"user={user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    cur = conn.execute("CREATE SCHEMA test_schema;", prepare=False)
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

    cur = conn.execute("DROP SCHEMA test_schema;", prepare=False)
    assert cur.pgresult is not None
    assert cur.pgresult.status == ExecStatus.COMMAND_OK
    assert cur.statusmessage == "DROP SCHEMA"


def test_procedures(postgres: Postgres):
    user = "postgres"
    password = "Passw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)
    postgres.instance.sql(f'GRANT CREATE PROCEDURE TO "{user}"', sudo=True)
    conn = psycopg.connect(f"user={user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    # Create table
    conn.execute("CREATE TABLE t (a int primary key)")

    # Create proc and run it
    conn.execute("CREATE PROCEDURE proc(int) AS $$INSERT INTO t VALUES($1)$$;")
    conn.execute("CALL proc(1)")

    # Ensure procedure's been executed
    cur = conn.execute("SELECT * FROM t")
    assert cur.fetchall() == [(1,)]

    # Alter proc name and call it
    conn.execute("ALTER PROCEDURE proc RENAME TO proc2;")
    conn.execute("CALL proc2(2)")

    # Ensure procedure's been executed
    cur = conn.execute("SELECT * FROM t")
    assert sorted(cur.fetchall()) == [(1,), (2,)]

    # Test procedure dropping
    conn.execute("DROP PROCEDURE proc2")
