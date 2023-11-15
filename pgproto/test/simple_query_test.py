import pytest
import pg8000.dbapi as pg  # type: ignore
import os
from conftest import Postgres


def test_simple_query_flow_errors(postgres: Postgres):
    host = "127.0.0.1"
    port = 5432

    postgres.start(host, port)
    i1 = postgres.instance

    user = "admin"
    password = "fANPIOUWEh79p12hdunqwADI"
    i1.eval("box.cfg{auth_type='md5'}")
    i1.eval(f"box.schema.user.passwd('{user}', '{password}')")

    with pytest.raises(pg.InterfaceError, match="Server refuses SSL"):
        pg.Connection(user, password=password, host=host, port=port, ssl_context=True)

    os.environ["PGSSLMODE"] = "disable"
    conn = pg.Connection(user, password=password, host=host, port=port)
    conn.autocommit = True
    cur = conn.cursor()

    with pytest.raises(
        pg.DatabaseError, match="expected CreateUser, AlterUser, DropUser"
    ):
        cur.execute(
            """
            CREATE TEMPORARY TABLE book (id SERIAL, title TEXT);
        """
        )

    with pytest.raises(pg.DatabaseError, match="space BOOK not found"):
        cur.execute(
            """
            INSERT INTO book VALUES (1, 2);
        """
        )


def test_simple_flow_session(postgres: Postgres):
    host = "127.0.0.1"
    port = 5432

    postgres.start(host, port)
    i1 = postgres.instance

    user = "admin"
    password = "password"
    i1.eval("box.cfg{auth_type='md5'}")
    i1.eval(f"box.schema.user.passwd('{user}', '{password}')")

    os.environ["PGSSLMODE"] = "disable"
    conn = pg.Connection(user, password=password, host=host, port=port)
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
    host = "127.0.0.1"
    port = 5432

    postgres.start(host, port)
    i1 = postgres.instance

    user = "admin"
    password = "password"
    i1.eval("box.cfg{auth_type='md5'}")
    i1.eval(f"box.schema.user.passwd('{user}', '{password}')")

    os.environ["PGSSLMODE"] = "disable"
    conn = pg.Connection(user, password=password, host=host, port=port)
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
    assert '    motion [policy: local segment([ref("COLUMN_1")])]' in plan[1]
    assert "        values" in plan[2]
    assert "            value row (data=ROW(0::unsigned))" in plan[3]
    assert "execution options:" in plan[4]

    cur.execute(query)
    cur.execute("explain " + query)
    plan = cur.fetchall()
    assert 'insert "explain" on conflict: fail' in plan[0]
    assert '    motion [policy: local segment([ref("COLUMN_1")])]' in plan[1]
    assert "        values" in plan[2]
    assert "            value row (data=ROW(0::unsigned))" in plan[3]
    assert "execution options:" in plan[4]

    query = """
        select * from "explain";
    """
    cur.execute("explain " + query)
    plan = cur.fetchall()
    assert 'projection ("explain"."id"::integer -> "id")' in plan[0]
    assert '    scan "explain"' in plan[1]
    assert "execution options:" in plan[2]

    cur.execute(query)
    cur.execute("explain " + query)
    plan = cur.fetchall()
    assert 'projection ("explain"."id"::integer -> "id")' in plan[0]
    assert '    scan "explain"' in plan[1]
    assert "execution options:" in plan[2]

    cur.execute('drop table "explain";')


# Aggregates return value type is decimal, which is currently not supported,
# so an error is expected.
def test_aggregate_error(postgres: Postgres):
    host = "127.0.0.1"
    port = 5432

    postgres.start(host, port)
    i1 = postgres.instance

    user = "admin"
    password = "password"
    i1.eval("box.cfg{auth_type='md5'}")
    i1.eval(f"box.schema.user.passwd('{user}', '{password}')")

    os.environ["PGSSLMODE"] = "disable"
    conn = pg.Connection(user, password=password, host=host, port=port)
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

    with pytest.raises(pg.DatabaseError, match="unknown column type 'decimal'"):
        cur.execute(
            """
            SELECT SUM("id") FROM "tall";
        """
        )

    with pytest.raises(pg.DatabaseError, match="unknown column type 'decimal'"):
        cur.execute(
            """
            SELECT AVG("id") FROM "tall";
        """
        )
