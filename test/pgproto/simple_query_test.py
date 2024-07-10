import pytest
import pg8000.dbapi as pg  # type: ignore
import os
from conftest import Postgres
from decimal import Decimal


def test_simple_query_flow_errors(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}' USING md5")

    os.environ["PGSSLMODE"] = "disable"
    conn = pg.Connection(
        user, password=password, host=postgres.host, port=postgres.port
    )
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
            INSERT INTO book VALUES (1, 2);
        """
        )


def test_simple_flow_session(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}' USING md5")

    os.environ["PGSSLMODE"] = "disable"
    conn = pg.Connection(
        user, password=password, host=postgres.host, port=postgres.port
    )
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
    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}' USING md5")

    os.environ["PGSSLMODE"] = "disable"
    conn = pg.Connection(
        user, password=password, host=postgres.host, port=postgres.port
    )
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
    assert '    motion [policy: segment([ref("COLUMN_1")])]' in plan[1]
    assert "        values" in plan[2]
    assert "            value row (data=ROW(0::unsigned))" in plan[3]
    assert "execution options:" in plan[4]

    cur.execute(query)
    cur.execute("explain " + query)
    plan = cur.fetchall()
    assert 'insert "explain" on conflict: fail' in plan[0]
    assert '    motion [policy: segment([ref("COLUMN_1")])]' in plan[1]
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


# test that we can handle aggregate functions returning decimals
def test_aggregates(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}' USING md5")

    os.environ["PGSSLMODE"] = "disable"
    conn = pg.Connection(
        user, password=password, host=postgres.host, port=postgres.port
    )
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
        INSERT INTO "tall" ("id") VALUES (1)
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
        INSERT INTO "tall" ("id") VALUES (2)
        """
    )
    # floating decimal
    cur.execute(
        """
        SELECT AVG("id") FROM "tall";
    """
    )
    assert cur.fetchall() == ([Decimal(1.5)],)
