import pytest
import pg8000.native as pg  # type: ignore
import os
from conftest import Postgres
from pg8000.exceptions import DatabaseError  # type: ignore

# We use psycopg for parameterized queries because pg8000
# doesn't send parameter oids, so we cannot decode them.
# Postgres can infer the types from the context, but we can't do it.
# So we treat any unspecified param id as text, just like pg does when
# it cannot infer the type. It works with psycopg because it sends ids for
# all the types, except strings (it cannot decide what type to use: TEXT or VARCHAR)
# And we can't use TEXT everywhere, because we'll get a type mismatch error
# if we try to bind a string when, for example, an integer is expected.
import psycopg


def test_extended_query(postgres: Postgres):
    user = "admin"
    password = "P@ssw0rd"
    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}' USING md5")

    os.environ["PGSSLMODE"] = "disable"
    conn = pg.Connection(
        user, password=password, host=postgres.host, port=postgres.port
    )

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
        conn.run(""" select * from tall """)

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

    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}' USING md5")

    conn = psycopg.connect(
        f"user = {user} password={password} host={host} port={port} sslmode=disable"
    )
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

    params1 = (-1, "string", True, 3.141592)
    # text parameters
    conn.execute(
        """
        INSERT INTO "tall" VALUES (%t, %t, %t, %t);
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
        INSERT INTO "tall" VALUES (%b, %b, %b, %b);
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
