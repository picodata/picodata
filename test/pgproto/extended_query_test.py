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

    # NOTE: psycopg sends parameters types for everything except strings,
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


def test_params_specified_via_cast(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"

    postgres.instance.sql(
        f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5"
    )
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    conn = pg.Connection(
        user, password=password, host=postgres.host, port=postgres.port
    )

    conn.run(
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

    # Types were not specified, so the default type text is used,
    # but parameters binding failed because the right type was integer.
    with pytest.raises(
        DatabaseError, match="could not determine data type of parameter"
    ):
        conn.run(
            """
            INSERT INTO "tall" VALUES (:p1, :p2, :p3, :p4);
            """,
            p1=-2,
            p2="string",
            p3=True,
            p4=3.141592,
        )

    # Now all the types were specified.
    conn.run(
        """
        INSERT INTO "tall" VALUES (:p1::integer, :p2::text, :p3::bool, :p4::double);
        """,
        p1=-2,
        p2="string",
        p3=True,
        p4=3.141592,
    )

    rows = conn.run(""" SELECT * FROM "tall"; """)
    assert rows == [[-2, "string", True, 3.141592]]

    # Test an ambiguous parameter type error.
    with pytest.raises(DatabaseError, match=r"parameter \$1 is ambiguous"):
        conn.run(
            """ SELECT "id" FROM "tall" WHERE "id" = :p1::integer + :p1::unsigned; """,
            p1=-1,
        )

    # Test an even more ambiguous parameter type error.
    with pytest.raises(DatabaseError, match=r"parameter \$1 is ambiguous"):
        conn.run(
            """ SELECT "id" FROM "tall" \
                WHERE "id" = :p1::integer + :p1::unsigned + :p1::number; """,
            p1=-1,
        )

    rows = conn.run(""" SELECT * FROM "tall"; """)
    assert rows == [[-2, "string", True, 3.141592]]

    # Parameter can be cast to the same type in several places.
    rows = conn.run(
        """ SELECT "id" FROM "tall" WHERE "id" = :p1::integer + :p1::integer; """, p1=-1
    )
    assert rows == [[-2]]

    # It's OK to cast parameter only once and then use it without any cast.
    rows = conn.run(
        """ SELECT "id" FROM "tall" WHERE "id" = :p1::integer + :p1 + :p1 - :p1; """,
        p1=-1,
    )
    assert rows == [[-2]]

    # Test that we can calculate the type of an arithmetic expression like $1::integer + $1.
    rows = conn.run(
        """ SELECT :p1::integer + :p1 FROM "tall"; """,
        p1=-1,
    )
    assert rows == [[-2]]
