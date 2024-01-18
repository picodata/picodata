import pytest
import pg8000.native as pg  # type: ignore
import os
from conftest import Postgres
from conftest import ReturnError
from pg8000.exceptions import DatabaseError  # type: ignore


def test_extended_query(postgres: Postgres):
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
    with pytest.raises(ReturnError, match="space TALL not found"):
        i1.sql(""" select * from tall """)

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
