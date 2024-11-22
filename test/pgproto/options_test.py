import psycopg
import pytest
from conftest import Postgres


def test_vdbe_max_steps_and_vtable_max_rows_options(postgres: Postgres):
    user = "postgres"
    password = "Passw0rd"
    admin_password = "T0psecret"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f"ALTER USER \"admin\" WITH PASSWORD '{admin_password}'")

    # Note that "vtable_max_rows%3D1" is an escaped version of "vtable_max_rows=1".

    # Set the default for "vtable_max_rows" to 1.
    conn = psycopg.connect(
        f"postgres://{user}:{password}@{host}:{port}?options=vtable_max_rows%3D1",
        autocommit=True,
    )
    with pytest.raises(
        psycopg.InternalError,
        match=r"Exceeded maximum number of rows \(1\) in virtual table: 2",
    ):
        conn.execute("SELECT * FROM (VALUES (1), (2))")

    # Check if it still fails with "vdbe_max_steps" provided.
    with pytest.raises(
        psycopg.InternalError,
        match=r"Exceeded maximum number of rows \(1\) in virtual table: 2",
    ):
        conn.execute("SELECT * FROM (VALUES (1), (2)) OPTION (VDBE_MAX_STEPS = 1000)")

    # Specify "vtable_max_rows" in a query so the default is not used.
    conn.execute("SELECT * FROM (VALUES (1), (2)) OPTION (VTABLE_MAX_ROWS = 2)")

    # Set the default for "vdbe_max_steps" to 1.
    conn = psycopg.connect(
        f"postgres://{user}:{password}@{host}:{port}?options=vdbe_max_steps%3D1",
        autocommit=True,
    )
    with pytest.raises(
        psycopg.InternalError,
        match="Reached a limit on max executed vdbe opcodes. Limit: 1",
    ):
        conn.execute("SELECT * FROM (VALUES (1), (2))")

    # Check if it still fails with "vtable_max_rows" provided.
    with pytest.raises(
        psycopg.InternalError,
        match="Reached a limit on max executed vdbe opcodes. Limit: 1",
    ):
        conn.execute("SELECT * FROM (VALUES (1), (2)) OPTION (VTABLE_MAX_ROWS = 1000)")

    # Specify "vdbe_max_steps" in a query so the default is not used.
    conn.execute("SELECT * FROM (VALUES (1), (2)) OPTION (VDBE_MAX_STEPS = 1000)")

    # Set both options and reach "vdbe_max_steps" limit.
    conn = psycopg.connect(
        f"postgres://{user}:{password}@{host}:{port}?"
        "options=vtable_max_rows%3D1,vdbe_max_steps%3D1",
        autocommit=True,
    )
    with pytest.raises(
        psycopg.InternalError,
        match=r"Reached a limit on max executed vdbe opcodes. Limit: 1",
    ):
        conn.execute("SELECT * FROM (VALUES (1), (2))")

    # Set both options and reach "vtable_max_rows" limit.
    conn = psycopg.connect(
        f"postgres://{user}:{password}@{host}:{port}?"
        "options=vtable_max_rows%3D1,vdbe_max_steps%3D1000",
        autocommit=True,
    )
    with pytest.raises(
        psycopg.InternalError,
        match=r"Exceeded maximum number of rows \(1\) in virtual table: 2",
    ):
        conn.execute("SELECT * FROM (VALUES (1), (2))")

    # Session values has higher priority than `ALTER SYSTEM`
    conn = psycopg.connect(
        f"postgres://admin:{admin_password}@{host}:{port}?"
        "options=vtable_max_rows%3D1",
        autocommit=True,
    )

    conn.execute("ALTER SYSTEM SET VTABLE_MAX_ROWS = 10;")

    with pytest.raises(
        psycopg.InternalError,
        match=r"Exceeded maximum number of rows \(1\) in virtual table: 2",
    ):
        conn.execute("SELECT * FROM (VALUES (1), (2))")


def test_repeating_options(postgres: Postgres):
    user = "postgres"
    password = "Passw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")

    # Note that "vtable_max_rows%3D1" is an escaped version of "vtable_max_rows=1".

    # Check if the last option value is applied (3 -> 2 -> 1).
    conn = psycopg.connect(
        f"postgres://{user}:{password}@{host}:{port}?"
        "options=vtable_max_rows%3D3,vtable_max_rows%3D2,"
        "vtable_max_rows%3D1",
        autocommit=True,
    )
    with pytest.raises(
        psycopg.InternalError,
        match=r"Exceeded maximum number of rows \(1\) in virtual table: 2",
    ):
        conn.execute("SELECT * FROM (VALUES (1), (2))")
