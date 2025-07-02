import psycopg
import pytest
from conftest import Postgres


def test_sql_vdbe_opcode_max_and_sql_motion_row_max_options(postgres: Postgres):
    user = "postgres"
    password = "Passw0rd"
    admin_password = "T0psecret"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f"ALTER USER \"admin\" WITH PASSWORD '{admin_password}'")

    # Note that "sql_motion_row_max%3D1" is an escaped version of "sql_motion_row_max=1".

    # Set the default for "sql_motion_row_max" to 1.
    conn = psycopg.connect(
        f"postgres://{user}:{password}@{host}:{port}?options=sql_motion_row_max%3D1",
        autocommit=True,
    )
    with pytest.raises(
        psycopg.InternalError,
        match=r"Exceeded maximum number of rows \(1\) in virtual table: 2",
    ):
        conn.execute("SELECT * FROM (VALUES (1), (2))")

    # Check if it still fails with "sql_vdbe_opcode_max" provided.
    with pytest.raises(
        psycopg.InternalError,
        match=r"Exceeded maximum number of rows \(1\) in virtual table: 2",
    ):
        conn.execute("SELECT * FROM (VALUES (1), (2)) OPTION (sql_vdbe_opcode_max = 1000)")

    # Specify "sql_motion_row_max" in a query so the default is not used.
    conn.execute("SELECT * FROM (VALUES (1), (2)) OPTION (sql_motion_row_max = 2)")

    # Set the default for "sql_vdbe_opcode_max" to 1.
    conn = psycopg.connect(
        f"postgres://{user}:{password}@{host}:{port}?options=sql_vdbe_opcode_max%3D1",
        autocommit=True,
    )
    with pytest.raises(
        psycopg.InternalError,
        match="Reached a limit on max executed vdbe opcodes. Limit: 1",
    ):
        conn.execute("SELECT * FROM (VALUES (1), (2))")

    # Check if it still fails with "sql_motion_row_max" provided.
    with pytest.raises(
        psycopg.InternalError,
        match="Reached a limit on max executed vdbe opcodes. Limit: 1",
    ):
        conn.execute("SELECT * FROM (VALUES (1), (2)) OPTION (sql_motion_row_max = 1000)")

    # Specify "sql_vdbe_opcode_max" in a query so the default is not used.
    conn.execute("SELECT * FROM (VALUES (1), (2)) OPTION (sql_vdbe_opcode_max = 1000)")

    # Set both options and reach "sql_vdbe_opcode_max" limit.
    conn = psycopg.connect(
        f"postgres://{user}:{password}@{host}:{port}?options=sql_motion_row_max%3D1,sql_vdbe_opcode_max%3D1",
        autocommit=True,
    )
    with pytest.raises(
        psycopg.InternalError,
        match=r"Reached a limit on max executed vdbe opcodes. Limit: 1",
    ):
        conn.execute("SELECT * FROM (VALUES (1), (2))")

    # Set both options and reach "sql_motion_row_max" limit.
    conn = psycopg.connect(
        f"postgres://{user}:{password}@{host}:{port}?options=sql_motion_row_max%3D1,sql_vdbe_opcode_max%3D1000",
        autocommit=True,
    )
    with pytest.raises(
        psycopg.InternalError,
        match=r"Exceeded maximum number of rows \(1\) in virtual table: 2",
    ):
        conn.execute("SELECT * FROM (VALUES (1), (2))")

    # Session values has higher priority than `ALTER SYSTEM`
    conn = psycopg.connect(
        f"postgres://admin:{admin_password}@{host}:{port}?options=sql_motion_row_max%3D1",
        autocommit=True,
    )

    conn.execute("ALTER SYSTEM SET sql_motion_row_max = 10;")

    with pytest.raises(
        psycopg.InternalError,
        match=r"Exceeded maximum number of rows \(1\) in virtual table: 2",
    ):
        conn.execute("SELECT * FROM (VALUES (1), (2))")

    # Check that system options are actually applied
    conn = psycopg.connect(
        f"postgres://admin:{admin_password}@{host}:{port}",
        autocommit=True,
    )
    conn.execute("ALTER SYSTEM SET sql_motion_row_max = 1;")
    with pytest.raises(
        psycopg.InternalError,
        match=r"Exceeded maximum number of rows \(1\) in virtual table: 2",
    ):
        conn.execute("SELECT * FROM (VALUES (1), (2))")
    conn.execute("ALTER SYSTEM SET sql_motion_row_max = 10;")
    conn.execute("ALTER SYSTEM SET sql_vdbe_opcode_max = 1;")
    with pytest.raises(
        psycopg.InternalError,
        match=r"Reached a limit on max executed vdbe opcodes. Limit: 1",
    ):
        conn.execute("SELECT * FROM (VALUES (1), (2))")


def test_repeating_options(postgres: Postgres):
    user = "postgres"
    password = "Passw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")

    # Note that "sql_motion_row_max%3D1" is an escaped version of "sql_motion_row_max=1".

    # Check if the last option value is applied (3 -> 2 -> 1).
    conn = psycopg.connect(
        f"postgres://{user}:{password}@{host}:{port}?"
        "options=sql_motion_row_max%3D3,sql_motion_row_max%3D2,"
        "sql_motion_row_max%3D1",
        autocommit=True,
    )
    with pytest.raises(
        psycopg.InternalError,
        match=r"Exceeded maximum number of rows \(1\) in virtual table: 2",
    ):
        conn.execute("SELECT * FROM (VALUES (1), (2))")
