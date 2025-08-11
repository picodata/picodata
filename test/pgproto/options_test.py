import psycopg
import pytest
from conftest import Postgres
from psycopg import RawCursor


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


def test_repeating_connection_options(postgres: Postgres):
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


def test_invalid_sql_options(postgres: Postgres):
    user = "postgres"
    password = "Passw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")

    conn = psycopg.connect(f"postgres://{user}:{password}@{host}:{port}", autocommit=True)

    with pytest.raises(
        psycopg.InternalError,
        match=r"sbroad: invalid query: option sql_motion_row_max specified more than once!",
    ):
        conn.execute("""
                     SELECT * FROM (VALUES (1), (2)) OPTION (
                       sql_motion_row_max = 3,
                       sql_motion_row_max = 2,
                       sql_vdbe_opcode_max = 1,
                       sql_vdbe_opcode_max = 2
                     )""")

    with pytest.raises(
        psycopg.InternalError,
        match=r"sbroad: invalid query: option sql_vdbe_opcode_max specified more than once!",
    ):
        conn.execute("""
                     SELECT * FROM (VALUES (1), (2)) OPTION (
                       sql_vdbe_opcode_max = 1,
                       sql_vdbe_opcode_max = 2
                     )""")


def test_parametrized_sql_options(postgres: Postgres):
    user = "postgres"
    password = "Passw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")

    conn = psycopg.connect(f"postgres://{user}:{password}@{host}:{port}", autocommit=True)

    q1 = """
        SELECT * FROM (VALUES (1), (2)) OPTION (
            sql_motion_row_max = %s,
            sql_vdbe_opcode_max = %s
        )
    """

    with pytest.raises(
        psycopg.InternalError,
        match=r"Exceeded maximum number of rows \(1\) in virtual table: 2",
    ):
        # also test out providing strings as parameters. these work due to them having the same on-wire representation as integers
        conn.execute(q1, ["1", "200"])
    with pytest.raises(
        psycopg.InternalError,
        match=r"Reached a limit on max executed vdbe opcodes. Limit: 1",
    ):
        conn.execute(q1, [200, 1])


def test_invalid_parametrized_sql_options(postgres: Postgres):
    user = "postgres"
    password = "Passw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")

    conn = psycopg.connect(f"postgres://{user}:{password}@{host}:{port}", autocommit=True)

    q1 = """
         SELECT * FROM (VALUES (1), (2)) OPTION (
            sql_motion_row_max = $1
         )
         """

    # use RawCursor to prevent the driver from erroring out due to invalid number of parameters
    with RawCursor(conn) as cur:
        # insufficient params
        with pytest.raises(
            psycopg.errors.ProtocolViolation,
            match="bind message supplies 0 parameters, but prepared statement .* requires 1",
        ):
            cur.execute(q1, [], prepare=True)

        # excessive params
        with pytest.raises(
            psycopg.errors.ProtocolViolation,
            match="bind message supplies 2 parameters, but prepared statement .* requires 1",
        ):
            cur.execute(q1, [42, 721077], prepare=True)

        # NOTE: these errors are only triggerable when using parameters, because literals are limited to be unsigned at parse time
        # a string gets rejected early by pgproto
        with pytest.raises(
            psycopg.errors.InvalidTextRepresentation,
            match=r"failed to bind parameter \$1: decoding error: 'много' is not a valid int8",
        ):
            cur.execute(q1, ["много"])
        # a negative integer only gets rejected during option lowering
        with pytest.raises(
            psycopg.InternalError,
            match=r"sbroad: invalid OptionSpec: expected option sql_motion_row_max to be either an unsigned or non-negative integer got: Integer\(-1\)",
        ):
            cur.execute(q1, [-1])
