import psycopg
import pytest
from conftest import Postgres
from inline_snapshot import snapshot


# https://git.picodata.io/core/picodata/-/issues/1991
def test_gl_1991(postgres: Postgres):
    user = "postgres"
    password = "Passw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)
    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    conn.execute(
        """
        CREATE TABLE t(id int primary key) OPTION(timeout = 3);
    """
    )

    # Set sql_vdbe_opcode_max to 1
    conn = psycopg.connect(
        f"postgres://{user}:{password}@{host}:{port}?options=sql_vdbe_opcode_max%3D1",
        autocommit=True,
    )

    cur = conn.execute(
        """
        EXPLAIN SELECT * FROM "t" WHERE 1 = 0;
        """,
    )

    # Expect that selection filter evaluates to
    # false and buckets set is empty.
    plan = cur.fetchall()
    assert "\n".join(row[0] for row in plan) == snapshot("""\
projection (t.id::int -> id)
  selection (false::bool)
    scan t
execution options:
  sql_vdbe_opcode_max = 1
  sql_motion_row_max = 5000
buckets = []\
""")

    # Check that query returns empty result
    cur = conn.execute("SELECT * FROM t WHERE 1 = 0")
    assert cur.fetchall() == []

    with pytest.raises(
        psycopg.InternalError,
        match=r"Reached a limit on max executed vdbe opcodes. Limit: 1",
    ):
        conn.execute("SELECT * FROM t WHERE 1 = 1")
