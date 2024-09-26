import shutil
import sys
import pexpect  # type: ignore
import pytest
import subprocess
import re
import os
import psycopg
from conftest import Postgres
from packaging.version import Version  # type: ignore


def psql_version() -> Version:
    cmd = ["psql", "--version"]
    output = subprocess.check_output(cmd).decode("utf-8")
    raw_version = re.sub(r"\([^)]*\)", "", output).strip()
    version = raw_version.rpartition(" ")[-1]
    return Version(version)


def test_tab_completion(postgres: Postgres):
    if not shutil.which("psql"):
        pytest.skip("cannot find psql")

    version = psql_version()
    if version < Version("15"):
        pytest.skip(f"unsupported psql {version}")

    user = "postgres"
    password = "Passw0rd"
    host = postgres.host
    port = postgres.port

    # create a postgres user using a postgres compatible password
    postgres.instance.sql(
        f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5"
    )

    # create a table
    postgres.instance.sql(""" create table "T" ("s" int primary key) """)

    # connect using psql
    psql = pexpect.spawn(
        command="psql",
        args=[
            "--quiet",
            "--no-psqlrc",
            f"postgres://{user}:{password}@{host}:{port}?sslmode=disable",
        ],
        env={"LC_ALL": "C"},
        encoding="utf-8",
        timeout=5,
    )
    psql.logfile = sys.stdout
    psql.expect_exact("=>")

    # request the completion by pressing TAB twice
    psql.send("select * from \t\t")
    psql.expect_exact('"T"')
    psql.expect("_pico.*")


def test_completion_query_15_plus(postgres: Postgres):
    def fmt_query(table: str) -> str:
        table = table.replace("_", r"\\_")
        return f"""
            SELECT c.relname,
                   NULL::pg_catalog.text
            FROM pg_catalog.pg_class c
            WHERE c.relkind IN ('r',
                                'S',
                                'v',
                                'm',
                                'f',
                                'p')
              AND (c.relname) LIKE '{table}%'
              AND pg_catalog.pg_table_is_visible(c.oid)
              AND c.relnamespace <>
                (SELECT oid
                 FROM pg_catalog.pg_namespace
                 WHERE nspname = 'pg_catalog')
            UNION ALL
            SELECT NULL::pg_catalog.text,
                   n.nspname
            FROM pg_catalog.pg_namespace n
            WHERE n.nspname LIKE '{table}%'
              AND n.nspname NOT LIKE E'pg\\\\_%'
            LIMIT 1000
        """

    user = "admin"
    password = "P@ssw0rd"
    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}' USING md5")

    os.environ["PGSSLMODE"] = "disable"
    conn = psycopg.connect(
        f"user={user} password={password} host={postgres.host} port={postgres.port}"
    )
    conn.autocommit = True

    conn.execute("create table kek(id int primary key)")

    cur = conn.execute(fmt_query("_pico_table"))
    assert [r[0] for r in cur.fetchall()] == ["_pico_table"]

    cur = conn.execute(fmt_query("kek"))
    assert [r[0] for r in cur.fetchall()] == ["kek"]

    cur = conn.execute(fmt_query("bar"))
    assert [r[0] for r in cur.fetchall()] == []


def test_completion_query_picodata(postgres: Postgres):
    query = """
        select * from (
            select
                name as relname,
                NULL::text as "text"
            from _pico_table
            where name like %s::text || '%%'
        ) q
        order by q.relname
    """

    user = "admin"
    password = "P@ssw0rd"
    postgres.instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}' USING md5")

    os.environ["PGSSLMODE"] = "disable"
    conn = psycopg.connect(
        f"user={user} password={password} host={postgres.host} port={postgres.port}"
    )
    conn.autocommit = True

    conn.execute("create table kek(id int primary key)")

    cur = conn.execute(query, ("_pico_table",))
    assert [r[0] for r in cur.fetchall()] == ["_pico_table"]

    cur = conn.execute(query, ("kek",))
    assert [r[0] for r in cur.fetchall()] == ["kek"]

    cur = conn.execute(query, ("bar",))
    assert [r[0] for r in cur.fetchall()] == []
