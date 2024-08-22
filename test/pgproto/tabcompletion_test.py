import shutil
import sys
import pexpect  # type: ignore
import pytest
from conftest import Postgres


def test_tabcompletion(postgres: Postgres):
    if not shutil.which("psql"):
        pytest.skip("couldn't find psql")

    user = "postgres"
    password = "Passw0rd"
    host = postgres.host
    port = postgres.port

    # create a postgres user using a postgres compatible password
    postgres.instance.sql(
        f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5"
    )

    # create a table
    postgres.instance.sql(
        """
        CREATE TABLE T (
            S TEXT NOT NULL,
            PRIMARY KEY (S)
        )
        USING MEMTX DISTRIBUTED BY (S);
        """
    )

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
