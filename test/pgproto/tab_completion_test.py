import shutil
import sys
import pexpect  # type: ignore
import pytest
import subprocess
import re
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
