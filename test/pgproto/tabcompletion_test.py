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
        args=[f"postgres://{user}:{password}@{host}:{port}?sslmode=disable"],
        encoding="utf-8",
        timeout=5,
    )
    psql.logfile = sys.stdout
    psql.expect_exact("psql")
    psql.expect_exact('Type "help" for help.')
    psql.expect_exact("=>")

    # request the completion by pressing TAB twice
    psql.send("select * from \t\t")
    suggestions = [
        "T",
        "_pico_plugin_migration",
        "_pico_service_route",
        "_pico_index",
        "_pico_privilege",
        "_pico_table",
        "_pico_instance",
        "_pico_property",
        "_pico_tier",
        "_pico_peer_address",
        "_pico_replicaset",
        "_pico_user",
        "_pico_plugin",
        "_pico_routine",
        "_pico_plugin_config",
        "_pico_service",
    ]
    for suggestion in suggestions:
        psql.expect_exact(suggestion)
