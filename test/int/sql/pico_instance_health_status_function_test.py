from conftest import Postgres

from pg8000.native import Connection  # type: ignore
import pg8000.exceptions  # type: ignore
import pytest


def setup_pg_env(postgres: Postgres) -> Connection:
    username = "postgres"
    password = "T0psecret"

    postgres.instance.create_user(
        with_name=username,
        with_password=password,
    )

    connection = Connection(
        user=username,
        password=password,
        host=postgres.host,
        port=postgres.port,
    )
    connection.autocommit = True

    return connection


def test_volatility(postgres: Postgres) -> None:
    connection = setup_pg_env(postgres)

    with pytest.raises(
        expected_exception=pg8000.exceptions.DatabaseError,
        match="volatile function is not allowed in filter clause",
    ):
        connection.run("""
            SELECT *
            FROM _pico_tier
            WHERE pico_instance_health_status(pico_instance_uuid());
        """)

    connection.close()


def test_type(postgres: Postgres) -> None:
    connection = setup_pg_env(postgres)

    connection.run("SELECT pico_instance_health_status(pico_instance_uuid())")

    assert connection.columns is not None
    column = connection.columns[0]

    JSON_TYPE_OID = 114
    assert column["type_oid"] == JSON_TYPE_OID

    connection.close()


def test_indexing(postgres: Postgres) -> None:
    connection = setup_pg_env(postgres)

    result = connection.run("""
        SELECT pico_instance_health_status(pico_instance_uuid())['tier']
    """)
    assert result == [["default"]]

    connection.close()


def test_nullable(postgres: Postgres) -> None:
    connection = setup_pg_env(postgres)

    result = connection.run("SELECT pico_instance_health_status('nothing')")
    assert result == [[None]]

    connection.close()
