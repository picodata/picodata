import pytest
import pg8000.dbapi as pg  # type: ignore
from conftest import Postgres
import os
from pathlib import Path
import psycopg


def test_ssl_refuse(postgres: Postgres):
    user = "user"
    password = "P@ssw0rd"
    postgres.instance.sql(
        f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5"
    )

    # disable: only try a non-SSL connection
    os.environ["PGSSLMODE"] = "disable"
    conn = pg.Connection(
        user, password=password, host=postgres.host, port=postgres.port
    )
    conn.close()

    # prefer: first try an SSL connection; if that fails,
    #         try a non-SSL connection.
    # As ssl is not supported, server will respond to SslRequest with
    # SslRefuse and client will try a non-SSL connection.
    os.environ["PGSSLMODE"] = "prefer"
    conn = pg.Connection(
        user, password=password, host=postgres.host, port=postgres.port
    )
    conn.close()

    # require: only try an SSL connection.
    # As ssl is not supported, server will respond to SslRequest with
    # SslRefuse and client won't try to connect again.
    # Client we will see: server does not support SSL, but SSL was required,
    # but client doesn't have to inform the server.
    os.environ["PGSSLMODE"] = "require"
    with pytest.raises(
        pg.DatabaseError, match=f"authentication failed for user '{user}'"
    ):
        pg.Connection(
            user, password="wrong password", host=postgres.host, port=postgres.port
        )


def test_ssl_accept(postgres_with_tls: Postgres):
    conn = psycopg.connect(prepare_with_tls(postgres_with_tls, ""))
    conn.close()


def test_mtls_with_known_cert(postgres_with_mtls: Postgres):
    conn = psycopg.connect(prepare_with_tls(postgres_with_mtls, "server"))
    conn.close()


def test_mtls_without_client_cert(postgres_with_mtls: Postgres):
    with pytest.raises(
        psycopg.OperationalError,
        match="certificate required",
    ):
        conn = psycopg.connect(prepare_with_tls(postgres_with_mtls, ""))
        conn.close()


def test_mtls_with_unknown_cert(postgres_with_mtls: Postgres):
    with pytest.raises(
        psycopg.OperationalError,
        match="unknown ca",
    ):
        conn = psycopg.connect(prepare_with_tls(postgres_with_mtls, "self-signed"))
        conn.close()


def prepare_with_tls(pg: Postgres, client_tls_pair_name: str):
    instance = pg.instance
    host = pg.host
    port = pg.port
    user = "user"
    password = "P@ssw0rd"
    connection_string = f"\
            user = {user} \
            password={password} \
            host={host} \
            port={port} \
            sslmode=require"

    instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5")

    if client_tls_pair_name != "":
        ssl_dir = Path(os.path.realpath(__file__)).parent.parent / "ssl_certs"
        client_cert_path = ssl_dir / (client_tls_pair_name + ".crt")
        client_key_path = ssl_dir / (client_tls_pair_name + ".key")
        connection_string += f" sslcert={client_cert_path} sslkey={client_key_path}"
        os.chmod(client_key_path, 0o600)

    return connection_string
