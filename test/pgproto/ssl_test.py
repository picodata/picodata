import pytest
import pg8000.dbapi as pg  # type: ignore
from conftest import Postgres
import os
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
    # where the server should find .crt and .key files
    instance = postgres_with_tls.instance
    host = postgres_with_tls.host
    port = postgres_with_tls.port
    ssl_dir = "ssl_dir"
    assert ssl_dir is not None

    user = "user"
    password = "P@ssw0rd"
    instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5")

    # where the client should find his certificate
    client_cert_file = os.path.join(ssl_dir, "root.crt")
    os.environ["SSL_CERT_FILE"] = client_cert_file

    os.environ["PGSSLMODE"] = "require"
    conn = psycopg.connect(
        f"user = {user} password={password} host={host} port={port} sslmode=require"
    )
    conn.close()
