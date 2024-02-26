import pytest
import pg8000.dbapi as pg  # type: ignore
from conftest import Postgres
import os


def test_ssl_refuse(postgres: Postgres):
    host = "127.0.0.1"
    port = 5432

    postgres.start(host, port)
    i1 = postgres.instance

    user = "user"
    password = "P@ssw0rd"
    i1.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5")

    # disable: only try a non-SSL connection
    os.environ["PGSSLMODE"] = "disable"
    conn = pg.Connection(user, password=password, host=host, port=port)
    conn.close()

    # prefer: first try an SSL connection; if that fails,
    #         try a non-SSL connection.
    # As ssl is not supported, server will respond to SslRequest with
    # SslRefuse and client will try a non-SSL connection.
    os.environ["PGSSLMODE"] = "prefer"
    conn = pg.Connection(user, password=password, host=host, port=port)
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
        pg.Connection(user, password="wrong password", host=host, port=port)


def test_ssl_accept(postgres: Postgres):
    host = "127.0.0.1"
    port = 5432
    # where the server should find .crt and .key files
    pgdata = "test/pgdata"
    instance = postgres.instance

    postgres.start(host, port, pgdata)

    user = "user"
    password = "P@ssw0rd"
    instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5")

    # where the client should find his certificate
    client_cert_file = os.path.join(os.path.abspath(pgdata), "root.crt")
    os.environ["SSL_CERT_FILE"] = client_cert_file

    os.environ["PGSSLMODE"] = "require"
    conn = pg.Connection(
        user, password=password, host=host, port=port, ssl_context=True
    )
    conn.close()
