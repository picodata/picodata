import pytest
import pg8000.dbapi as pg  # type: ignore
from conftest import Postgres
import os


def test_ssl_request_handling(postgres: Postgres):
    host = "127.0.0.1"
    port = 5432

    postgres.start(host, port)
    i1 = postgres.instance

    user = "user"
    password = "password"
    i1.eval("box.cfg{auth_type='md5'}")
    i1.call("pico.create_user", user, password, dict(timeout=3))

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
