import pytest
import pg8000.dbapi as pg  # type: ignore
from conftest import Postgres


def test_auth(postgres: Postgres):
    host = "127.0.0.1"
    port = 5432

    postgres.start(host, port)
    i1 = postgres.instance

    user = "user"
    password = "P@ssw0rd"
    i1.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5")

    # test successful authentication
    conn = pg.Connection(user, password=password, host=host, port=port)
    conn.close()

    # test authentication with a wrong password
    with pytest.raises(
        pg.DatabaseError, match=f"authentication failed for user '{user}'"
    ):
        pg.Connection(user, password="wrong password", host=host, port=port)

    # test authentication with an unknown user
    with pytest.raises(
        pg.DatabaseError, match="authentication failed for user 'unknown-user'"
    ):
        pg.Connection("unknown-user", password="aaa", host=host, port=port)

    sha_user = "chap-sha-enjoyer"
    password = "P@ssw0rd"
    i1.sql(f"CREATE USER \"{sha_user}\" WITH PASSWORD '{password}' USING md5")

    # test authentication with an unsupported method
    with pytest.raises(
        pg.DatabaseError, match=f"authentication failed for user '{sha_user}'"
    ):
        pg.Connection(sha_user, password="aaa", host=host, port=port)
