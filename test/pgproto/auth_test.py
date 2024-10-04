import pytest
import pg8000.dbapi as pg  # type: ignore
from conftest import Postgres, Cluster, log_crawler


def test_auth(postgres: Postgres):
    i1 = postgres.instance

    user = "user"
    password = "P@ssw0rd"
    i1.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")

    # test successful authentication
    conn = pg.Connection(
        user, password=password, host=postgres.host, port=postgres.port
    )
    conn.close()

    # test authentication with a wrong password
    with pytest.raises(
        pg.DatabaseError, match=f"authentication failed for user '{user}'"
    ):
        pg.Connection(
            user, password="wrong password", host=postgres.host, port=postgres.port
        )

    # test authentication with an unknown user
    with pytest.raises(
        pg.DatabaseError, match="authentication failed for user 'unknown-user'"
    ):
        pg.Connection(
            "unknown-user", password="aaa", host=postgres.host, port=postgres.port
        )

    sha_user = "chap-sha-enjoyer"
    password = "P@ssw0rd"
    i1.sql(f"CREATE USER \"{sha_user}\" WITH PASSWORD '{password}'")

    # test authentication with an unsupported method
    with pytest.raises(
        pg.DatabaseError, match=f"authentication failed for user '{sha_user}'"
    ):
        pg.Connection(sha_user, password="aaa", host=postgres.host, port=postgres.port)


def test_admin_auth(cluster: Cluster):
    import os

    os.environ["PICODATA_ADMIN_PASSWORD"] = "#AdminX12345"

    cluster.set_config_file(
        yaml="""
    cluster:
        cluster_name: test
        tier:
            default:
    instance:
        pg:
            listen: "127.0.0.1:5442"
            ssl: False
    """
    )
    i1 = cluster.add_instance(wait_online=False)
    i1.env.update(os.environ)
    i1.start()
    i1.wait_online()

    user = "admin"
    password = os.getenv("PICODATA_ADMIN_PASSWORD")

    # test authentication with a wrong password
    with pytest.raises(
        pg.DatabaseError, match=f"authentication failed for user '{user}'"
    ):
        pg.Connection(user, password="wrong password", host="127.0.0.1", port=5442)

    conn = pg.Connection(user=user, password=password, host="127.0.0.1", port=5442)
    conn.close()


def test_user_blocking_after_a_series_of_unsuccessful_auth_attempts(cluster: Cluster):
    user = "user"
    password = "P@ssw0rd"
    host = "127.0.0.1"
    port = "5433"

    cluster.set_config_file(
        yaml=f"""
    cluster:
        cluster_id: test
        tier:
            default:
    instance:
        pg:
            listen: "{host}:{port}"
    """
    )

    i1 = cluster.add_instance(wait_online=False)
    user_banned_lc = log_crawler(
        i1, "Maximum number of login attempts exceeded; user blocked"
    )
    i1.start()
    i1.wait_online()

    # create a user to connect
    i1.sql(f"CREATE USER {user} WITH PASSWORD '{password}'")

    # check if the user can connect
    conn = pg.Connection(user, password=password, host=host, port=port)
    conn.close()

    # connect many times with an invalid password to block the user
    for _ in range(4):
        with pytest.raises(
            pg.DatabaseError, match=f"authentication failed for user '{user}'"
        ):
            pg.Connection(user, password="WrongPassword", host=host, port=port)

    # the user is blocked now
    with pytest.raises(
        pg.DatabaseError, match=f"authentication failed for user '{user}'"
    ):
        pg.Connection(user, password=password, host=host, port=port)

    # and we can see it in the audit log
    user_banned_lc.wait_matched()


def test_user_auth_failure_counter_resets_on_success(postgres: Postgres):
    i1 = postgres.instance

    user = "user"
    password = "P@ssw0rd"
    i1.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")

    # user is banned after 4 failures in a row

    # fail 3 times
    for _ in range(3):
        with pytest.raises(
            pg.DatabaseError, match=f"authentication failed for user '{user}'"
        ):
            pg.Connection(
                user, password="WrongPassword", host=postgres.host, port=postgres.port
            )

    # reset the failure counter by a successful authentication
    pg.Connection(user, password=password, host=postgres.host, port=postgres.port)

    # fail 3 more times after the reset
    for _ in range(3):
        with pytest.raises(
            pg.DatabaseError, match=f"authentication failed for user '{user}'"
        ):
            pg.Connection(
                user, password="WrongPassword", host=postgres.host, port=postgres.port
            )

    # check if the user is not banned
    pg.Connection(user, password=password, host=postgres.host, port=postgres.port)
