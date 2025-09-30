import pytest
import pg8000.dbapi as pg  # type: ignore
from conftest import Postgres, Cluster, log_crawler, Instance
from framework.ldap import LdapServer, is_glauth_available
from framework.port_distributor import PortDistributor


def test_auth(postgres: Postgres):
    i1 = postgres.instance

    user = "user"
    password = "P@ssw0rd"
    i1.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")

    # test successful authentication
    conn = pg.Connection(user, password=password, host=postgres.host, port=postgres.port)
    conn.close()

    # test authentication with a wrong password
    with pytest.raises(pg.DatabaseError, match=f"authentication failed for user '{user}'"):
        pg.Connection(user, password="wrong password", host=postgres.host, port=postgres.port)

    # test authentication with an unknown user
    with pytest.raises(pg.DatabaseError, match="authentication failed for user 'unknown-user'"):
        pg.Connection("unknown-user", password="aaa", host=postgres.host, port=postgres.port)

    sha_user = "chap-sha-enjoyer"
    password = "P@ssw0rd"
    i1.sql(f"CREATE USER \"{sha_user}\" WITH PASSWORD '{password}'")

    # test authentication with an unsupported method
    with pytest.raises(pg.DatabaseError, match=f"authentication failed for user '{sha_user}'"):
        pg.Connection(sha_user, password="aaa", host=postgres.host, port=postgres.port)


def test_admin_auth(cluster: Cluster):
    import os

    os.environ["PICODATA_ADMIN_PASSWORD"] = "#AdminX12345"

    i1 = cluster.add_instance(wait_online=False, pg_port=5442)
    i1.env.update(os.environ)
    i1.start()
    i1.wait_online()

    user = "admin"
    password = os.getenv("PICODATA_ADMIN_PASSWORD")

    # test authentication with a wrong password
    with pytest.raises(pg.DatabaseError, match=f"authentication failed for user '{user}'"):
        pg.Connection(user, password="wrong password", host=i1.pg_host, port=i1.pg_port)

    conn = pg.Connection(user=user, password=password, host=i1.pg_host, port=i1.pg_port)

    conn.close()


def test_user_blocking_after_a_series_of_unsuccessful_auth_attempts(
    cluster: Cluster, port_distributor: PortDistributor
):
    user = "user"
    password = "P@ssw0rd"
    host = "127.0.0.1"
    port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
    cluster:
        name: test
        tier:
            default:
    instance:
        pg:
            listen: "{host}:{port}"
    """
    )

    i1 = cluster.add_instance(wait_online=False, pg_port=int(port))
    user_banned_lc = log_crawler(i1, "Maximum number of login attempts exceeded; user blocked")
    i1.start()
    i1.wait_online()

    # create a user to connect
    i1.sql(f"CREATE USER {user} WITH PASSWORD '{password}'")

    # check if the user can connect
    conn = pg.Connection(user, password=password, host=host, port=port)
    conn.close()

    # connect many times with an invalid password to block the user
    for _ in range(4):
        with pytest.raises(pg.DatabaseError, match=f"authentication failed for user '{user}'"):
            pg.Connection(user, password="WrongPassword", host=host, port=port)

    # the user is blocked now
    with pytest.raises(pg.DatabaseError, match=f"authentication failed for user '{user}'"):
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
        with pytest.raises(pg.DatabaseError, match=f"authentication failed for user '{user}'"):
            pg.Connection(user, password="WrongPassword", host=postgres.host, port=postgres.port)

    # reset the failure counter by a successful authentication
    pg.Connection(user, password=password, host=postgres.host, port=postgres.port)

    # fail 3 more times after the reset
    for _ in range(3):
        with pytest.raises(pg.DatabaseError, match=f"authentication failed for user '{user}'"):
            pg.Connection(user, password="WrongPassword", host=postgres.host, port=postgres.port)

    # check if the user is not banned
    pg.Connection(user, password=password, host=postgres.host, port=postgres.port)


@pytest.mark.skipif(
    not is_glauth_available(),
    reason=("need installed glauth"),
)
def test_auth_ldap(cluster: Cluster, ldap_server: LdapServer, port_distributor: PortDistributor):
    # Configure the instance
    i1 = cluster.add_instance(wait_online=False)
    i1.env["TT_LDAP_URL"] = f"ldap://{ldap_server.host}:{ldap_server.port}"
    i1.env["TT_LDAP_DN_FMT"] = "cn=$USER,dc=example,dc=org"
    i1.pg_host = "127.0.0.1"
    i1.pg_port = port_distributor.get()
    i1.start()
    i1.wait_online()

    i1.sql(
        f"""
            CREATE USER "{ldap_server.user}" USING LDAP
        """
    )

    # valid creds
    conn = pg.Connection(
        ldap_server.user,
        password=ldap_server.password,
        host=i1.pg_host,
        port=i1.pg_port,
    )

    assert conn.execute_simple("SELECT 1").rows == [[1]]

    # invalid creds
    with pytest.raises(
        pg.DatabaseError, match=f"authentication failed for user '{ldap_server.user}': LDAP: Invalid credentials"
    ):
        pg.Connection(
            ldap_server.user,
            password="invalid",
            host=i1.pg_host,
            port=i1.pg_port,
        )

    # nonexistent user should give the same error as invalid password
    with pytest.raises(pg.DatabaseError, match="authentication failed for user 'missing_user'"):
        pg.Connection(
            "missing_user",
            password="invalid",
            host=i1.pg_host,
            port=i1.pg_port,
        )


def test_auth_ldap_server_down(cluster: Cluster, port_distributor: PortDistributor):
    class NotWorkingServer:
        host = "127.0.0.1"
        port = 389
        user = "user"
        password = "password"

    ldap_server = NotWorkingServer()

    # Configure the instance
    i1 = cluster.add_instance(wait_online=False)
    i1.env["TT_LDAP_URL"] = f"ldap://{ldap_server.host}:{ldap_server.port}"
    i1.env["TT_LDAP_DN_FMT"] = "cn=$USER,dc=example,dc=org"
    i1.pg_host = "127.0.0.1"
    i1.pg_port = port_distributor.get()
    i1.start()
    i1.wait_online()

    i1.sql(
        f"""
            CREATE USER "{ldap_server.user}" USING LDAP
        """
    )

    with pytest.raises(
        pg.DatabaseError, match=f"authentication failed for user '{ldap_server.user}': LDAP: Can't contact LDAP server"
    ):
        pg.Connection(
            ldap_server.user,
            password=ldap_server.password,
            host=i1.pg_host,
            port=i1.pg_port,
        )


@pytest.mark.skipif(
    not is_glauth_available(),
    reason=("need installed glauth"),
)
@pytest.mark.parametrize(
    "tls_enabled",
    [
        pytest.param("true", id="tls_enabled"),
        pytest.param("false", id="tls_disabled"),
    ],
)
def test_auth_ldap_starttls(
    cluster: Cluster, ldap_server_with_tls: LdapServer, port_distributor: PortDistributor, tls_enabled: str
):
    assert ldap_server_with_tls.tls
    # Configure the instance
    i1 = cluster.add_instance(wait_online=False)
    i1.env["TT_LDAP_URL"] = f"ldap://{ldap_server_with_tls.host}:{ldap_server_with_tls.port}"
    i1.env["TT_LDAP_DN_FMT"] = "cn=$USER,dc=example,dc=org"
    i1.env["TT_LDAP_ENABLE_TLS"] = tls_enabled
    # Don't bother to check the server's cert
    # Pass the cert check if cert is bad for some reason
    # (only for tests because our test certs are self-signed)
    # see https://www.openldap.org/doc/admin21/tls.html
    i1.env["LDAPTLS_REQCERT"] = "never"
    i1.pg_host = "127.0.0.1"
    i1.pg_port = port_distributor.get()
    i1.start()
    i1.wait_online()

    i1.sql(
        f"""
            CREATE USER "{ldap_server_with_tls.user}" USING LDAP
        """
    )
    # valid creds
    conn = pg.Connection(
        ldap_server_with_tls.user,
        password=ldap_server_with_tls.password,
        host=i1.pg_host,
        port=i1.pg_port,
    )
    assert conn.execute_simple("SELECT 1").rows == [[1]]


@pytest.mark.skipif(
    not is_glauth_available(),
    reason=("need installed glauth"),
)
def test_auth_ldap_starttls_bad_cert(
    cluster: Cluster, ldap_server_with_tls: LdapServer, port_distributor: PortDistributor
):
    """
    Scenario: Self-signed certificate in certificate chain,
    so cannot set up TLS connection
    """
    assert ldap_server_with_tls.tls
    # Configure the instance
    i1 = cluster.add_instance(wait_online=False)
    i1.env["TT_LDAP_URL"] = f"ldap://{ldap_server_with_tls.host}:{ldap_server_with_tls.port}"
    i1.env["TT_LDAP_DN_FMT"] = "cn=$USER,dc=example,dc=org"
    # Enable tls
    i1.env["TT_LDAP_ENABLE_TLS"] = "true"
    i1.pg_host = "127.0.0.1"
    i1.pg_port = port_distributor.get()
    i1.start()
    i1.wait_online()

    i1.sql(
        f"""
            CREATE USER "{ldap_server_with_tls.user}" USING LDAP
        """
    )
    with pytest.raises(
        pg.DatabaseError, match=f"authentication failed for user '{ldap_server_with_tls.user}': LDAP: Connect error"
    ):
        pg.Connection(
            ldap_server_with_tls.user,
            password=ldap_server_with_tls.password,
            host=i1.pg_host,
            port=i1.pg_port,
        )


def test_same_error_from_different_auth_methods_pgproto(cluster: Cluster):
    """
    Check that auth methods fail with the same error
    """
    (i1,) = cluster.deploy(instance_count=1)
    for idx, m in enumerate(["chap-sha1", "md5", "scram-sha256"]):
        user = f"u{idx}"

        i1.sql(f"CREATE USER {user} WITH PASSWORD 'Admin1234' USING {m}")

        with pytest.raises(pg.DatabaseError, match="authentication failed for user") as e:
            pg.Connection(
                user,
                password="not-the-correct-one",
                host=i1.pg_host,
                port=i1.pg_port,
            )

        assert e.value.args[0] == {"S": "ERROR", "C": "28P01", "M": f"authentication failed for user '{user}'"}


def check_authenticated(i: Instance, user: str, password="Admin1234"):
    conn = pg.Connection(
        user,
        password=password,
        host=i.pg_host,
        port=i.pg_port,
    )
    assert conn.execute_simple("SELECT pico_raft_leader_id()").rows == [[1]]


def test_scram_sha256_cases(cluster: Cluster):
    (i1,) = cluster.deploy(instance_count=1)

    # new user
    i1.sql("CREATE USER u1 WITH PASSWORD 'Admin1234' USING scram-sha256")
    check_authenticated(i1, "u1")

    # change password, recheck
    i1.sql("ALTER USER u1 PASSWORD '1234Admin' USING scram-sha256")
    check_authenticated(i1, "u1", "1234Admin")

    # another user, start with md5, then alter to scram
    i1.sql("CREATE USER u2 WITH PASSWORD 'Admin1234' USING md5")
    check_authenticated(i1, "u2")

    i1.sql("ALTER USER u2 PASSWORD 'Admin1234' USING scram-sha256")
    check_authenticated(i1, "u2")
