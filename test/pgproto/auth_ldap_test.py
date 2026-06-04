import os
from pathlib import Path
from typing import Literal

import pytest
import pg8000.dbapi as pg  # type: ignore
from conftest import Cluster
from framework.ldap import LdapServer, make_picodata_legacy_env_config, make_picodata_ldap_config


@pytest.mark.parametrize("config_mode", ["legacy", "yaml"])
def test_auth_ldap_plaintext(
    cluster: Cluster,
    ldap_server_plaintext: LdapServer,
    config_mode: Literal["legacy", "yaml"],
):
    # Configure the instance
    i1 = cluster.add_instance(wait_online=False)

    if config_mode == "legacy":
        i1.env |= ldap_server_plaintext.picodata_legacy_env_config()
    elif config_mode == "yaml":
        cluster.set_config_file(
            config={
                "cluster": {
                    "name": "test",
                },
                "instance": {
                    "ldap": ldap_server_plaintext.picodata_ldap_config(),
                },
            }
        )
    else:
        raise ValueError("Unknown config_mode: " + str(config_mode))

    i1.start()
    i1.wait_online()

    i1.sql(f'CREATE USER "{ldap_server_plaintext.user}" USING LDAP')

    # valid creds
    conn = pg.Connection(
        ldap_server_plaintext.user,
        password=ldap_server_plaintext.password,
        host=i1.pg_host,
        port=i1.pg_port,
    )

    assert conn.execute_simple("SELECT 1").rows == [[1]]

    # invalid creds
    with pytest.raises(
        pg.DatabaseError,
        match=f"authentication failed for user '{ldap_server_plaintext.user}': LDAP: User not found or supplied credentials are invalid",
    ):
        pg.Connection(
            ldap_server_plaintext.user,
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


def test_auth_ldap_server_down(cluster: Cluster):
    class NotWorkingServer:
        host = "127.0.0.1"
        port = 389
        user = "user"
        password = "password"

    ldap_server = NotWorkingServer()

    # Configure the instance
    i1 = cluster.add_instance(wait_online=False)

    cluster.set_config_file(
        config={
            "cluster": {
                "name": "test",
            },
            "instance": {
                "ldap": make_picodata_ldap_config(mode="plaintext", host=ldap_server.host, port=ldap_server.port),
            },
        }
    )

    i1.start()
    i1.wait_online()

    i1.sql(f'CREATE USER "{ldap_server.user}" USING LDAP')

    with pytest.raises(
        pg.DatabaseError,
        match=f"authentication failed for user '{ldap_server.user}': LDAP: authentication failed: I/O error: Connection refused",
    ):
        pg.Connection(
            ldap_server.user,
            password=ldap_server.password,
            host=i1.pg_host,
            port=i1.pg_port,
        )


@pytest.mark.parametrize("tls_client_mode", ["starttls", "plaintext"])
@pytest.mark.parametrize("config_mode", ["legacy", "yaml"])
def test_auth_ldap_starttls(
    cluster: Cluster,
    ldap_server_starttls: LdapServer,
    tls_client_mode: Literal["starttls", "plaintext"],
    config_mode: Literal["legacy", "yaml"],
):
    """
    Scenario: glauth is configured to use starttls, but without forcing the use of TLS.
    Two client configurations should work: using starttls and plaintext.
    """
    assert ldap_server_starttls.mode == "starttls"
    # Configure the instance
    i1 = cluster.add_instance(wait_online=False)

    if config_mode == "legacy":
        i1.env |= make_picodata_legacy_env_config(
            mode=tls_client_mode, host=ldap_server_starttls.host, port=ldap_server_starttls.port
        )
    elif config_mode == "yaml":
        cluster.set_config_file(
            config={
                "cluster": {
                    "name": "test",
                },
                "instance": {
                    "ldap": make_picodata_ldap_config(
                        mode=tls_client_mode, host=ldap_server_starttls.host, port=ldap_server_starttls.port
                    ),
                },
            }
        )
    else:
        raise ValueError("Unknown config_mode: " + str(config_mode))

    # Don't check the server's cert.
    # Pass the cert check if cert is bad for some reason (only for tests because our test certs are self-signed).
    # We use a self-signed cert in tests, so it won't pass the validation.
    # `test_auth_ldap_verified_tls` will specify a custom trusted CA to check.
    error_injection = "LDAP_NO_TLS_VERIFICATION"
    i1.env[f"PICODATA_ERROR_INJECTION_{error_injection}"] = "1"
    i1.start()
    i1.wait_online()

    i1.sql(f'CREATE USER "{ldap_server_starttls.user}" USING LDAP')
    # valid creds
    conn = pg.Connection(
        ldap_server_starttls.user,
        password=ldap_server_starttls.password,
        host=i1.pg_host,
        port=i1.pg_port,
    )
    assert conn.execute_simple("SELECT 1").rows == [[1]]


@pytest.mark.parametrize("config_mode", ["legacy", "yaml"])
def test_auth_ldap_implicit_tls(
    cluster: Cluster,
    ldap_server_tls: LdapServer,
    config_mode: Literal["legacy", "yaml"],
):
    assert ldap_server_tls.mode == "tls"
    # Configure the instance
    i1 = cluster.add_instance(wait_online=False)

    if config_mode == "legacy":
        i1.env |= ldap_server_tls.picodata_legacy_env_config()
    elif config_mode == "yaml":
        cluster.set_config_file(
            config={
                "cluster": {
                    "name": "test",
                },
                "instance": {
                    "ldap": ldap_server_tls.picodata_ldap_config(),
                },
            }
        )
    else:
        raise ValueError("Unknown config_mode: " + str(config_mode))

    # Don't check the server's cert.
    # Pass the cert check if cert is bad for some reason (only for tests because our test certs are self-signed).
    # We use a self-signed cert in tests, so it won't pass the validation.
    # `test_auth_ldap_verified_tls` will specify a custom trusted CA to check.
    error_injection = "LDAP_NO_TLS_VERIFICATION"
    i1.env[f"PICODATA_ERROR_INJECTION_{error_injection}"] = "1"
    i1.start()
    i1.wait_online()

    i1.sql(f'CREATE USER "{ldap_server_tls.user}" USING LDAP')
    # valid creds
    conn = pg.Connection(
        ldap_server_tls.user,
        password=ldap_server_tls.password,
        host=i1.pg_host,
        port=i1.pg_port,
    )
    assert conn.execute_simple("SELECT 1").rows == [[1]]


@pytest.mark.parametrize("ldap_server", ["starttls", "tls"], indirect=True)
def test_auth_ldap_verified_tls(
    cluster: Cluster,
    ldap_server: LdapServer,
):
    """
    Use TLS __without__ fully disabling certificate verification.
    This requires specifying a custom root CA file for picodata, so it won't work with legacy environment variable-based configuration.
    """
    ssl_dir = Path(os.path.realpath(__file__)).parent.parent / "ssl_certs"

    i1 = cluster.add_instance(wait_online=False)
    cluster.set_config_file(
        config={
            "cluster": {
                "name": "test",
            },
            "instance": {
                "ldap": ldap_server.picodata_ldap_config(ca_file=ssl_dir / "root-ca.crt"),
            },
        }
    )

    i1.start()
    i1.wait_online()

    i1.sql(f'CREATE USER "{ldap_server.user}" USING LDAP')
    # valid creds
    conn = pg.Connection(
        ldap_server.user,
        password=ldap_server.password,
        host=i1.pg_host,
        port=i1.pg_port,
    )
    assert conn.execute_simple("SELECT 1").rows == [[1]]


@pytest.mark.parametrize("ldap_server", ["starttls", "tls"], indirect=True)
@pytest.mark.parametrize("config_mode", ["legacy", "yaml"])
def test_auth_ldap_bad_cert(
    cluster: Cluster,
    ldap_server: LdapServer,
    config_mode: Literal["legacy", "yaml"],
):
    """
    Scenario: Self-signed certificate in certificate chain, so cannot set up TLS connection
    """
    # Configure the instance
    i1 = cluster.add_instance(wait_online=False)

    if config_mode == "legacy":
        i1.env |= ldap_server.picodata_legacy_env_config()
    elif config_mode == "yaml":
        cluster.set_config_file(
            config={
                "cluster": {
                    "name": "test",
                },
                "instance": {
                    "ldap": ldap_server.picodata_ldap_config(),
                },
            }
        )
    else:
        raise ValueError("Unknown config_mode: " + str(config_mode))

    i1.start()
    i1.wait_online()

    i1.sql(f'CREATE USER "{ldap_server.user}" USING LDAP')
    with pytest.raises(
        pg.DatabaseError,
        match=f"authentication failed for user '{ldap_server.user}': LDAP: authentication failed: native TLS error: error:.*:certificate verify failed",
    ):
        pg.Connection(
            ldap_server.user,
            password=ldap_server.password,
            host=i1.pg_host,
            port=i1.pg_port,
        )
