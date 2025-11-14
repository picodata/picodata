import os
import pexpect  # type: ignore
import pytest
import sys
from pathlib import Path
from conftest import CLI_TIMEOUT, Cluster, Instance

TEST_USER = "Client"
TEST_PASSWORD = "Testpa55"
SERVICE_USER = "pico_service"
SERVICE_PASSWORD = "Testpa77"
SSL_DIR = Path(os.path.realpath(__file__)).parent.parent / "ssl_certs"


@pytest.fixture(scope="class")
def instance_with_tls(cluster: Cluster) -> Instance:
    cluster.set_service_password(SERVICE_PASSWORD)
    i = cluster.add_instance(wait_online=False)
    i.iproto_tls_enabled = True
    i.iproto_tls_cert = str(SSL_DIR / "server-with-ext.crt")
    i.iproto_tls_key = str(SSL_DIR / "server.key")
    i.iproto_tls_ca = str(SSL_DIR / "combined-ca.crt")
    i.iproto_tls = (Path(i.iproto_tls_cert), Path(i.iproto_tls_key), Path(i.iproto_tls_ca))
    i.start_and_wait()
    acl = i.sql(f"CREATE USER \"{TEST_USER}\" WITH PASSWORD '{TEST_PASSWORD}'", sudo=True)
    assert acl["row_count"] == 1
    return i


@pytest.mark.xdist_group(name="cli_with_tls")
class TestCliWithTls:
    @pytest.mark.parametrize(
        "cert_auth_enabled",
        [False, True],
        ids=["cert_auth_disabled", "cert_auth_enabled"],
    )
    def test_connect_tls(self, instance_with_tls: Instance, cert_auth_enabled: bool):
        i = instance_with_tls
        user = TEST_USER

        args = [
            "connect",
            f"{i.host}:{i.port}",
            "-u",
            user,
            "--tls-cert",
            str(SSL_DIR / "client.crt"),
            "--tls-key",
            str(SSL_DIR / "client.key"),
            "--tls-ca",
            str(SSL_DIR / "combined-ca.crt"),
        ]
        if cert_auth_enabled:
            args.append("--tls-auth")
        cli = pexpect.spawn(
            command=i.runtime.command,
            args=args,
            encoding="utf-8",
            timeout=CLI_TIMEOUT,
        )
        cli.logfile = sys.stdout

        if not cert_auth_enabled:
            cli.expect_exact(f"Enter password for {user}: ")
            cli.sendline(TEST_PASSWORD)

        cli.expect_exact("sql> ")

    @pytest.mark.parametrize(
        "cert_auth_enabled",
        [False, True],
        ids=["cert_auth_disabled", "cert_auth_enabled"],
    )
    def test_status_tls(self, instance_with_tls: Instance, cert_auth_enabled: bool):
        i = instance_with_tls
        user = SERVICE_USER

        args = [
            "status",
            "--peer",
            f"{user}@{i.host}:{i.port}",
            "--tls-cert",
            str(SSL_DIR / "server-with-ext.crt"),
            "--tls-key",
            str(SSL_DIR / "server.key"),
            "--tls-ca",
            str(SSL_DIR / "combined-ca.crt"),
        ]
        if cert_auth_enabled:
            args.append("--tls-auth")
        cli = pexpect.spawn(
            command=i.runtime.command,
            args=args,
            encoding="utf-8",
            timeout=CLI_TIMEOUT,
        )
        cli.logfile = sys.stdout

        if not cert_auth_enabled:
            cli.expect_exact(f"Enter password for {user}: ")
            cli.sendline(SERVICE_PASSWORD)

        cli.expect(f".+Online.+{i.host}:{i.port}.+")

    def test_status_tls_wrong_auth_certs(self, instance_with_tls: Instance):
        i = instance_with_tls
        user = SERVICE_USER

        args = [
            "status",
            "--peer",
            # Service user
            f"{user}@{i.host}:{i.port}",
            # Client certs
            "--tls-cert",
            str(SSL_DIR / "client.crt"),
            "--tls-key",
            str(SSL_DIR / "client.key"),
            "--tls-ca",
            str(SSL_DIR / "combined-ca.crt"),
            "--tls-auth",
        ]
        cli = pexpect.spawn(
            command=i.runtime.command,
            args=args,
            encoding="utf-8",
            timeout=CLI_TIMEOUT,
        )
        cli.logfile = sys.stdout

        cli.expect("User not found or supplied credentials are invalid")

    def test_status_tls_without_certs(self, instance_with_tls: Instance):
        i = instance_with_tls
        user = SERVICE_USER

        args = [
            "status",
            "--peer",
            f"{user}@{i.host}:{i.port}",
        ]
        cli = pexpect.spawn(
            command=i.runtime.command,
            args=args,
            encoding="utf-8",
            timeout=CLI_TIMEOUT,
        )
        cli.logfile = sys.stdout

        # We are hanging now
        # https://git.picodata.io/core/picodata/-/issues/2279
        cli.expect(pexpect.TIMEOUT, timeout=1)
