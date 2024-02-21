import pexpect  # type: ignore
import pytest
import sys
import subprocess
import hashlib
import socket
import time
from conftest import Cluster, Instance, eprint
from dataclasses import dataclass


@pytest.fixture
def i1(cluster: Cluster) -> Instance:
    [i1] = cluster.deploy(instance_count=1)
    acl = i1.sudo_sql("create user \"testuser\" with password 'Testpa55'")
    assert acl["row_count"] == 1
    return i1


def test_connect_testuser(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "testuser"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("Testpa55")

    cli.expect_exact("picodata> ")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact(pexpect.EOF)


def test_connect_user_host_port(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"testuser@{i1.host}:{i1.port}", "-u", "overridden"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("Testpa55")

    cli.expect_exact("picodata> ")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact(pexpect.EOF)


def test_connect_guest(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("picodata> ")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact(pexpect.EOF)


def test_no_pass(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "user"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for user: ")
    eprint("^D")
    cli.sendcontrol("d")

    cli.expect_exact("Failed to prompt for a password: operation interrupted")
    cli.expect_exact(pexpect.EOF)


def test_wrong_pass(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "testuser"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("Badpa5s")

    cli.expect_exact("service responded with error")
    cli.expect_exact("User not found or supplied credentials are invalid")
    cli.expect_exact(pexpect.EOF)


def test_connection_refused(binary_path: str):
    eprint("")
    cli = pexpect.spawn(
        command=binary_path,
        args=["connect", "127.0.0.1:0", "-u", "testuser"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("")

    cli.expect_exact("failed to connect to address '127.0.0.1:0'")
    cli.expect_exact("Connection refused (os error 111)")
    cli.expect_exact(pexpect.EOF)


def test_connect_auth_type_ok(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "testuser", "-a", "chap-sha1"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("Testpa55")

    cli.expect_exact("picodata> ")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact(pexpect.EOF)


def test_connect_auth_type_wrong(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "testuser", "-a", "ldap"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("Testpa55")

    cli.expect_exact("service responded with error")
    cli.expect_exact("User not found or supplied credentials are invalid")
    cli.expect_exact(pexpect.EOF)


@pytest.mark.xfail(reason="not implemeted yet")
def test_connect_auth_type_md5(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "testuser", "-a", "md5"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("Testpa55")

    cli.expect_exact("picosql :)")


@dataclass
class LDAPServerState:
    host: str
    port: int
    process: subprocess.Popen


def configure_ldap_server(username, password, data_dir) -> LDAPServerState:
    # Check `glauth` executable is available
    subprocess.Popen(["glauth", "--version"])

    LDAP_SERVER_HOST = "127.0.0.1"
    LDAP_SERVER_PORT = 1389

    ldap_cfg_path = f"{data_dir}/ldap.cfg"
    with open(ldap_cfg_path, "x") as f:
        password_sha256 = hashlib.sha256(password.encode("utf8")).hexdigest()
        f.write(
            f"""
            [ldap]
                enabled = true
                listen = "{LDAP_SERVER_HOST}:{LDAP_SERVER_PORT}"

            [ldaps]
                enabled = false

            [backend]
                datastore = "config"
                baseDN = "dc=example,dc=org"

            [[users]]
                name = "{username}"
                uidnumber = 5001
                primarygroup = 5501
                passsha256 = "{password_sha256}"
                    [[users.capabilities]]
                        action = "search"
                        object = "*"

            [[groups]]
                name = "ldapgroup"
                gidnumber = 5501
        """
        )

    process = subprocess.Popen(["glauth", "-c", ldap_cfg_path])

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    deadline = time.time() + 3

    while deadline > time.time():
        try:
            sock.connect((LDAP_SERVER_HOST, LDAP_SERVER_PORT))
            break
        except ConnectionRefusedError:
            time.sleep(0.1)

    return LDAPServerState(
        host=LDAP_SERVER_HOST, port=LDAP_SERVER_PORT, process=process
    )


@pytest.mark.xfail(
    run=False,
    reason=("need installed glauth"),
)
def test_connect_auth_type_ldap(cluster: Cluster):
    username = "ldapuser"
    password = "ldappass"

    ldap_server = configure_ldap_server(username, password, cluster.data_dir)
    try:
        #
        # Configure the instance
        #

        i1 = cluster.add_instance(wait_online=False)
        i1.env["TT_LDAP_URL"] = f"ldap://{ldap_server.host}:{ldap_server.port}"
        i1.env["TT_LDAP_DN_FMT"] = "cn=$USER,dc=example,dc=org"
        i1.start()
        i1.wait_online()

        i1.sql(
            f"""
                CREATE USER "{username}" PASSWORD '{password}' USING ldap
            """
        )

        #
        # Try connecting
        #

        cli = pexpect.spawn(
            command=i1.binary_path,
            args=["connect", f"{i1.host}:{i1.port}", "-u", username, "-a", "ldap"],
            encoding="utf-8",
            timeout=1,
        )
        cli.logfile = sys.stdout

        cli.expect_exact(f"Enter password for {username}: ")
        cli.sendline(password)

        cli.expect_exact("picosql :)")

    finally:
        ldap_server.process.kill()


def test_connect_auth_type_unknown(binary_path: str):
    cli = pexpect.spawn(
        command=binary_path,
        args=["connect", ":0", "-u", "testuser", "-a", "deadbeef"],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact('unknown AuthMethod "deadbeef"')
    cli.expect_exact(pexpect.EOF)


def test_admin_enoent(binary_path: str):
    cli = pexpect.spawn(
        command=binary_path,
        args=["admin", "wrong/path/t.sock"],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact(
        "connection via unix socket by path 'wrong/path/t.sock' is not established"
    )
    cli.expect_exact("No such file or directory")
    cli.expect_exact(pexpect.EOF)


def test_admin_econnrefused(binary_path: str):
    cli = pexpect.spawn(
        command=binary_path,
        args=["admin", "/dev/null"],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact(
        "connection via unix socket by path '/dev/null' is not established"
    )
    cli.expect_exact("Connection refused")
    cli.expect_exact(pexpect.EOF)


def test_admin_invalid_path(binary_path: str):
    cli = pexpect.spawn(
        command=binary_path,
        args=["admin", "./[][]"],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("connection via unix socket by path './[][]' is not established")
    cli.expect_exact("No such file or directory")
    cli.expect_exact(pexpect.EOF)


def test_admin_empty_path(binary_path: str):
    cli = pexpect.spawn(
        command=binary_path,
        args=["admin", ""],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("connection via unix socket by path '' is not established")
    cli.expect_exact("Invalid argument")
    cli.expect_exact(pexpect.EOF)


def test_connect_unix_ok_via_default_sock(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()

    cli = pexpect.spawn(
        # For some uninvestigated reason, readline trims the propmt in CI
        # Instead of
        #   unix/:/some/path/to/admin.sock>
        # it prints
        #   </path/to/admin.sock>
        #
        # We were unable to debug it quickly and used cwd as a workaround
        cwd=i1.data_dir,
        command=i1.binary_path,
        args=["admin", "./admin.sock"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("picodata> ")

    # Change language to SQL works
    cli.sendline("\\sql")
    cli.sendline("CREATE ROLE CHANGE_TO_SQL_WORKS")
    cli.expect_exact("1")

    cli.sendline("\\lua")
    cli.expect_exact("Language switched to Lua")

    cli.sendline("box.session.user()")
    cli.expect_exact("---\r\n")
    cli.expect_exact("- admin\r\n")
    cli.expect_exact("...\r\n")
    cli.expect_exact("\r\n")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact("Bye")
    cli.expect_exact(pexpect.EOF)


def test_connect_with_empty_password_path(binary_path: str):
    cli = pexpect.spawn(
        command=binary_path,
        args=["connect", ":3301", "--password-file", "", "-u", "trash"],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact(
        'can\'t read password from password file by "", '
        "reason: No such file or directory (os error 2)"
    )
    cli.expect_exact(pexpect.EOF)


def test_connect_with_wrong_password_path(binary_path: str):
    cli = pexpect.spawn(
        command=binary_path,
        args=[
            "connect",
            ":3301",
            "--password-file",
            "/not/existing/path",
            "-u",
            "trash",
        ],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact(
        'can\'t read password from password file by "/not/existing/path", '
        "reason: No such file or directory (os error 2)"
    )
    cli.expect_exact(pexpect.EOF)


def test_connect_with_password_from_file(i1: Instance, binary_path: str):
    password_path = i1.data_dir + "/password"
    with open(password_path, "w") as f:
        f.write("Testpa55")

    cli = pexpect.spawn(
        command=binary_path,
        args=[
            "connect",
            f"{i1.host}:{i1.port}",
            "--password-file",
            password_path,
            "-u",
            "testuser",
        ],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("picodata> ")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact(pexpect.EOF)


def test_connect_connection_info_and_help(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "testuser", "-a", "chap-sha1"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("Testpa55")

    cli.expect_exact(
        f'Connected to interactive console by address "{i1.host}:{i1.port}" under "testuser" user'
    )
    cli.expect_exact("type '\\help' for interactive help")
    cli.expect_exact("picodata> ")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact(pexpect.EOF)


def test_admin_connection_info_and_help(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()

    cli = pexpect.spawn(
        # For some uninvestigated reason, readline trims the propmt in CI
        # Instead of
        #   unix/:/some/path/to/admin.sock>
        # it prints
        #   </path/to/admin.sock>
        #
        # We were unable to debug it quickly and used cwd as a workaround
        cwd=i1.data_dir,
        command=i1.binary_path,
        args=["admin", "./admin.sock"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact('Connected to admin console by socket path "./admin.sock"')
    cli.expect_exact("type '\\help' for interactive help")
    cli.expect_exact("picodata> ")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact("Bye")
    cli.expect_exact(pexpect.EOF)
