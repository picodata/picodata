import pexpect  # type: ignore
import pytest
import subprocess
import hashlib
import socket
import time
import sys
from conftest import CLI_TIMEOUT, Cluster, Instance, eprint
from dataclasses import dataclass


@pytest.fixture
def i1(cluster: Cluster) -> Instance:
    [i1] = cluster.deploy(instance_count=1)
    acl = i1.sql("create user \"testuser\" with password 'Testpa55'", sudo=True)
    assert acl["row_count"] == 1
    return i1


def test_connect_testuser(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "testuser"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("Testpa55")

    cli.expect_exact("sql> ")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact(pexpect.EOF)


def test_connect_user_host_port(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=[
            "connect",
            f"testuser@{i1.host}:{i1.port}",
            "-u",
            "overridden",
        ],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("Testpa55")

    cli.expect_exact("sql> ")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact(pexpect.EOF)


def test_connect_guest(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("sql> ")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact(pexpect.EOF)


def test_connect_user_with_role(i1: Instance):
    acl = i1.sql('create role "testrole"', sudo=True)
    assert acl["row_count"] == 1
    acl = i1.sql('grant "testrole" to "testuser"', sudo=True)
    assert acl["row_count"] == 1

    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "testuser"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("Testpa55")

    cli.expect_exact("sql> ")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact(pexpect.EOF)


def test_no_pass(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "user"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
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
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("Badpa5s")

    cli.expect_exact("User not found or supplied credentials are invalid")
    cli.expect_exact(pexpect.EOF)


def test_connection_refused(binary_path_fixt: str):
    eprint("")
    cli = pexpect.spawn(
        command=binary_path_fixt,
        args=["connect", "127.0.0.1:0", "-u", "testuser"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("")

    cli.expect_exact("failed to connect to address '127.0.0.1:0'")

    cli.expect_exact(
        [
            "Connection refused (os error 111)",
            "Can't assign requested address (os error 49)",
        ]
    )

    cli.expect_exact(pexpect.EOF)


def test_connect_auth_type_ok(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "testuser"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("Testpa55")

    cli.expect_exact("sql> ")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact(pexpect.EOF)


def test_connect_auth_type_wrong(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "testuser", "-a", "ldap"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("Testpa55")

    cli.expect_exact("User not found or supplied credentials are invalid")
    cli.expect_exact(pexpect.EOF)


def test_connect_auth_type_md5(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "testuser", "-a", "md5"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("Testpa55")

    cli.expect_exact(
        f'Connected to interactive console by address "{i1.host}:{i1.port}" under "testuser" user'
    )
    cli.expect_exact("type '\\help' for interactive help")
    cli.expect_exact("sql> ")


@dataclass
class LDAPServerState:
    host: str
    port: int
    process: subprocess.Popen


def is_glauth_available():
    try:
        subprocess.Popen(["glauth", "--version"])
    except Exception:
        return False

    return True


def configure_ldap_server(username, password, instance_dir) -> LDAPServerState:
    subprocess.Popen(["glauth", "--version"])

    LDAP_SERVER_HOST = "127.0.0.1"
    LDAP_SERVER_PORT = 1389

    ldap_cfg_path = f"{instance_dir}/ldap.cfg"
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

    process = subprocess.Popen(["glauth", "-c", ldap_cfg_path], stdout=subprocess.PIPE)

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


@pytest.mark.skipif(
    not is_glauth_available(),
    reason=("need installed glauth"),
)
def test_connect_auth_type_ldap(cluster: Cluster):
    username = "ldapuser"
    password = "ldappass"

    ldap_server = configure_ldap_server(username, password, cluster.instance_dir)
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
            timeout=CLI_TIMEOUT,
        )
        cli.logfile = sys.stdout

        cli.expect_exact(f"Enter password for {username}: ")
        cli.sendline(password)

        cli.expect_exact("sql> ")

    finally:
        ldap_server.process.kill()


def test_connect_auth_type_unknown(binary_path_fixt: str):
    cli = pexpect.spawn(
        command=binary_path_fixt,
        args=["connect", ":0", "-u", "testuser", "-a", "deadbeef"],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact('Invalid value "deadbeef"')
    cli.expect_exact(pexpect.EOF)


def test_admin_enoent(binary_path_fixt: str):
    cli = pexpect.spawn(
        command=binary_path_fixt,
        args=["admin", "wrong/path/t.sock"],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact(
        "Connection via unix socket by path 'wrong/path/t.sock' is not established"
    )
    cli.expect_exact("No such file or directory (os error 2)")
    cli.expect_exact(pexpect.EOF)


def test_admin_econnrefused(binary_path_fixt: str):
    cli = pexpect.spawn(
        command=binary_path_fixt,
        args=["admin", "/dev/null"],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact(
        "Connection via unix socket by path '/dev/null' is not established"
    )

    if sys.platform == "darwin":
        cli.expect_exact("Socket operation on non-socket (os error 38)")
    else:
        cli.expect_exact("Connection refused (os error 111)")

    cli.expect_exact(pexpect.EOF)


def test_admin_invalid_path(binary_path_fixt: str):
    cli = pexpect.spawn(
        command=binary_path_fixt,
        args=["admin", "./[][]"],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Connection via unix socket by path './[][]' is not established")
    cli.expect_exact("No such file or directory (os error 2)")
    cli.expect_exact(pexpect.EOF)


def test_admin_empty_path(binary_path_fixt: str):
    cli = pexpect.spawn(
        command=binary_path_fixt,
        args=["admin", ""],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Connection via unix socket by path '' is not established")
    cli.expect_exact("Invalid argument (os error 22)")
    cli.expect_exact(pexpect.EOF)


def test_admin_with_password(cluster: Cluster):
    import os

    os.environ["PICODATA_ADMIN_PASSWORD"] = "#AdminX12345"

    i1 = cluster.add_instance(wait_online=False)
    i1.env.update(os.environ)
    i1.start()
    i1.wait_online()

    cli = pexpect.spawn(
        command=i1.binary_path,
        args=[
            "connect",
            f"{i1.host}:{i1.port}",
            "-u",
            "admin",
        ],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout
    cli.expect_exact("Enter password for admin: ")

    password = os.getenv("PICODATA_ADMIN_PASSWORD")
    cli.sendline(password)

    cli.expect_exact("sql> ")

    eprint("^D")
    cli.sendcontrol("d")
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
        cwd=i1.instance_dir,
        command=i1.binary_path,
        args=["admin", "./admin.sock"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("(admin) sql> ")

    # Change language to SQL works
    cli.sendline("\\sql")
    cli.sendline("CREATE ROLE CHANGE_TO_SQL_WORKS;")
    cli.expect_exact("1")

    cli.sendline("\\lua")
    cli.expect_exact("Language switched to lua")

    cli.sendline("box.session.user();")
    cli.expect_exact("---\r\n")
    cli.expect_exact("- admin\r\n")
    cli.expect_exact("...\r\n")
    cli.expect_exact("\r\n")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact("Bye")
    cli.expect_exact(pexpect.EOF)


def test_connect_with_empty_password_path(binary_path_fixt: str):
    cli = pexpect.spawn(
        command=binary_path_fixt,
        args=["connect", ":3301", "--password-file", "", "-u", "trash"],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact(
        'can\'t read password from password file by "", '
        "reason: No such file or directory (os error 2)"
    )
    cli.expect_exact(pexpect.EOF)


def test_connect_with_wrong_password_path(binary_path_fixt: str):
    cli = pexpect.spawn(
        command=binary_path_fixt,
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
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact(
        'can\'t read password from password file by "/not/existing/path", '
        "reason: No such file or directory (os error 2)"
    )
    cli.expect_exact(pexpect.EOF)


def test_connect_with_password_from_file(i1: Instance, binary_path_fixt: str):
    password_path = i1.instance_dir + "/password"
    with open(password_path, "w") as f:
        f.write("Testpa55")

    cli = pexpect.spawn(
        command=binary_path_fixt,
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
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("sql> ")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact(pexpect.EOF)


def test_connect_connection_info_and_help(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "testuser"],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("Testpa55")

    cli.expect_exact(
        f'Connected to interactive console by address "{i1.host}:{i1.port}" under "testuser" user'
    )
    cli.expect_exact("type '\\help' for interactive help")
    cli.expect_exact("sql> ")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact(pexpect.EOF)


def test_admin_connection_info_and_help(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)

    socket_path = f"{i1.instance_dir}/explicit.sock"
    i1.env["PICODATA_ADMIN_SOCK"] = socket_path
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
        cwd=i1.instance_dir,
        command=i1.binary_path,
        args=["admin", socket_path],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact(f'Connected to admin console by socket path "{socket_path}"')
    cli.expect_exact("type '\\help' for interactive help")
    cli.expect_exact("(admin) sql> ")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact("Bye")
    cli.expect_exact(pexpect.EOF)


def test_connect_with_incorrect_url(cluster: Cluster):
    i1 = cluster.add_instance()

    def connect_to(address):
        cli = pexpect.spawn(
            cwd=i1.instance_dir,
            command=i1.binary_path,
            args=["connect", address],
            encoding="utf-8",
            timeout=CLI_TIMEOUT,
        )
        cli.logfile = sys.stdout
        return cli

    # GL685
    cli = connect_to("unix:/tmp/sock")
    cli.expect_exact(
        "Error while parsing instance port '/tmp/sock': invalid digit found in string"
    )
    cli.expect_exact(pexpect.EOF)

    cli = connect_to("trash:not_a_port")
    cli.expect_exact(
        "Error while parsing instance port 'not_a_port': invalid digit found in string"
    )
    cli.expect_exact(pexpect.EOF)

    cli = connect_to("random:999999999")
    cli.expect_exact(
        "Error while parsing instance port '999999999': number too large to fit in target type"
    )
    cli.expect_exact(pexpect.EOF)

    cli = connect_to("random:-1")
    cli.expect_exact(
        "Error while parsing instance port '-1': invalid digit found in string"
    )
    cli.expect_exact(pexpect.EOF)


def test_connect_timeout(cluster: Cluster):
    i1 = cluster.add_instance()

    def connect_to(address, timeout=None):
        cli = pexpect.spawn(
            cwd=i1.instance_dir,
            command=i1.binary_path,
            args=[
                "connect",
                address,
                *([f"--timeout={timeout}"] if timeout is not None else []),
            ],
            encoding="utf-8",
            timeout=CLI_TIMEOUT,
        )
        cli.logfile = sys.stdout
        return cli

    cli = connect_to("100")

    if sys.platform == "darwin":
        cli.expect_exact(
            "Connection Error. Try to reconnect: failed to connect to address '100:3301': "
            "No route to host (os error 65)"
        )
    else:
        cli.expect_exact("Connection Error. Try to reconnect: connect timeout")

    cli.expect_exact(pexpect.EOF)

    cli = connect_to("192.168.0.1")

    if sys.platform == "darwin":
        cli.expect_exact(
            "Connection Error. Try to reconnect: failed to connect to address '192.168.0.1:3301': "
            "Connection refused (os error 61)"
        )
    else:
        cli.expect_exact("Connection Error. Try to reconnect: connect timeout")

    cli.expect_exact(pexpect.EOF)

    cli = connect_to("1000010002")
    cli.expect_exact("Connection Error. Try to reconnect: connect timeout")
    cli.expect_exact(pexpect.EOF)

    cli = connect_to("1000010002", timeout=CLI_TIMEOUT)
    cli.expect_exact("Connection Error. Try to reconnect: connect timeout")
    cli.expect_exact(pexpect.EOF)

    cli = connect_to("192.168.0.1", timeout=0)
    cli.expect_exact("Connection Error. Try to reconnect: connect timeout")
    cli.expect_exact(pexpect.EOF)
