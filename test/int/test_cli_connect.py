import os
import pexpect  # type: ignore
import pytest
import stat
import sys
from conftest import CLI_TIMEOUT, Cluster, Instance, eprint

from framework.ldap import is_glauth_available, LdapServer


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

    cli.expect_exact(f'Connected to interactive console by address "{i1.host}:{i1.port}" under "testuser" user')
    cli.expect_exact("type '\\help' for interactive help")
    cli.expect_exact("sql> ")


@pytest.mark.skipif(
    not is_glauth_available(),
    reason=("need installed glauth"),
)
def test_connect_auth_type_ldap(cluster: Cluster, ldap_server: LdapServer):
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
            CREATE USER "{ldap_server.user}" USING LDAP
        """
    )

    #
    # Try connecting
    #

    cli = pexpect.spawn(
        command=i1.binary_path,
        args=[
            "connect",
            f"{i1.host}:{i1.port}",
            "-u",
            ldap_server.user,
            "-a",
            "ldap",
        ],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact(f"Enter password for {ldap_server.user}: ")
    cli.sendline(ldap_server.password)

    cli.expect_exact("sql> ")


def test_connect_auth_type_unknown(binary_path_fixt: str):
    cli = pexpect.spawn(
        command=binary_path_fixt,
        args=["connect", "127.0.0.1:0", "-u", "testuser", "-a", "deadbeef"],
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

    cli.expect_exact("Connection via unix socket by path 'wrong/path/t.sock' is not established")
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

    cli.expect_exact("Connection via unix socket by path '/dev/null' is not established")

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

    st = os.stat(f"{i1.instance_dir}/admin.sock")
    mode = stat.S_IMODE(st.st_mode)
    assert oct(mode) == "0o660"

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

    cli.sendline("box.session.user()")
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
        args=["connect", "127.0.0.1:3301", "--password-file", "", "-u", "trash"],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("""ERROR: bad password file at '""': No such file or directory (os error 2)""")
    cli.expect_exact(pexpect.EOF)


def test_connect_with_wrong_password_path(binary_path_fixt: str):
    cli = pexpect.spawn(
        command=binary_path_fixt,
        args=[
            "connect",
            "127.0.0.1:3301",
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

    cli.expect_exact("""ERROR: bad password file at '"/not/existing/path"': No such file or directory (os error 2)""")
    cli.expect_exact(pexpect.EOF)


def test_connect_with_password_from_file(i1: Instance, binary_path_fixt: str):
    password_path = i1.instance_dir / "password"
    with open(password_path, "w") as f:
        f.write("Testpa55")

    cli = pexpect.spawn(
        command=binary_path_fixt,
        args=[
            "connect",
            f"{i1.host}:{i1.port}",
            "--password-file",
            password_path.as_posix(),
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

    cli.expect_exact(f'Connected to interactive console by address "{i1.host}:{i1.port}" under "testuser" user')
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
    def connect_to(address):
        cli = pexpect.spawn(
            command=cluster.binary_path,
            args=["connect", address],
            encoding="utf-8",
            timeout=CLI_TIMEOUT,
        )
        cli.logfile = sys.stdout
        return cli

    # GL685
    cli = connect_to("unix:/tmp/sock")
    cli.expect_exact("ERROR: parsing port '/tmp/sock' failed: invalid digit found in string")
    cli.expect_exact(pexpect.EOF)

    cli = connect_to("trash:not_a_port")
    cli.expect_exact("ERROR: parsing port 'not_a_port' failed: invalid digit found in string")
    cli.expect_exact(pexpect.EOF)

    cli = connect_to("random:999999999")
    cli.expect_exact("ERROR: parsing port '999999999' failed: number too large to fit in target type")
    cli.expect_exact(pexpect.EOF)

    cli = connect_to("random:-1")
    cli.expect_exact("ERROR: parsing port '-1' failed: invalid digit found in string")
    cli.expect_exact(pexpect.EOF)


def test_connect_timeout(cluster: Cluster):
    def connect_to(address, timeout=None):
        cli = pexpect.spawn(
            command=cluster.binary_path,
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

    # The main purpose of tests below is to show that connection
    # to non-existent address:
    # * won't crash and result in panic and
    # * will end up with timeout or error.
    #
    # Expected error message pattern is selected to be general
    # specifically to cover all possible error. E.g.:
    # * timeout error
    # * connection refused (os error 111)
    # * no route to host (os error 113)
    # * many others that depend on your/CI network settings and
    #   which we don't want to list here

    TIMEOUT = 1

    cli = connect_to("100:1234", timeout=TIMEOUT)
    cli.expect_exact("ERROR: connection failure for address '100:1234': connect timeout")
    cli.expect_exact(pexpect.EOF)

    cli = connect_to("192.168.0.1:1234", timeout=TIMEOUT)
    cli.expect_exact("ERROR: connection failure for address '192.168.0.1:1234': connect timeout")
    cli.expect_exact(pexpect.EOF)

    cli = connect_to("1000010002:1234", timeout=TIMEOUT)
    cli.expect_exact("ERROR: connection failure for address '1000010002:1234': connect timeout")
    cli.expect_exact(pexpect.EOF)

    cli = connect_to("1000010002:1234", timeout=TIMEOUT)
    cli.expect_exact("ERROR: connection failure for address '1000010002:1234': connect timeout")
    cli.expect_exact(pexpect.EOF)

    cli = connect_to("192.168.0.1:1234", timeout=0)
    cli.expect_exact("ERROR: connection failure for address '192.168.0.1:1234': connect timeout")
    cli.expect_exact(pexpect.EOF)
