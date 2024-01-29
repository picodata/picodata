import pexpect  # type: ignore
import pytest
import sys
from conftest import Cluster, Instance, eprint


@pytest.fixture
def i1(cluster: Cluster) -> Instance:
    [i1] = cluster.deploy(instance_count=1)
    i1.eval(
        """
        box.session.su("admin")
        box.schema.user.create('testuser', { password = 'testpass' })
        box.schema.user.grant('testuser', 'read,execute', 'universe')
        """
    )
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
    cli.sendline("testpass")

    cli.expect_exact("picosql :)")

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
    cli.sendline("testpass")

    cli.expect_exact("picosql :)")

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

    cli.expect_exact("picosql :)")

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
    cli.sendline("badpass")

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
    cli.sendline("testpass")

    cli.expect_exact("picosql :)")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact(pexpect.EOF)


def test_connect_auth_type_different(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}", "-u", "testuser", "-a", "chap-sha1"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("")

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
    cli.sendline("testpass")

    cli.expect_exact("picosql :)")


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

    cli.expect_exact("picoadmin :) ")

    # Change language to SQL works
    cli.sendline("\\sql")
    cli.sendline("CREATE ROLE CHANGE_TO_SQL_WORKS")
    cli.expect_exact("---\r\n")
    cli.expect_exact("- row_count: 1\r\n")
    cli.expect_exact("...\r\n")
    cli.expect_exact("\r\n")

    cli.sendline("\\lua")
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
        f.write("testpass")

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

    cli.expect_exact("picosql :)")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact(pexpect.EOF)


def test_lua_completion(cluster: Cluster):
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

    cli.expect_exact("picoadmin :) ")

    # With several possible variants they are shown as list
    cli.sendline("to\t\t")
    cli.expect_exact("tostring(    tonumber(    tonumber64(")

    cli.sendline("box.c\t\t")
    cli.expect_exact("box.ctl      box.cfg      box.commit(")

    cli.sendline("tonumber(to\t\t")
    cli.expect_exact("tostring(    tonumber(    tonumber64(")

    # With one possible variant it automaticaly completes current word
    # so we can check that is completed by result of completing this command
    cli.sendline("hel\t\t")
    cli.expect_exact(
        "To get help, see the Tarantool manual at https://tarantool.io/en/doc/"
    )

    cli.sendline("bred bo\t\t")
    cli.expect_exact("bred box")
    cli.sendline(" ")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact("Bye")
    cli.expect_exact(pexpect.EOF)
