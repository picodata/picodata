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

    cli.expect_exact(f"connected to {i1.host}:{i1.port}")
    cli.expect_exact(f"{i1.host}:{i1.port}>")

    cli.sendline("box.session.user()")
    cli.expect_exact("---\r\n")
    cli.expect_exact("- testuser\r\n")
    cli.expect_exact("...\r\n")
    cli.expect_exact("\r\n")

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

    cli.expect_exact(f"connected to {i1.host}:{i1.port}")
    cli.expect_exact(f"{i1.host}:{i1.port}>")

    cli.sendline("box.session.user()")
    cli.expect_exact("---\r\n")
    cli.expect_exact("- testuser\r\n")
    cli.expect_exact("...\r\n")
    cli.expect_exact("\r\n")

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

    cli.expect_exact("Enter password for guest: ")
    cli.sendline("")

    cli.expect_exact(f"connected to {i1.host}:{i1.port}")
    cli.expect_exact(f"{i1.host}:{i1.port}>")

    cli.sendline("box.session.user()")
    cli.expect_exact("---\r\n")
    cli.expect_exact("- guest\r\n")
    cli.expect_exact("...\r\n")
    cli.expect_exact("\r\n")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact(pexpect.EOF)


def test_no_pass(i1: Instance):
    cli = pexpect.spawn(
        command=i1.binary_path,
        args=["connect", f"{i1.host}:{i1.port}"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for guest: ")
    eprint("^D")
    cli.sendcontrol("d")

    cli.expect_exact("No password provided")
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

    cli.expect_exact("Connection is not established")
    cli.expect_exact(pexpect.EOF)


def test_connection_refused(binary_path: str):
    eprint("")
    cli = pexpect.spawn(
        command=binary_path,
        args=["connect", ":0", "-u", "testuser"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for testuser: ")
    cli.sendline("")

    cli.expect_exact("Connection is not established")
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

    cli.expect_exact(f"connected to {i1.host}:{i1.port}")
    cli.expect_exact(f"{i1.host}:{i1.port}>")

    cli.sendline("box.session.user()")
    cli.expect_exact("---\r\n")
    cli.expect_exact("- testuser\r\n")
    cli.expect_exact("...\r\n")
    cli.expect_exact("\r\n")

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

    cli.expect_exact("Connection is not established")
    cli.expect_exact(pexpect.EOF)


def test_connect_auth_type_unknown(binary_path: str):
    cli = pexpect.spawn(
        command=binary_path,
        args=["connect", ":0", "-u", "testuser", "-a", "deadbeef"],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact(
        "error: \"deadbeef\" isn't a valid value for '--auth-type <METHOD>"
    )
    cli.expect_exact(pexpect.EOF)


def test_connect_unix_enoent(binary_path: str):
    cli = pexpect.spawn(
        command=binary_path,
        args=["connect", "--unix", "wrong/path/t.sock"],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Connection is not established: No such file or directory")
    cli.expect_exact("uri: unix/:./wrong/path/t.sock")
    cli.expect_exact(pexpect.EOF)


def test_connect_unix_econnrefused(binary_path: str):
    cli = pexpect.spawn(
        command=binary_path,
        args=["connect", "--unix", "/dev/null"],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Connection is not established: Connection refused")
    cli.expect_exact("uri: unix/:/dev/null")
    cli.expect_exact(pexpect.EOF)


def test_connect_unix_invalid_path(binary_path: str):
    cli = pexpect.spawn(
        command=binary_path,
        args=["connect", "--unix", "./[][]"],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("invalid socket path: ./[][]")
    cli.expect_exact(pexpect.EOF)


def test_connect_unix_empty_path(binary_path: str):
    cli = pexpect.spawn(
        command=binary_path,
        args=["connect", "--unix", ""],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("invalid socket path:")
    cli.expect_exact(pexpect.EOF)


def test_connect_unix_ok(cluster: Cluster, tmpdir):
    i1 = cluster.add_instance(wait_online=False)
    i1.env.update({"PICODATA_CONSOLE_SOCK": f"{i1.data_dir}/console.sock"})
    i1.start()
    i1.wait_online()

    cli = pexpect.spawn(
        # For some uninvestigated reason, readline trims the propmt in CI
        # Instead of
        #   unix/:/some/path/to/console.sock>
        # it prints
        #   </path/to/console.sock>
        #
        # We were unable to debug it quickly and used cwd as a workaround
        cwd=i1.data_dir,
        command=i1.binary_path,
        args=["connect", "--unix", "./console.sock"],
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("connected to unix/:./console.sock")
    cli.expect_exact("unix/:./console.sock>")

    cli.sendline("box.session.user()")
    cli.expect_exact("---\r\n")
    cli.expect_exact("- admin\r\n")
    cli.expect_exact("...\r\n")
    cli.expect_exact("\r\n")

    eprint("^D")
    cli.sendcontrol("d")
    cli.expect_exact(pexpect.EOF)
