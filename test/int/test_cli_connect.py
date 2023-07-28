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


def test_address_wrong_format(binary_path: str):
    eprint("")
    cli = pexpect.spawn(
        command=binary_path,
        args=["connect", "testuser:testpass@localhost:3301"],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=1,
    )
    cli.logfile = sys.stdout

    cli.expect_exact(
        'error: Invalid value "testuser:testpass@localhost:3301" '
        + "for '<ADDRESS>': valid format: [user@][host][:port]\r\n\r\n"
        + "For more information try --help\r\n"
    )
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
