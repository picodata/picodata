import os
import pexpect  # type: ignore
import stat
import sys
from conftest import CLI_TIMEOUT, Cluster

from framework.rolling.runtime import Runtime
from framework.util import eprint


def test_admin_enoent(current_runtime: Runtime):
    cli = pexpect.spawn(
        command=current_runtime.command,
        args=["admin", "wrong/path/t.sock"],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Connection via unix socket by path 'wrong/path/t.sock' is not established")
    cli.expect_exact("No such file or directory (os error 2)")
    cli.expect_exact(pexpect.EOF)


def test_admin_econnrefused(current_runtime: Runtime):
    cli = pexpect.spawn(
        command=current_runtime.command,
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


def test_admin_invalid_path(current_runtime: Runtime):
    cli = pexpect.spawn(
        command=current_runtime.command,
        args=["admin", "./[][]"],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Connection via unix socket by path './[][]' is not established")
    cli.expect_exact("No such file or directory (os error 2)")
    cli.expect_exact(pexpect.EOF)


def test_admin_empty_path(current_runtime: Runtime):
    cli = pexpect.spawn(
        command=current_runtime.command,
        args=["admin", ""],
        env={"NO_COLOR": "1"},
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Connection via unix socket by path '' is not established")
    cli.expect_exact("Invalid argument (os error 22)")
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
        command=i1.runtime.command,
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
        command=i1.runtime.command,
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
