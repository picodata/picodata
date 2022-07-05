import signal
import subprocess
import os
import time
import pytest

from conftest import (
    Cluster,
    Instance,
    retrying,
)
from functools import reduce


@pytest.fixture
def instance(cluster: Cluster):
    cluster.deploy(instance_count=1)
    [i1] = cluster.instances
    return i1


def pgrep_tree(pid):
    command = f"exec pgrep -P{pid}"
    try:
        ps = subprocess.check_output(command, shell=True)
        ps = ps.strip().split()
        ps = list(map(lambda p: int(p), ps))
        subps = map(lambda p: pgrep_tree(p), ps)  # list of lists of pids
        subps = reduce(lambda acc, p: [*acc, *p], subps, [])  # list of pids
        return [pid, *subps]
    except subprocess.SubprocessError:
        return [pid]


def pid_alive(pid):
    """Check For the existence of a unix pid."""
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


def assert_all_pids_down(pids):
    assert all(map(lambda pid: not pid_alive(pid), pids))


def test_sigkill(instance: Instance):
    # Scenario: terminating process should terminate all child processes
    #   Given an instance
    #   When the process terminated
    #   Then all subprocesses are teminated too

    assert instance.process
    pids = pgrep_tree(instance.process.pid)
    assert len(pids) == 2

    instance.kill()

    retrying(lambda: assert_all_pids_down(pids))


def test_sigint(instance: Instance):
    # Scenario: suspending of child process prevents the parent process from interrupting
    #   Given an instance
    #   When child process is stopped
    #   And parent process got SIGINT
    #   Then parent process keep living
    #   When child process is continued
    #   Then parent process gracefully exits

    assert instance.process
    pids = pgrep_tree(instance.process.pid)
    child_pid = pids[1]

    os.kill(child_pid, signal.SIGSTOP)

    # Signal the supervisor and give it some time to handle one.
    # Without a sleep the next assertion is useless. Unfortunately,
    # there're no alternatives to sleep, because the signal
    # delivery is a mystery of the kernel.
    os.kill(instance.process.pid, signal.SIGINT)
    time.sleep(0.1)

    # We've signalled supervisor. It should forward the signal
    # the child and keep waiting. But the child is stopped now,
    # and can't handle the forwarded signal.
    # Supervisor must still be alive.
    assert pid_alive(instance.process.pid)

    os.kill(child_pid, signal.SIGCONT)
    instance.process.wait(timeout=1)

    retrying(lambda: assert_all_pids_down(pids))
