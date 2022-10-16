import signal
import os
import time
import pytest

from conftest import (
    Cluster,
    Instance,
    retrying,
    pid_alive,
    pgrep_tree,
)


@pytest.fixture
def instance(cluster: Cluster):
    cluster.deploy(instance_count=1)
    [i1] = cluster.instances
    return i1


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
