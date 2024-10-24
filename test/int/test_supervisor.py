import signal
import os
import time
import pytest

from conftest import (
    Cluster,
    Instance,
    Retriable,
    pid_alive,
    pgrep_tree,
)


@pytest.fixture
def instance(cluster: Cluster):
    cluster.deploy(instance_count=1)
    [i1] = cluster.instances
    return i1


def assert_all_pids_down(pids):
    for pid in pids:
        assert not pid_alive(pid)


def test_instance_kill(instance: Instance):
    # Scenario: instance.kill() should kill all child processes
    #   Given an instance
    #   When the process terminated
    #   Then all subprocesses are teminated too

    assert instance.process
    pids = pgrep_tree(instance.process.pid)
    assert len(pids) == 2

    instance.kill()

    Retriable(timeout=3, rps=5).call(lambda: assert_all_pids_down(pids))


def test_sigkill_parent(instance: Instance):
    # Scenario: killing a supervisor should be noticed by child
    #   Given an instance
    #   When parent process is killed with SIGKILL
    #   Then child process gracefully exits

    assert instance.process
    pids = pgrep_tree(instance.process.pid)

    os.kill(instance.process.pid, signal.SIGKILL)
    os.waitpid(instance.process.pid, 0)

    Retriable(timeout=3, rps=5).call(lambda: assert_all_pids_down(pids))


def test_sigint_parent(instance: Instance):
    # Scenario: suspending of child process prevents the parent process from interrupting
    #   Given an instance
    #   When child process is stopped with SIGSTOP
    #   And parent process got SIGINT
    #   Then parent process keep living
    #   When child process is continued with SIGCONT
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
    assert instance.process.wait(timeout=1) == 0

    Retriable(timeout=3, rps=5).call(lambda: assert_all_pids_down(pids))


def test_sigsegv_child(instance: Instance):
    # Scenario: sipervisor terminates when the child segfaults
    #   Given an instance
    #   When the child process segfaults
    #   Then the supervisor is teminated too

    assert instance.process
    pids = pgrep_tree(instance.process.pid)
    child_pid = pids[1]

    os.kill(child_pid, signal.SIGSEGV)

    # Upon segmentation fault tarantool calls `crash_signal_cb` handler
    # which ends with `abort()`. That's why the exit status is SIGABRT
    # and not SIGSEGV.
    assert instance.process.wait(timeout=5) == signal.SIGABRT

    Retriable(timeout=3, rps=5).call(lambda: assert_all_pids_down(pids))
