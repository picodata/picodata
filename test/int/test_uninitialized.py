import pytest

from typing import Any, Callable, Generator

from conftest import (
    TarantoolError,
    eprint,
    Cluster,
    Instance,
    Retriable,
    ReturnError,
)


@pytest.fixture
def uninitialized_instance(cluster: Cluster) -> Generator[Instance, None, None]:
    """Returns a running instance that is stuck in discovery phase."""

    # Connecting TCP/0 always results in "Connection refused"
    instance = cluster.add_instance(peers=[":0"], wait_online=False)
    instance.start()

    def check_running(instance):
        assert instance.eval("return box.info.status") == "running"
        eprint(f"{instance} is running (but stuck in discovery phase)")

    Retriable(timeout=6, rps=5).call(check_running, instance)
    yield instance


def test_raft_api(uninitialized_instance: Instance):
    functions: list[Callable[[Instance], Any]] = [
        lambda i: i.call("pico.raft_status"),
        lambda i: i.call(".proc_raft_info"),
        lambda i: i.raft_get_index(),
        lambda i: i.raft_read_index(),
        lambda i: i.raft_wait_index(0),
        lambda i: i.call("pico.raft_propose_nop"),
        lambda i: i.call("pico.whoami"),
        lambda i: i.call("pico.instance_info", "i1"),
        lambda i: i.call(".proc_instance_info", "i2"),
    ]

    for f in functions:
        with pytest.raises((ReturnError, TarantoolError)) as e:
            f(uninitialized_instance)
        assert "uninitialized yet" in str(e)
