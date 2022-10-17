import funcy  # type: ignore
import pytest

from typing import Any, Callable, Generator

from conftest import (
    eprint,
    Cluster,
    Instance,
    ReturnError,
)


@pytest.fixture
def uninitialized_instance(cluster: Cluster) -> Generator[Instance, None, None]:
    """Returns a running instance that is stuck in discovery phase."""

    # Connecting TCP/0 always results in "Connection refused"
    instance = cluster.add_instance(peers=[":0"], wait_online=False)
    instance.start()

    @funcy.retry(tries=30, timeout=0.2)
    def wait_running():
        assert instance.eval("return box.info.status") == "running"
        eprint(f"{instance} is running (but stuck in discovery phase)")

    wait_running()
    yield instance


def test_raft_api(uninitialized_instance: Instance):
    functions: list[Callable[[Instance], Any]] = [
        lambda i: i._raft_status(),
        lambda i: i.call("picolib.raft_propose_nop"),
        lambda i: i.call("picolib.raft_propose_info", "who cares"),
        lambda i: i.call("picolib.whoami"),
        lambda i: i.call("picolib.peer_info", "i1"),
        lambda i: i.call("picolib.peer_info", "i2"),
    ]

    for f in functions:
        with pytest.raises(ReturnError) as e:
            f(uninitialized_instance)
        assert e.value.args == ("uninitialized yet",)
