import tarantool
from conftest import Instance
import funcy

# Pre-configured retry decorators with an exponential timeout
retry = funcy.retry(
    tries=10,
    timeout=lambda attempt_number: 0.01 * 2**attempt_number,
)
retry_on_network_errors = funcy.retry(
    tries=10,
    errors=tarantool.NetworkError,
    timeout=lambda attempt_number: 0.01 * 2**attempt_number,
)


def raft_propose_eval(instance: Instance, lua_code: str, timeout_seconds=2):
    assert isinstance(instance, Instance)
    return instance.call(
        "picolib.raft_propose_eval",
        timeout_seconds,
        lua_code,
    )


def raft_status(instance):
    return instance.call("picolib.raft_status")


def assert_covers(covering: dict, covered: dict):
    merged = covered | covering
    assert merged == covering


_IGNORE = object()


def assert_raft_status(instance, raft_state, leader_id=_IGNORE):
    assert isinstance(instance, Instance)
    status = raft_status(instance)
    assert isinstance(status, dict)
    if leader_id is _IGNORE:
        status["raft_state"] == raft_state
    else:
        assert_covers(status, {"raft_state": raft_state, "leader_id": leader_id})


@retry
def promote_or_fail(instance):
    instance.call("picolib.raft_timeout_now")
    status = raft_status(instance)
    assert isinstance(status, dict)
    raft_status_id = status["id"]
    assert_raft_status(instance, raft_state="Leader", leader_id=raft_status_id)
