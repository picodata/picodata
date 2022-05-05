import errno
import os
import funcy  # type: ignore
import pytest
import signal

from conftest import (
    xdist_worker_number,
    Instance,
    RaftStatus,
    TarantoolError,
    ReturnError,
    MalformedAPI,
)


def test_xdist_worker_number():
    assert xdist_worker_number("master") == 0
    assert xdist_worker_number("gw0") == 0
    assert xdist_worker_number("gw1") == 1
    assert xdist_worker_number("gw007") == 7
    assert xdist_worker_number("gw1024") == 1024

    with pytest.raises(ValueError, match=r"gw"):
        assert xdist_worker_number("gw")

    with pytest.raises(ValueError, match=r"xgw8x"):
        assert xdist_worker_number("xgw8x")

    with pytest.raises(ValueError, match=r"wtf"):
        assert xdist_worker_number("wtf")


def test_raft_status():
    s = RaftStatus(
        id=1,
        raft_state="SomeState",
        leader_id=1,
    )

    assert (s == "SomeState") is True
    assert (s == ("SomeState")) is True
    assert (s == ("SomeState",)) is False
    assert (s == "OtherState") is False

    assert (s == ("SomeState", 1)) is True
    assert (s == ("SomeState", -1)) is False
    assert (s == ("OtherState", 1)) is False

    assert (s == s) is True
    assert (s == RaftStatus(s.id, s.raft_state, s.leader_id)) is True
    assert (s == RaftStatus(-1, s.raft_state, s.leader_id)) is False
    assert (s == RaftStatus(s.id, "OtherState", s.leader_id)) is False
    assert (s == RaftStatus(s.id, s.raft_state, -1)) is False

    assert RaftStatus(1, "Follower", 0).is_ready() is False
    assert RaftStatus(1, "Follower", 1).is_ready() is True


def test_call_normalization(instance: Instance):
    assert instance.call("tostring", 1) == "1"
    assert instance.call("dostring", "return") is None
    assert instance.call("dostring", "return 1") == 1
    assert instance.call("dostring", "return { }") == []
    assert instance.call("dostring", "return 's'") == "s"
    assert instance.call("dostring", "return nil") is None
    assert instance.call("dostring", "return true") is True

    with pytest.raises(ReturnError) as e1:
        instance.call("dostring", "return nil, 'some error'")
    assert e1.value.args == ("some error",)

    with pytest.raises(MalformedAPI) as e2:
        instance.call("dostring", "return 'x', 1")
    assert e2.value.args == ("x", 1)

    with pytest.raises(TarantoolError) as e3:
        instance.call("error", "lua exception", 0)
    assert e3.value.args == ("ER_PROC_LUA", "lua exception")

    with pytest.raises(TarantoolError) as e4:
        instance.call("void")
    assert e4.value.args == ("ER_NO_SUCH_PROC", "Procedure 'void' is not defined")

    # Python connector for tarantool misinterprets timeout errors.
    # It should be TimeoutError instead of ECONNRESET
    with pytest.raises(OSError) as e5:
        instance.call("package.loaded.fiber.sleep", 1, timeout=0.1)
    assert e5.value.errno == errno.ECONNRESET

    with pytest.raises(OSError) as e6:
        instance.call("os.exit", 0)
    assert e6.value.errno == errno.ECONNRESET

    instance.terminate()
    with pytest.raises(OSError) as e7:
        instance.call("anything")
    assert e7.value.errno == errno.ECONNREFUSED


def test_eval_normalization(instance: Instance):
    assert instance.eval("return") is None
    assert instance.eval("return 1") == 1
    assert instance.eval("return { }") == []
    assert instance.eval("return 's'") == "s"
    assert instance.eval("return nil") is None
    assert instance.eval("return true") is True

    with pytest.raises(ReturnError) as e1:
        instance.eval("return nil, 'some error'")
    assert e1.value.args == ("some error",)

    with pytest.raises(MalformedAPI) as e2:
        instance.eval("return 'x', 2")
    assert e2.value.args == ("x", 2)

    with pytest.raises(TarantoolError) as e3:
        instance.eval("error('lua exception', 0)")
    assert e3.value.args == ("ER_PROC_LUA", "lua exception")

    with pytest.raises(TarantoolError) as e4:
        instance.eval("return box.schema.space.drop(0, 'void')")
    assert e4.value.args == ("ER_NO_SUCH_SPACE", "Space 'void' does not exist")


def test_process_management(instance: Instance):
    """
    The test ensures pytest can kill all subprocesses
    even if they don't terminate and hang
    """

    assert instance.eval("return 'ok'") == "ok"
    assert instance.process is not None
    pid = instance.process.pid
    pgrp = pid

    class StillAlive(Exception):
        pass

    @funcy.retry(tries=10, timeout=0.01, errors=StillAlive)
    def waitpg(pgrp):
        try:
            os.killpg(pgrp, 0)
        except ProcessLookupError:
            return True
        else:
            raise StillAlive

    # Sigstop entire pg so that the picodata child can't
    # handle the supervisor termination
    os.killpg(pgrp, signal.SIGSTOP)

    # Now kill the supervisor
    os.kill(pid, signal.SIGKILL)
    os.waitpid(pid, 0)

    # Make sure the supervisor is dead
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)

    # Make sure the child is still hanging
    with pytest.raises(OSError) as exc:
        instance.eval("return 'ok'", timeout=0.1)
    assert exc.value.errno == errno.ECONNRESET
    with pytest.raises(StillAlive):
        waitpg(pgrp)

    # Kill the remaining child in the process group
    instance.killpg()

    # When the supervisor is killed, the orphaned child is reparented to
    # a subreaper. Pytest isn't the one, and therefore it can't do `waitpid`.
    # Instead, the test retries `killpg` until it succeeds.

    # Ensure the child is dead
    waitpg(pgrp)
    with pytest.raises(ProcessLookupError):
        os.killpg(pgrp, 0)

    # Check idempotency
    instance.start()
    pid1 = instance.process.pid
    instance.start()
    pid2 = instance.process.pid
    assert pid1 == pid2

    instance.terminate()
    instance.terminate()
    instance.killpg()
    instance.killpg()


def test_propose_eval(instance: Instance):
    with pytest.raises(ReturnError, match="timeout"):
        instance.raft_propose_eval("return", timeout_seconds=0)

    assert instance.raft_propose_eval("_G.success = true")
    assert instance.eval("return _G.success") is True
