import errno
import os
import funcy  # type: ignore
import pytest
import signal

from conftest import (
    Instance,
    TarantoolError,
    ReturnError,
    MalformedAPI,
)


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
        except PermissionError:
            # According to `man 2 kill`, MacOS raises it if at least one process
            # in the process group has insufficient permissions. In fact, it also
            # returns EPERM if the targed process is a zombie.
            # See https://git.picodata.io/picodata/picodata/picodata/-/snippets/7
            raise StillAlive
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
    print(f"{instance} is still alive")

    # Kill the remaining child in the process group using conftest API
    instance.kill()

    # When the supervisor is killed, the orphaned child is reparented
    # to a subreaper. Pytest isn't the one, and therefore it can't do
    # `waitpid` directly. Instead, the test retries `killpg` until
    # it succeeds.
    #
    # Also, note, that after the child is killed, it remains
    # a zombie for a while. The child is removed from the process
    # table when a subreaper calls `waitpid`.
    #
    waitpg(pgrp)
    print(f"{instance} is finally dead")

    # Ensure the child is dead
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
    instance.kill()
    instance.kill()


def test_propose_eval(instance: Instance):
    with pytest.raises(ReturnError, match="timeout"):
        instance.raft_propose_eval("return", timeout_seconds=0)

    instance.raft_propose_eval("_G.success = true")
    assert instance.eval("return _G.success") is True


def test_graceful_stop(instance: Instance):
    instance.terminate()
    *_, last_xlog = sorted(
        [f for f in os.listdir(instance.data_dir) if f.endswith(".xlog")]
    )
    with open(os.path.join(instance.data_dir, last_xlog), "rb") as f:
        assert f.read()[-4:] == b"\xd5\x10\xad\xed"


def test_whoami(instance: Instance):
    assert instance.call("picolib.whoami") == {
        "raft_id": 1,
        "instance_id": "i1",
        "cluster_id": instance.cluster_id,
    }


def test_peer_info(instance: Instance):
    def peer_info(iid: str):
        return instance.call("picolib.peer_info", iid)

    # Don't compare entire structure, a couple of fields is enough
    myself = peer_info("i1")
    assert myself["raft_id"] == 1
    assert myself["instance_id"] == "i1"
    assert myself["replicaset_id"] == "r1"

    with pytest.raises(ReturnError) as e:
        peer_info("i2")
    assert e.value.args == ('peer with id "i2" not found',)
