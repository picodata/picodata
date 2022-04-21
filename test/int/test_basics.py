import os
import pytest
import tarantool

from conftest import Cluster
from util import raft_propose_eval, retry_on_network_errors


def process_exists(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    else:
        return True


def test_instance(instance):
    assert str(instance)

    assert instance.call("tostring", 1) == "1"
    assert instance.call("dostring", "return") is None
    assert instance.call("dostring", "return 1") == 1
    assert instance.call("dostring", "return 1, nil, 2, '3'") == (1, None, 2, "3")

    assert instance.eval("return") is None
    assert instance.eval("return 1") == 1
    assert instance.eval("return 1, nil, 2, '3'") == (1, None, 2, "3")

    pid = instance.process.pid
    instance.stop()
    assert not process_exists(pid)
    with pytest.raises(tarantool.NetworkError):
        instance.call("whatever")
    instance.start()
    assert instance.eval("return 1") == 1

    pid = instance.process.pid
    instance.kill()
    assert not process_exists(pid)
    with pytest.raises(tarantool.NetworkError):
        instance.eval("return")
    instance.remove_data()
    assert os.listdir(instance.workdir) == []
    instance.start()
    retry_on_network_errors(instance.eval)("return")

    pid = instance.process.pid
    instance.restart()
    assert not process_exists(pid)
    retry_on_network_errors(instance.eval)("return")


def test_single_instance_raft_eval(instance):
    raft_propose_eval(instance, "_G.success = true")
    assert instance.eval("return _G.success")


def test_cluster(cluster: Cluster):
    assert cluster.instances
    assert str(cluster)

    for instance in cluster.instances:
        retry_on_network_errors(raft_propose_eval)(instance, "return true")

    cluster.restart()

    for instance in cluster.instances:
        retry_on_network_errors(raft_propose_eval)(instance, "return true")

    cluster.kill()
    cluster.remove_data()


@pytest.mark.xfail
def test_propose_eval(run_cluster):
    cluster: Cluster = run_cluster(instance_count=3)

    i1, i2, i3 = cluster.instances

    assert raft_propose_eval(i1, "_G.x1 = 1") == (None, None)
    assert raft_propose_eval(i2, "_G.x2 = 2") == (None, None)
    assert raft_propose_eval(i3, "_G.x3 = 3") == (None, None)

    assert i1.eval("return rawget(_G, 'x1')") == 1
    assert i1.eval("return rawget(_G, 'x2')") == 2
    assert i1.eval("return rawget(_G, 'x3')") == 3

    assert i2.eval("return rawget(_G, 'x1')") == 1
    assert i2.eval("return rawget(_G, 'x2')") == 2
    assert i2.eval("return rawget(_G, 'x3')") == 3

    assert i3.eval("return rawget(_G, 'x1')") == 1
    assert i3.eval("return rawget(_G, 'x2')") == 2
    assert i3.eval("return rawget(_G, 'x3')") == 3
