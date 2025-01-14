import pytest
from conftest import Cluster, TarantoolError


def test_set_via_alter_system(cluster: Cluster):
    instance = cluster.add_instance()

    # default values
    box_config = instance.eval("return box.cfg")
    assert box_config["net_msg_max"] == 768
    assert box_config["checkpoint_interval"] == 3600
    assert box_config["checkpoint_count"] == 2

    # picodata parameters names are slightly different from the tarantools
    instance.sql("ALTER SYSTEM SET iproto_net_msg_max TO 100")
    instance.sql("ALTER SYSTEM SET memtx_checkpoint_interval TO 100")
    instance.sql("ALTER SYSTEM SET memtx_checkpoint_count TO 100")

    # parameters values changed
    box_config = instance.eval("return box.cfg")
    assert box_config["net_msg_max"] == 100
    assert box_config["checkpoint_interval"] == 100
    assert box_config["checkpoint_count"] == 100

    # box settings isn't persistent, so it should be reapplied
    instance.restart()
    instance.wait_online()

    # parameters values are correct even after restart
    box_config = instance.eval("return box.cfg")
    assert box_config["net_msg_max"] == 100
    assert box_config["checkpoint_interval"] == 100
    assert box_config["checkpoint_count"] == 100

    # bad values for parameters shouldn't pass validation
    # stage before creating DML from ir node
    with pytest.raises(
        TarantoolError,
        match="""invalid value for 'iproto_net_msg_max' expected unsigned, got integer""",
    ):
        instance.sql("ALTER SYSTEM SET iproto_net_msg_max = -1")

    with pytest.raises(
        TarantoolError,
        match="""invalid value for 'memtx_checkpoint_interval' expected unsigned, got decimal""",
    ):
        instance.sql("ALTER SYSTEM SET memtx_checkpoint_interval = -1.0")

    with pytest.raises(
        TarantoolError,
        match="""invalid value for 'memtx_checkpoint_count' expected unsigned, got integer""",
    ):
        instance.sql("ALTER SYSTEM SET memtx_checkpoint_count = -1")


def test_snapshot_and_dynamic_parameters(cluster: Cluster):
    i1, i2, _ = cluster.deploy(instance_count=3)

    i2.kill()

    i1.sql("ALTER SYSTEM SET iproto_net_msg_max = 100")

    # Trigger raft log compaction
    i1.sql("ALTER SYSTEM SET raft_wal_count_max TO 1")

    # Add a new instance and restart `i2`, which catches up by raft snapshot
    i2.start_and_wait()
    i4 = cluster.add_instance()

    for catched_up_by_snapshot in [i2, i4]:
        box_config = catched_up_by_snapshot.eval("return box.cfg")
        assert box_config["net_msg_max"] == 100
