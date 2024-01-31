import errno
import os
import pytest
import signal

from conftest import (
    Cluster,
    Instance,
    Retriable,
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

    def check_pg(pgrp):
        try:
            os.killpg(pgrp, 0)
        except ProcessLookupError:
            pass
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
        Retriable(timeout=1, rps=10).call(check_pg, pgrp)
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
    Retriable(timeout=1, rps=100).call(check_pg, pgrp)
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


def test_graceful_stop(instance: Instance):
    instance.terminate()
    *_, last_xlog = sorted(
        [f for f in os.listdir(instance.data_dir) if f.endswith(".xlog")]
    )
    with open(os.path.join(instance.data_dir, last_xlog), "rb") as f:
        assert f.read()[-4:] == b"\xd5\x10\xad\xed"


def test_whoami(instance: Instance):
    assert instance.call("pico.whoami") == {
        "raft_id": 1,
        "instance_id": "i1",
        "cluster_id": instance.cluster_id,
        "tier": "default",
    }


def test_whoami_in_different_tiers(cluster: Cluster):
    cfg = {
        "tier": {
            "storage": {"replication_factor": 1},
            "router": {"replication_factor": 2},
        }
    }

    cluster.set_init_cfg(cfg)
    i1 = cluster.add_instance(tier="storage")
    i2 = cluster.add_instance(tier="router")

    assert i1.call("pico.whoami") == {
        "raft_id": 1,
        "instance_id": "i1",
        "cluster_id": i1.cluster_id,
        "tier": "storage",
    }

    assert i2.call("pico.whoami") == {
        "raft_id": 2,
        "instance_id": "i2",
        "cluster_id": i2.cluster_id,
        "tier": "router",
    }


def test_pico_instance_info(instance: Instance):
    def instance_info(iid: str | None = None):
        return instance.call("pico.instance_info", iid)

    # Don't compare entire structure, a couple of fields is enough
    myself = instance_info("i1")
    assert myself["raft_id"] == 1
    assert myself["instance_id"] == "i1"
    assert myself["replicaset_id"] == "r1"

    with pytest.raises(ReturnError) as e:
        instance_info("i2")
    assert e.value.args == ('instance with id "i2" not found',)

    assert instance_info() == myself


def test_raft_log(instance: Instance):
    raft_log = instance.call("pico.raft_log", dict(max_width=1024))

    raft_log = str.join("\n", raft_log)

    def preprocess(s: str):
        res = []
        for line in s.splitlines():
            line = line.strip()
            if line.endswith("-+"):
                # don't care about the vertical lines
                continue

            columns = line.split("|")
            columns = [c.strip() for c in columns]
            # This is what's to the left of first '|' and it's a special '\u200b' character
            columns[0] = ""

            # blank out the index column so we don't need to manually update it
            if columns[1].isdigit():
                columns[1] = "69"

            res.append(str.join("|", columns))

        return str.join("\n", res)

    def space_id(space_name: str):
        return instance.eval("return box.space[...].id", space_name)

    expected = """\
+-----+----+--------+
|index|term|contents|
+-----+----+--------+
|  0  | 1  |Insert({_pico_peer_address}, [1,"127.0.0.1:{p}"])|
|  0  | 1  |Insert({_pico_instance}, ["i1","68d4a766-4144-3248-aeb4-e212356716e4",1,"r1","e0df68c5-e7f9-395f-86b3-30ad9e1b7b07",["Offline",0],["Offline",0],{b},"default"])|
|  0  | 1  |Insert({_pico_tier}, ["default",1])|
|  0  | 1  |Insert({_pico_property}, ["global_schema_version",0])|
|  0  | 1  |Insert({_pico_property}, ["next_schema_version",1])|
|  0  | 1  |Insert({_pico_property}, ["password_min_length",8])|
|  0  | 1  |Insert({_pico_property}, ["auto_offline_timeout",5.0])|
|  0  | 1  |Insert({_pico_property}, ["max_heartbeat_period",5.0])|
|  0  | 1  |Insert({_pico_property}, ["max_pg_portals",50])|
|  0  | 1  |Insert({_pico_property}, ["snapshot_chunk_max_size",16777216])|
|  0  | 1  |Insert({_pico_property}, ["snapshot_read_view_close_timeout",86400.0])|
|  0  | 1  |Insert({_pico_user}, [0,"guest",0,["chap-sha1","vhvewKp0tNyweZQ+cFKAlsyphfg="],1])|
|  0  | 1  |Insert({_pico_user}, [1,"admin",0,["chap-sha1",""],1])|
|  0  | 1  |Insert({_pico_user}, [32,"pico_service",0,["chap-sha1","vhvewKp0tNyweZQ+cFKAlsyphfg="],1])|
|  0  | 1  |Insert({_pico_role}, [2,"public",0,1])|
|  0  | 1  |Insert({_pico_role}, [31,"super",0,1])|
|  0  | 1  |Insert({_pico_privilege}, ["login","universe",0,0,1,0])|
|  0  | 1  |Insert({_pico_privilege}, ["login","universe",0,1,1,0])|
|  0  | 1  |Insert({_pico_privilege}, ["execute","role",2,0,1,0])|
|  0  | 1  |Insert({_pico_privilege}, ["read","universe",0,32,1,0])|
|  0  | 1  |Insert({_pico_privilege}, ["write","universe",0,32,1,0])|
|  0  | 1  |Insert({_pico_privilege}, ["execute","universe",0,32,1,0])|
|  0  | 1  |Insert({_pico_privilege}, ["login","universe",0,32,1,0])|
|  0  | 1  |Insert({_pico_privilege}, ["create","universe",0,32,1,0])|
|  0  | 1  |Insert({_pico_privilege}, ["drop","universe",0,32,1,0])|
|  0  | 1  |Insert({_pico_privilege}, ["alter","universe",0,32,1,0])|
|  0  | 1  |Insert({_pico_privilege}, ["execute","role",3,32,1,0])|
|  0  | 1  |Insert({_pico_table}, [{_pico_table},"_pico_table",["global"],[["id","unsigned",false],["name","string",false],["distribution","array",false],["format","array",false],["schema_version","unsigned",false],["operable","boolean",false],["engine","string",false],["owner","unsigned",false]],0,true,"memtx",1])|
|  0  | 1  |Insert({_pico_table}, [{_pico_index},"_pico_index",["global"],[["table_id","unsigned",false],["id","unsigned",false],["name","string",false],["local","boolean",false],["parts","array",false],["schema_version","unsigned",false],["operable","boolean",false],["unique","boolean",false]],0,true,"memtx",1])|
|  0  | 1  |Insert({_pico_table}, [{_pico_peer_address},"_pico_peer_address",["global"],[["raft_id","unsigned",false],["address","string",false]],0,true,"memtx",1])|
|  0  | 1  |Insert({_pico_table}, [{_pico_instance},"_pico_instance",["global"],[["instance_id","string",false],["instance_uuid","string",false],["raft_id","unsigned",false],["replicaset_id","string",false],["replicaset_uuid","string",false],["current_grade","array",false],["target_grade","array",false],["failure_domain","map",false],["tier","string",false]],0,true,"memtx",1])|
|  0  | 1  |Insert({_pico_table}, [{_pico_property},"_pico_property",["global"],[["key","string",false],["value","any",false]],0,true,"memtx",1])|
|  0  | 1  |Insert({_pico_table}, [{_pico_replicaset},"_pico_replicaset",["global"],[["replicaset_id","string",false],["replicaset_uuid","string",false],["current_master_id","string",false],["target_master_id","string",false],["tier","string",false],["weight","number",false],["weight_origin","string",false],["state","string",false]],0,true,"memtx",1])|
|  0  | 1  |Insert({_pico_table}, [{_pico_user},"_pico_user",["global"],[["id","unsigned",false],["name","string",false],["schema_version","unsigned",false],["auth","array",false],["owner","unsigned",false]],0,true,"memtx",1])|
|  0  | 1  |Insert({_pico_table}, [{_pico_privilege},"_pico_privilege",["global"],[["privilege","string",false],["object_type","string",false],["object_id","integer",false],["grantee_id","unsigned",false],["grantor_id","unsigned",false],["schema_version","unsigned",false]],0,true,"memtx",1])|
|  0  | 1  |Insert({_pico_table}, [{_pico_role},"_pico_role",["global"],[["id","unsigned",false],["name","string",false],["schema_version","unsigned",false],["owner","unsigned",false]],0,true,"memtx",1])|
|  0  | 1  |Insert({_pico_table}, [{_pico_tier},"_pico_tier",["global"],[["name","string",false],["replication_factor","unsigned",false]],0,true,"memtx",1])|
|  0  | 1  |Insert({_pico_table}, [{_pico_routine},"_pico_routine",["global"],[["id","unsigned",false],["name","string",false],["kind","string",false],["params","array",false],["returns","array",false],["language","string",false],["body","string",false],["security","string",false],["operable","boolean",false],["schema_version","unsigned",false],["owner","unsigned",false]],0,true,"memtx",1])|
|  0  | 1  |AddNode(1)|
|  0  | 2  |-|
|  0  | 2  |Replace({_pico_instance}, ["i1","68d4a766-4144-3248-aeb4-e212356716e4",1,"r1","e0df68c5-e7f9-395f-86b3-30ad9e1b7b07",["Offline",0],["Online",1],{b},"default"])|
|  0  | 2  |Insert({_pico_replicaset}, ["r1","e0df68c5-e7f9-395f-86b3-30ad9e1b7b07","i1","i1","default",0.0,"auto","not-ready"])|
|  0  | 2  |Replace({_pico_instance}, ["i1","68d4a766-4144-3248-aeb4-e212356716e4",1,"r1","e0df68c5-e7f9-395f-86b3-30ad9e1b7b07",["Replicated",1],["Online",1],{b},"default"])|
|  0  | 2  |Update({_pico_replicaset}, ["r1"], [["=","weight",1.0], ["=","state","ready"]])|
|  0  | 2  |Replace({_pico_property}, ["target_vshard_config",[{{"e0df68c5-e7f9-395f-86b3-30ad9e1b7b07":[{{"68d4a766-4144-3248-aeb4-e212356716e4":["pico_service:@127.0.0.1:{p}","i1",true]}},1.0]}},"on"]])|
|  0  | 2  |Replace({_pico_property}, ["current_vshard_config",[{{"e0df68c5-e7f9-395f-86b3-30ad9e1b7b07":[{{"68d4a766-4144-3248-aeb4-e212356716e4":["pico_service:@127.0.0.1:{p}","i1",true]}},1.0]}},"on"]])|
|  0  | 2  |Replace({_pico_property}, ["vshard_bootstrapped",true])|
|  0  | 2  |Replace({_pico_instance}, ["i1","68d4a766-4144-3248-aeb4-e212356716e4",1,"r1","e0df68c5-e7f9-395f-86b3-30ad9e1b7b07",["Online",1],["Online",1],{b},"default"])|
+-----+----+--------+
""".format(  # noqa: E501
        p=instance.port,
        b="{}",
        _pico_peer_address=space_id("_pico_peer_address"),
        _pico_property=space_id("_pico_property"),
        _pico_replicaset=space_id("_pico_replicaset"),
        _pico_routine=space_id("_pico_routine"),
        _pico_instance=space_id("_pico_instance"),
        _pico_tier=space_id("_pico_tier"),
        _pico_privilege=space_id("_pico_privilege"),
        _pico_user=space_id("_pico_user"),
        _pico_role=space_id("_pico_role"),
        _pico_table=space_id("_pico_table"),
        _pico_index=space_id("_pico_index"),
    )
    assert preprocess(raft_log) == preprocess(expected)


def test_governor_notices_restarts(instance: Instance):
    def check_vshard_configured(instance: Instance):
        assert instance.eval(
            """
                local replicasets = vshard.router.info().replicasets
                local replica_uuid = replicasets[box.info.cluster.uuid].replica.uuid
                return replica_uuid == box.info.uuid
        """
        )

    # vshard is configured after first start
    check_vshard_configured(instance)

    assert instance.current_grade() == dict(variant="Online", incarnation=1)

    instance.restart()
    instance.wait_online()

    # vshard is configured again after restart
    check_vshard_configured(instance)

    assert instance.current_grade() == dict(variant="Online", incarnation=2)


def test_proc_version_info(instance: Instance):
    info = instance.call(".proc_version_info")
    assert info.keys() == set(["picodata_version", "proc_api_version"])  # type: ignore


def test_proc_instance_info(cluster: Cluster):
    cfg = {
        "tier": {
            "storage": {"replication_factor": 1},
            "router": {"replication_factor": 2},
        }
    }
    cluster.set_init_cfg(cfg)

    i1 = cluster.add_instance(tier="storage")
    i2 = cluster.add_instance(tier="router")

    i1_info = i1.call(".proc_instance_info")
    assert i1_info == dict(
        raft_id=1,
        advertise_address=f"{i1.host}:{i1.port}",
        instance_id="i1",
        instance_uuid=i1.instance_uuid(),
        replicaset_id="r1",
        replicaset_uuid=i1.replicaset_uuid(),
        cluster_id=i1.cluster_id,
        current_grade=dict(variant="Online", incarnation=1),
        target_grade=dict(variant="Online", incarnation=1),
        tier="storage",
    )

    info = i1.call(".proc_instance_info", "i1")
    assert i1_info == info

    i2_info = i1.call(".proc_instance_info", "i2")
    assert i2_info == dict(
        raft_id=2,
        advertise_address=f"{i2.host}:{i2.port}",
        instance_id="i2",
        instance_uuid=i2.instance_uuid(),
        replicaset_id="r2",
        replicaset_uuid=i2.replicaset_uuid(),
        cluster_id=i1.cluster_id,
        current_grade=dict(variant="Online", incarnation=1),
        target_grade=dict(variant="Online", incarnation=1),
        tier="router",
    )

    with pytest.raises(TarantoolError) as e:
        i1.call(".proc_instance_info", "i3")
    assert 'instance with id "i3" not found' in str(e)


def test_proc_raft_info(instance: Instance):
    info = instance.call(".proc_raft_info")

    assert isinstance(info["applied"], int)
    # This field is super volatile, don't want to be updating it every time we
    # add a bootstrap entry.
    info["applied"] = 69

    assert info == dict(
        id=1,
        term=2,
        applied=69,
        leader_id=1,
        state="Leader",
    )


@pytest.mark.webui
def test_proc_runtime_info(instance: Instance):
    info = instance.call(".proc_runtime_info")

    assert isinstance(info["raft"]["applied"], int)
    # This field is super volatile, don't want to be updating it every time we
    # add a bootstrap entry.
    info["raft"]["applied"] = 69

    version_info = instance.call(".proc_version_info")

    host_port = instance.env["PICODATA_HTTP_LISTEN"]
    host, port = host_port.split(":")
    port = int(port)  # type: ignore

    assert info == dict(
        raft=dict(
            id=1,
            term=2,
            applied=69,
            leader_id=1,
            state="Leader",
        ),
        internal=dict(
            main_loop_status="idle",
            governor_loop_status="idle",
        ),
        http=dict(
            host=host,
            port=port,
        ),
        version_info=version_info,
    )


def test_file_shredding(cluster: Cluster, tmp_path):
    i1 = cluster.add_instance(wait_online=False)
    i1.env["PICODATA_SHREDDING"] = "1"
    i1.start()
    i1.wait_online()

    i1.call("pico._inject_error", "KEEP_FILES_AFTER_SHREDDING", True)

    with open(os.path.join(tmp_path, "i1/00000000000000000000.xlog"), "rb") as xlog:
        xlog_before_shred = xlog.read(100)
    with open(os.path.join(tmp_path, "i1/00000000000000000000.snap"), "rb") as snap:
        snap_before_shred = snap.read(100)

    # allow only one snapshot at a time
    i1.eval("box.cfg{ checkpoint_count = 1 }")

    # make a new snapshot to give the gc a reason to remove the old one immediately
    # do it twice to remove an old xlog too
    i1.eval("box.snapshot(); box.snapshot()")

    with open(os.path.join(tmp_path, "i1/00000000000000000000.xlog"), "rb") as xlog:
        xlog_after_shred = xlog.read(100)
    with open(os.path.join(tmp_path, "i1/00000000000000000000.snap"), "rb") as snap:
        snap_after_shred = snap.read(100)

    i1.call("pico._inject_error", "KEEP_FILES_AFTER_SHREDDING", False)

    assert xlog_before_shred != xlog_after_shred
    assert snap_before_shred != snap_after_shred
