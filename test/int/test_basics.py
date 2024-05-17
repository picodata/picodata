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
    log_crawler,
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


def test_config_storage_conflicts_on_restart(instance: Instance):
    instance.terminate()

    #
    # Change cluster_id
    #
    was = instance.cluster_id  # type: ignore
    instance.cluster_id = "new-cluster-id"
    assert instance.cluster_id != was
    err = f"""\
invalid configuration: instance restarted with a different `cluster_id`, which is not allowed, was: '{was}' became: 'new-cluster-id'
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched
    instance.cluster_id = was

    #
    # Change instance_id
    #
    was = instance.instance_id  # type: ignore
    instance.instance_id = "new-instance-id"
    assert instance.instance_id != was
    err = f"""\
invalid configuration: instance restarted with a different `instance_id`, which is not allowed, was: '{was}' became: 'new-instance-id'
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched
    instance.instance_id = was

    #
    # Change tier
    #
    was = instance.tier  # type: ignore
    instance.tier = "new-tier"
    assert instance.tier != was
    err = """\
invalid configuration: instance restarted with a different `tier`, which is not allowed, was: 'default' became: 'new-tier'
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched
    instance.tier = was

    #
    # Change replicaset_id
    #
    was = instance.replicaset_id  # type: ignore
    instance.replicaset_id = "new-replicaset-id"
    assert instance.replicaset_id != was
    err = """\
invalid configuration: instance restarted with a different `replicaset_id`, which is not allowed, was: 'r1' became: 'new-replicaset-id'
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched
    instance.replicaset_id = was

    #
    # Change advertise address
    #
    was = instance.listen  # type: ignore
    instance.env["PICODATA_ADVERTISE"] = "example.com:1234"
    assert instance.env["PICODATA_ADVERTISE"] != was
    err = f"""\
invalid configuration: instance restarted with a different `advertise_address`, which is not allowed, was: '{was}' became: 'example.com:1234'
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched
    del instance.env["PICODATA_ADVERTISE"]


def test_whoami(instance: Instance):
    assert instance.call("pico.whoami") == {
        "raft_id": 1,
        "instance_id": "i1",
        "cluster_id": instance.cluster_id,
        "tier": "default",
    }


def test_whoami_in_different_tiers(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    cluster_id: test
    tiers:
        storage:
            replication_factor: 2
            can_vote: true
        router:
            replication_factor: 1
            can_vote: true
"""
    )
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
    raft_log = instance.call("pico.raft_log", dict(max_width=9000))

    raft_log = str.join("\n", raft_log)

    def preprocess(s: str):
        res = []
        tail = s
        while tail:
            line, *ltail = tail.split("\n", 1)
            tail = ltail[0] if ltail else ""

            line = line.strip()
            if line.endswith("-+"):
                # don't care about the vertical lines
                continue

            # generated output should only contain endlines after "+" (handled
            # above) or "|", but in our expected string we may add linebreaks
            # for readability. Here we join these artifically broken up lines.
            # Notice how "\n" is replaced with " ", so keep this in mind when
            # editting the expected string.
            while tail and not line.endswith("|"):
                next_piece, tail = tail.split("\n", 1)
                line += " " + next_piece

            # these are the hacks used to make pretty tables work in tarantool's console
            line = line.replace("\u01c0", "|")
            line = line.replace("\u00a0", " ")

            columns = line.split("|")
            columns = [c.strip() for c in columns]

            # blank out the index column so we don't need to manually update it
            if columns[1].isdigit():
                columns[1] = "69"

            # now let's break up the gigantic raft log rows with long BatchDml
            # entries into several lines, so that each sub operation is on it's
            # own line.
            # That way when you run pytest with `-vv` option the diff
            # is understandable enough.
            # By the way you probaly want to also add `--no-showlocals` flag
            # when running this test.
            contents = columns[3]
            if contents.startswith("BatchDml("):
                res.append(str.join("|", columns[0:3] + ["BatchDml("]))
                cursor = len("BatchDml(")
                op_start = cursor
                paren_depth = 0
                while cursor < len(contents):
                    if contents[cursor] == "(":
                        paren_depth += 1
                    elif contents[cursor] == ")":
                        paren_depth -= 1
                        if paren_depth == 0:
                            # eat also the ", " after the current argument
                            if contents[cursor + 1 : cursor + 3] == ", ":  # noqa: E203
                                cursor += 2
                            res.append(
                                contents[op_start : cursor + 1].strip()  # noqa: E203
                            )
                            op_start = cursor + 1
                    cursor += 1
                # append the rest, this is probably just ")|"
                res.append(contents[op_start:].strip())
            else:
                res.append(str.join("|", columns))

        return str.join("\n", res)

    def space_id(space_name: str):
        return instance.eval("return box.space[...].id", space_name)

    #
    # NOTE: run pytest with `-vv --no-showlocals` options for managable output
    #       in case of test failure
    #
    expected = """\
+-----+----+--------+
|index|term|contents|
+-----+----+--------+
|  0  | 1  |BatchDml(
Replace({_pico_peer_address}, [1,"127.0.0.1:{p}"]),
Insert({_pico_instance}, ["i1","68d4a766-4144-3248-aeb4-e212356716e4",1,"r1","e0df68c5-e7f9-395f-86b3-30ad9e1b7b07",["Offline",0],["Offline",0],{b},"default"]),
Insert({_pico_replicaset}, ["r1","e0df68c5-e7f9-395f-86b3-30ad9e1b7b07","i1","i1","default",0.0,"auto","not-ready"]))|
|  0  | 1  |BatchDml(Insert({_pico_tier}, ["default",1,true]))|
|  0  | 1  |BatchDml(
Insert({_pico_property}, ["global_schema_version",0]),
Insert({_pico_property}, ["next_schema_version",1]),
Insert({_pico_property}, ["password_min_length",8]),
Insert({_pico_property}, ["password_enforce_uppercase",true]),
Insert({_pico_property}, ["password_enforce_lowercase",true]),
Insert({_pico_property}, ["password_enforce_digits",true]),
Insert({_pico_property}, ["password_enforce_specialchars",false]),
Insert({_pico_property}, ["auto_offline_timeout",5.0]),
Insert({_pico_property}, ["max_heartbeat_period",5.0]),
Insert({_pico_property}, ["max_pg_statements",50]),
Insert({_pico_property}, ["max_pg_portals",50]),
Insert({_pico_property}, ["snapshot_chunk_max_size",16777216]),
Insert({_pico_property}, ["snapshot_read_view_close_timeout",86400.0]))|
|  0  | 1  |BatchDml(
Insert({_pico_user}, [0,"guest",0,["chap-sha1","vhvewKp0tNyweZQ+cFKAlsyphfg="],1,"user"]),
Insert({_pico_privilege}, ["login","universe",0,0,1,0]),
Insert({_pico_privilege}, ["execute","role",2,0,1,0]),
Insert({_pico_user}, [1,"admin",0,["chap-sha1",""],1,"user"]),
Insert({_pico_privilege}, ["read","universe",0,1,1,0]),
Insert({_pico_privilege}, ["write","universe",0,1,1,0]),
Insert({_pico_privilege}, ["execute","universe",0,1,1,0]),
Insert({_pico_privilege}, ["login","universe",0,1,1,0]),
Insert({_pico_privilege}, ["create","universe",0,1,1,0]),
Insert({_pico_privilege}, ["drop","universe",0,1,1,0]),
Insert({_pico_privilege}, ["alter","universe",0,1,1,0]),
Insert({_pico_user}, [32,"pico_service",0,["chap-sha1","WMA2zaUdjou7vy+epavxEa2kRPA="],1,"user"]),
Insert({_pico_privilege}, ["read","universe",0,32,1,0]),
Insert({_pico_privilege}, ["write","universe",0,32,1,0]),
Insert({_pico_privilege}, ["execute","universe",0,32,1,0]),
Insert({_pico_privilege}, ["login","universe",0,32,1,0]),
Insert({_pico_privilege}, ["create","universe",0,32,1,0]),
Insert({_pico_privilege}, ["drop","universe",0,32,1,0]),
Insert({_pico_privilege}, ["alter","universe",0,32,1,0]),
Insert({_pico_privilege}, ["execute","role",3,32,1,0]),
Insert({_pico_user}, [2,"public",0,null,1,"role"]),
Insert({_pico_user}, [31,"super",0,null,1,"role"]))|
|  0  | 1  |BatchDml(
Insert({_pico_table}, [{_pico_table},"_pico_table",{{"Global":null}},[{{"field_type":"unsigned","is_nullable":false,"name":"id"}},{{"field_type":"string","is_nullable":false,"name":"name"}},{{"field_type":"map","is_nullable":false,"name":"distribution"}},{{"field_type":"array","is_nullable":false,"name":"format"}},{{"field_type":"unsigned","is_nullable":false,"name":"schema_version"}},{{"field_type":"boolean","is_nullable":false,"name":"operable"}},{{"field_type":"string","is_nullable":false,"name":"engine"}},{{"field_type":"unsigned","is_nullable":false,"name":"owner"}}],0,true,"memtx",1]),
Insert({_pico_index}, [{_pico_table},0,"_pico_table_id","tree",[{{"unique":true}}],[["id",null,null,null,null]],true,0,1]),
Insert({_pico_index}, [{_pico_table},1,"_pico_table_name","tree",[{{"unique":true}}],[["name",null,null,null,null]],true,0,1]),
Insert({_pico_table}, [{_pico_index},"_pico_index",{{"Global":null}},[{{"field_type":"unsigned","is_nullable":false,"name":"table_id"}},{{"field_type":"unsigned","is_nullable":false,"name":"id"}},{{"field_type":"string","is_nullable":false,"name":"name"}},{{"field_type":"string","is_nullable":false,"name":"type"}},{{"field_type":"array","is_nullable":false,"name":"opts"}},{{"field_type":"array","is_nullable":false,"name":"parts"}},{{"field_type":"boolean","is_nullable":false,"name":"operable"}},{{"field_type":"unsigned","is_nullable":false,"name":"schema_version"}},{{"field_type":"unsigned","is_nullable":false,"name":"owner"}}],0,true,"memtx",1]),
Insert({_pico_index}, [{_pico_index},0,"_pico_index_id","tree",[{{"unique":true}}],[["table_id",null,null,null,null],["id",null,null,null,null]],true,0,1]),
Insert({_pico_index}, [{_pico_index},1,"_pico_index_name","tree",[{{"unique":true}}],[["table_id",null,null,null,null],["name",null,null,null,null]],true,0,1]),
Insert({_pico_table}, [{_pico_peer_address},"_pico_peer_address",{{"Global":null}},[{{"field_type":"unsigned","is_nullable":false,"name":"raft_id"}},{{"field_type":"string","is_nullable":false,"name":"address"}}],0,true,"memtx",1]),
Insert({_pico_index}, [{_pico_peer_address},0,"_pico_peer_address_raft_id","tree",[{{"unique":true}}],[["raft_id",null,null,null,null]],true,0,1]),
Insert({_pico_table}, [{_pico_instance},"_pico_instance",{{"Global":null}},[{{"field_type":"string","is_nullable":false,"name":"instance_id"}},{{"field_type":"string","is_nullable":false,"name":"instance_uuid"}},{{"field_type":"unsigned","is_nullable":false,"name":"raft_id"}},{{"field_type":"string","is_nullable":false,"name":"replicaset_id"}},{{"field_type":"string","is_nullable":false,"name":"replicaset_uuid"}},{{"field_type":"array","is_nullable":false,"name":"current_grade"}},{{"field_type":"array","is_nullable":false,"name":"target_grade"}},{{"field_type":"map","is_nullable":false,"name":"failure_domain"}},{{"field_type":"string","is_nullable":false,"name":"tier"}}],0,true,"memtx",1]),
Insert({_pico_index}, [{_pico_instance},0,"_pico_instance_id","tree",[{{"unique":true}}],[["instance_id",null,null,null,null]],true,0,1]),
Insert({_pico_index}, [{_pico_instance},1,"_pico_instance_raft_id","tree",[{{"unique":true}}],[["raft_id",null,null,null,null]],true,0,1]),
Insert({_pico_index}, [{_pico_instance},2,"_pico_instance_replicaset_id","tree",[{{"unique":false}}],[["replicaset_id",null,null,null,null]],true,0,1]),
Insert({_pico_table}, [{_pico_property},"_pico_property",{{"Global":null}},[{{"field_type":"string","is_nullable":false,"name":"key"}},{{"field_type":"any","is_nullable":false,"name":"value"}}],0,true,"memtx",1]),
Insert({_pico_index}, [{_pico_property},0,"_pico_property_key","tree",[{{"unique":true}}],[["key",null,null,null,null]],true,0,1]),
Insert({_pico_table}, [{_pico_replicaset},"_pico_replicaset",{{"Global":null}},[{{"field_type":"string","is_nullable":false,"name":"replicaset_id"}},{{"field_type":"string","is_nullable":false,"name":"replicaset_uuid"}},{{"field_type":"string","is_nullable":false,"name":"current_master_id"}},{{"field_type":"string","is_nullable":false,"name":"target_master_id"}},{{"field_type":"string","is_nullable":false,"name":"tier"}},{{"field_type":"number","is_nullable":false,"name":"weight"}},{{"field_type":"string","is_nullable":false,"name":"weight_origin"}},{{"field_type":"string","is_nullable":false,"name":"state"}}],0,true,"memtx",1]),
Insert({_pico_index}, [{_pico_replicaset},0,"_pico_replicaset_id","tree",[{{"unique":true}}],[["replicaset_id",null,null,null,null]],true,0,1]),
Insert({_pico_table}, [{_pico_user},"_pico_user",{{"Global":null}},[{{"field_type":"unsigned","is_nullable":false,"name":"id"}},{{"field_type":"string","is_nullable":false,"name":"name"}},{{"field_type":"unsigned","is_nullable":false,"name":"schema_version"}},{{"field_type":"array","is_nullable":true,"name":"auth"}},{{"field_type":"unsigned","is_nullable":false,"name":"owner"}},{{"field_type":"string","is_nullable":false,"name":"type"}}],0,true,"memtx",1]),
Insert({_pico_index}, [{_pico_user},0,"_pico_user_id","tree",[{{"unique":true}}],[["id",null,null,null,null]],true,0,1]),
Insert({_pico_index}, [{_pico_user},1,"_pico_user_name","tree",[{{"unique":true}}],[["name",null,null,null,null]],true,0,1]),
Insert({_pico_table}, [{_pico_privilege},"_pico_privilege",{{"Global":null}},[{{"field_type":"string","is_nullable":false,"name":"privilege"}},{{"field_type":"string","is_nullable":false,"name":"object_type"}},{{"field_type":"integer","is_nullable":false,"name":"object_id"}},{{"field_type":"unsigned","is_nullable":false,"name":"grantee_id"}},{{"field_type":"unsigned","is_nullable":false,"name":"grantor_id"}},{{"field_type":"unsigned","is_nullable":false,"name":"schema_version"}}],0,true,"memtx",1]),
Insert({_pico_index}, [{_pico_privilege},0,"_pico_privilege_primary","tree",[{{"unique":true}}],[["grantee_id",null,null,null,null],["object_type",null,null,null,null],["object_id",null,null,null,null],["privilege",null,null,null,null]],true,0,1]),
Insert({_pico_index}, [{_pico_privilege},1,"_pico_privilege_object","tree",[{{"unique":false}}],[["object_type",null,null,null,null],["object_id",null,null,null,null]],true,0,1]),
Insert({_pico_table}, [{_pico_tier},"_pico_tier",{{"Global":null}},[{{"field_type":"string","is_nullable":false,"name":"name"}},{{"field_type":"unsigned","is_nullable":false,"name":"replication_factor"}},{{"field_type":"boolean","is_nullable":false,"name":"can_vote"}}],0,true,"memtx",1]),
Insert({_pico_index}, [{_pico_tier},0,"_pico_tier_name","tree",[{{"unique":true}}],[["name",null,null,null,null]],true,0,1]),
Insert({_pico_table}, [{_pico_routine},"_pico_routine",{{"Global":null}},[{{"field_type":"unsigned","is_nullable":false,"name":"id"}},{{"field_type":"string","is_nullable":false,"name":"name"}},{{"field_type":"string","is_nullable":false,"name":"kind"}},{{"field_type":"array","is_nullable":false,"name":"params"}},{{"field_type":"array","is_nullable":false,"name":"returns"}},{{"field_type":"string","is_nullable":false,"name":"language"}},{{"field_type":"string","is_nullable":false,"name":"body"}},{{"field_type":"string","is_nullable":false,"name":"security"}},{{"field_type":"boolean","is_nullable":false,"name":"operable"}},{{"field_type":"unsigned","is_nullable":false,"name":"schema_version"}},{{"field_type":"unsigned","is_nullable":false,"name":"owner"}}],0,true,"memtx",1]),
Insert({_pico_index}, [{_pico_routine},0,"_pico_routine_id","tree",[{{"unique":true}}],[["id",null,null,null,null]],true,0,1]),
Insert({_pico_index}, [{_pico_routine},1,"_pico_routine_name","tree",[{{"unique":true}}],[["name",null,null,null,null]],true,0,1]),
Insert({_pico_table}, [{_pico_plugin},"_pico_plugin",{{"Global":null}},[{{"field_type":"string","is_nullable":false,"name":"name"}},{{"field_type":"boolean","is_nullable":false,"name":"enabled"}},{{"field_type":"array","is_nullable":false,"name":"services"}},{{"field_type":"string","is_nullable":false,"name":"version"}},{{"field_type":"string","is_nullable":false,"name":"description"}},{{"field_type":"array","is_nullable":false,"name":"migration_list"}},{{"field_type":"integer","is_nullable":false,"name":"migration_progress"}}],0,true,"memtx",1]),
Insert({_pico_index}, [{_pico_plugin},0,"_pico_plugin_name","tree",[{{"unique":true}}],[["name",null,null,null,null]],true,0,1]),
Insert({_pico_table}, [{_pico_service},"_pico_service",{{"Global":null}},[{{"field_type":"string","is_nullable":false,"name":"plugin_name"}},{{"field_type":"string","is_nullable":false,"name":"name"}},{{"field_type":"string","is_nullable":false,"name":"version"}},{{"field_type":"array","is_nullable":false,"name":"tiers"}},{{"field_type":"any","is_nullable":false,"name":"configuration"}},{{"field_type":"string","is_nullable":false,"name":"description"}}],0,true,"memtx",1]),
Insert({_pico_index}, [{_pico_service},0,"_pico_service_name","tree",[{{"unique":true}}],[["plugin_name",null,null,null,null],["name",null,null,null,null],["version",null,null,null,null]],true,0,1]),
Insert({_pico_table}, [{_pico_service_route},"_pico_service_route",{{"Global":null}},[{{"field_type":"string","is_nullable":false,"name":"instance_id"}},{{"field_type":"string","is_nullable":false,"name":"plugin_name"}},{{"field_type":"string","is_nullable":false,"name":"service_name"}},{{"field_type":"boolean","is_nullable":false,"name":"poison"}}],0,true,"memtx",1]),
Insert({_pico_index}, [{_pico_service_route},0,"_pico_service_routing_key","tree",[{{"unique":true}}],[["instance_id",null,null,null,null],["plugin_name",null,null,null,null],["service_name",null,null,null,null]],true,0,1])
)|
|  0  | 1  |AddNode(1)|
|  0  | 2  |-|
|  0  | 2  |Replace({_pico_instance}, ["i1","68d4a766-4144-3248-aeb4-e212356716e4",1,"r1","e0df68c5-e7f9-395f-86b3-30ad9e1b7b07",["Offline",0],["Online",1],{b},"default"])|
|  0  | 2  |Replace({_pico_instance}, ["i1","68d4a766-4144-3248-aeb4-e212356716e4",1,"r1","e0df68c5-e7f9-395f-86b3-30ad9e1b7b07",["Replicated",1],["Online",1],{b},"default"])|
|  0  | 2  |Update({_pico_replicaset}, ["r1"], [["=","weight",1.0], ["=","state","ready"]])|
|  0  | 2  |Replace({_pico_property}, ["target_vshard_config",[{{"e0df68c5-e7f9-395f-86b3-30ad9e1b7b07":[{{"68d4a766-4144-3248-aeb4-e212356716e4":["pico_service@127.0.0.1:{p}","i1",true]}},1.0]}},"on"]])|
|  0  | 2  |Replace({_pico_property}, ["current_vshard_config",[{{"e0df68c5-e7f9-395f-86b3-30ad9e1b7b07":[{{"68d4a766-4144-3248-aeb4-e212356716e4":["pico_service@127.0.0.1:{p}","i1",true]}},1.0]}},"on"]])|
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
        _pico_table=space_id("_pico_table"),
        _pico_index=space_id("_pico_index"),
        _pico_plugin=space_id("_pico_plugin"),
        _pico_service=space_id("_pico_service"),
        _pico_service_route=space_id("_pico_service_route"),
    )
    try:
        assert preprocess(raft_log) == preprocess(expected)
    except AssertionError as e:
        # hide the huge string variables from the verbose pytest output enabled
        # by the `--showlocals` option
        del raft_log
        del expected
        raise e from e


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
    assert info.keys() == set(["picodata_version", "rpc_api_version"])  # type: ignore


def test_proc_instance_info(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    cluster_id: test
    tiers:
        storage:
            replication_factor: 2
        router:
            replication_factor: 1
"""
    )

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


def test_pico_service_password_security_warning(cluster: Cluster):
    password_file = f"{cluster.data_dir}/service-password.txt"
    with open(password_file, "w") as f:
        print("secret", file=f)

    i1 = cluster.add_instance(wait_online=False)
    i1.service_password_file = password_file

    message = "service password file's permissions are too open, this is a security risk"  # noqa: E501
    lc = log_crawler(i1, message)
    i1.start()
    i1.wait_online()
    assert lc.matched

    i1.terminate()
    i1.remove_data()

    os.chmod(password_file, 0o600)

    lc.matched = False
    i1.start()
    i1.wait_online()
    assert not lc.matched
