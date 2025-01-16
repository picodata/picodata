import os
import pytest
import signal

from conftest import (
    Cluster,
    Instance,
    ProcessDead,
    Retriable,
    TarantoolError,
    ReturnError,
    ErrorCode,
    MalformedAPI,
    log_crawler,
    pgrep_tree,
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
    assert e1.value.args[:2] == ("some error",)

    with pytest.raises(MalformedAPI) as e2:
        instance.call("dostring", "return 'x', 1")
    assert e2.value.args[:2] == ("x", 1)

    with pytest.raises(TarantoolError) as e3:
        instance.call("error", "lua exception", 0)
    assert e3.value.args[:2] == ("ER_PROC_LUA", "lua exception")

    with pytest.raises(TarantoolError) as e4:
        instance.call("void")
    assert e4.value.args[:2] == ("ER_NO_SUCH_PROC", "Procedure 'void' is not defined")

    with pytest.raises(TimeoutError):
        instance.call("package.loaded.fiber.sleep", 1, timeout=0.1)

    with pytest.raises(ProcessDead):
        instance.call("os.exit", 0)

    instance.terminate()
    with pytest.raises(ProcessDead):
        instance.call("anything")


def test_eval_normalization(instance: Instance):
    assert instance.eval("return") is None
    assert instance.eval("return 1") == 1
    assert instance.eval("return { }") == []
    assert instance.eval("return 's'") == "s"
    assert instance.eval("return nil") is None
    assert instance.eval("return true") is True

    with pytest.raises(ReturnError) as e1:
        instance.eval("return nil, 'some error'")
    assert e1.value.args[:2] == ("some error",)

    with pytest.raises(MalformedAPI) as e2:
        instance.eval("return 'x', 2")
    assert e2.value.args[:2] == ("x", 2)

    with pytest.raises(TarantoolError) as e3:
        instance.eval("error('lua exception', 0)")
    assert e3.value.args[:2] == ("ER_PROC_LUA", "lua exception")

    with pytest.raises(TarantoolError) as e4:
        instance.eval("return box.schema.space.drop(0, 'void')")
    assert e4.value.args[:2] == ("ER_NO_SUCH_SPACE", "Space 'void' does not exist")


def test_process_management(instance: Instance):
    assert instance.eval("return 'ok'") == "ok"
    assert instance.process is not None
    pid = instance.process.pid
    pgrp = pid

    pids = pgrep_tree(pid)
    assert len(pids) == 1

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
    with pytest.raises(ProcessDead):
        instance.eval("return 'ok'", timeout=0.1)

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
        [f for f in os.listdir(instance.instance_dir) if f.endswith(".xlog")]
    )
    with open(os.path.join(instance.instance_dir, last_xlog), "rb") as f:
        assert f.read()[-4:] == b"\xd5\x10\xad\xed"


def test_config_storage_conflicts_on_restart(instance: Instance):
    instance.terminate()

    #
    # Change cluster_name
    #
    was = instance.cluster_name  # type: ignore
    instance.cluster_name = "new-cluster-name"
    assert instance.cluster_name != was
    err = f"""\
invalid configuration: instance restarted with a different `cluster_name`, which is not allowed, was: '{was}' became: 'new-cluster-name'
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched
    instance.cluster_name = was

    #
    # Change instance name
    #
    was = instance.name  # type: ignore
    instance.name = "new-instance-name"
    assert instance.name != was
    err = f"""\
invalid configuration: instance restarted with a different `instance_name`, which is not allowed, was: '{was}' became: 'new-instance-name'
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched
    instance.name = was

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
    # Change replicaset_name
    #
    was = instance.replicaset_name  # type: ignore
    instance.replicaset_name = "new-replicaset-name"
    assert instance.replicaset_name != was
    err = f"""\
invalid configuration: instance restarted with a different `replicaset_name`, which is not allowed, was: '{was}' became: 'new-replicaset-name'
"""  # noqa: E501
    crawler = log_crawler(instance, err)
    instance.fail_to_start()
    assert crawler.matched
    instance.replicaset_name = was

    #
    # Change advertise address
    #
    was = instance.iproto_listen  # type: ignore
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
        "instance_name": "default_1_1",
        "cluster_name": instance.cluster_name,
        "tier": "default",
    }


def test_whoami_in_different_tiers(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
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
        "instance_name": "storage_1_1",
        "cluster_name": i1.cluster_name,
        "tier": "storage",
    }

    assert i2.call("pico.whoami") == {
        "raft_id": 2,
        "instance_name": "router_1_1",
        "cluster_name": i2.cluster_name,
        "tier": "router",
    }


def test_pico_instance_info(instance: Instance):
    def instance_info(iid: str | None = None):
        return instance.call("pico.instance_info", iid)

    # Don't compare entire structure, a couple of fields is enough
    myself = instance_info("default_1_1")
    assert myself["raft_id"] == 1
    assert myself["name"] == "default_1_1"
    assert myself["replicaset_name"] == "default_1"

    with pytest.raises(ReturnError) as e:
        instance_info("i2")
    assert e.value.args[:2] == ('instance with name "i2" not found',)

    assert instance_info() == myself


def test_raft_log(instance: Instance):
    raft_log = instance.call("pico.raft_log", dict(max_width=99000))

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
            # for readability. Here we join these artificially broken up lines.
            # Notice how "\n" is replaced with " ", so keep this in mind when
            # editing the expected string.
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
            # By the way you probably want to also add `--no-showlocals` flag
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
    # NOTE: run pytest with `-vv --no-showlocals` options for manageable output
    #       in case of test failure
    #
    expected = """\
+-----+----+--------+
|index|term|contents|
+-----+----+--------+
|  0  | 1  |BatchDml(
Replace(_pico_peer_address, [1,"127.0.0.1:{p}"]),
Insert(_pico_instance, ["default_1_1","{i1_uuid}",1,"default_1","{r1_uuid}",["Offline",0],["Offline",0],{b},"default"]),
Insert(_pico_replicaset, ["default_1","{r1_uuid}","default_1_1","default_1_1","default",0.0,"auto","not-ready",0,0,{{}}]))|
|  0  | 1  |BatchDml(Insert(_pico_tier, ["default",1,true,0,0,false]))|
|  0  | 1  |BatchDml(
Insert(_pico_property, ["global_schema_version",0]),
Insert(_pico_property, ["next_schema_version",1]),
Insert(_pico_property, ["system_catalog_version",1]),
Insert(_pico_db_config, ["auth_password_length_min",8]),
Insert(_pico_db_config, ["auth_password_enforce_uppercase",true]),
Insert(_pico_db_config, ["auth_password_enforce_lowercase",true]),
Insert(_pico_db_config, ["auth_password_enforce_digits",true]),
Insert(_pico_db_config, ["auth_password_enforce_specialchars",false]),
Insert(_pico_db_config, ["auth_login_attempt_max",4]),
Insert(_pico_db_config, ["pg_statement_max",1024]),
Insert(_pico_db_config, ["pg_portal_max",1024]),
Insert(_pico_db_config, ["raft_snapshot_chunk_size_max",16777216]),
Insert(_pico_db_config, ["raft_snapshot_read_view_close_timeout",86400]),
Insert(_pico_db_config, ["raft_wal_size_max",67108864]),
Insert(_pico_db_config, ["raft_wal_count_max",64]),
Insert(_pico_db_config, ["governor_auto_offline_timeout",30.0]),
Insert(_pico_db_config, ["governor_raft_op_timeout",3.0]),
Insert(_pico_db_config, ["governor_common_rpc_timeout",3.0]),
Insert(_pico_db_config, ["governor_plugin_rpc_timeout",10.0]),
Insert(_pico_db_config, ["sql_vdbe_opcode_max",45000]),
Insert(_pico_db_config, ["sql_motion_row_max",5000]),
Insert(_pico_db_config, ["memtx_checkpoint_count",2]),
Insert(_pico_db_config, ["memtx_checkpoint_interval",3600]),
Insert(_pico_db_config, ["iproto_net_msg_max",768]))|
|  0  | 1  |BatchDml(
Insert(_pico_user, [0,"guest",0,["md5","md5084e0343a0486ff05530df6c705c8bb4"],1,"user"]),
Insert(_pico_privilege, [1,0,"login","universe",0,0]),
Insert(_pico_privilege, [1,0,"execute","role",2,0]),
Insert(_pico_user, [1,"admin",0,["md5",""],1,"user"]),
Insert(_pico_privilege, [1,1,"read","universe",0,0]),
Insert(_pico_privilege, [1,1,"write","universe",0,0]),
Insert(_pico_privilege, [1,1,"execute","universe",0,0]),
Insert(_pico_privilege, [1,1,"login","universe",0,0]),
Insert(_pico_privilege, [1,1,"create","universe",0,0]),
Insert(_pico_privilege, [1,1,"drop","universe",0,0]),
Insert(_pico_privilege, [1,1,"alter","universe",0,0]),
Insert(_pico_user, [32,"pico_service",0,["chap-sha1","WMA2zaUdjou7vy+epavxEa2kRPA="],1,"user"]),
Insert(_pico_privilege, [1,32,"read","universe",0,0]),
Insert(_pico_privilege, [1,32,"write","universe",0,0]),
Insert(_pico_privilege, [1,32,"execute","universe",0,0]),
Insert(_pico_privilege, [1,32,"login","universe",0,0]),
Insert(_pico_privilege, [1,32,"create","universe",0,0]),
Insert(_pico_privilege, [1,32,"drop","universe",0,0]),
Insert(_pico_privilege, [1,32,"alter","universe",0,0]),
Insert(_pico_privilege, [1,32,"execute","role",3,0]),
Insert(_pico_user, [2,"public",0,null,1,"role"]),
Insert(_pico_user, [31,"super",0,null,1,"role"]),
Insert(_pico_user, [3,"replication",0,null,1,"role"]))|
|  0  | 1  |ChangeAuth(1, 0, 1)|
|  0  | 1  |BatchDml(
Insert(_pico_table, [{_pico_table},"_pico_table",{{"Global":null}},[{{"field_type":"unsigned","is_nullable":false,"name":"id"}},{{"field_type":"string","is_nullable":false,"name":"name"}},{{"field_type":"map","is_nullable":false,"name":"distribution"}},{{"field_type":"array","is_nullable":false,"name":"format"}},{{"field_type":"unsigned","is_nullable":false,"name":"schema_version"}},{{"field_type":"boolean","is_nullable":false,"name":"operable"}},{{"field_type":"string","is_nullable":false,"name":"engine"}},{{"field_type":"unsigned","is_nullable":false,"name":"owner"}},{{"field_type":"string","is_nullable":false,"name":"description"}}],0,true,"memtx",1,"Stores metadata of all the cluster tables in picodata."]),
Insert(_pico_index, [{_pico_table},0,"_pico_table_id","tree",[{{"unique":true}}],[["id","unsigned",null,false,null]],true,0]),
Insert(_pico_index, [{_pico_table},1,"_pico_table_name","tree",[{{"unique":true}}],[["name","string",null,false,null]],true,0]),
Insert(_pico_index, [{_pico_table},2,"_pico_table_owner_id","tree",[{{"unique":false}}],[["owner","unsigned",null,false,null]],true,0]),
Insert(_pico_table, [{_pico_index},"_pico_index",{{"Global":null}},[{{"field_type":"unsigned","is_nullable":false,"name":"table_id"}},{{"field_type":"unsigned","is_nullable":false,"name":"id"}},{{"field_type":"string","is_nullable":false,"name":"name"}},{{"field_type":"string","is_nullable":false,"name":"type"}},{{"field_type":"array","is_nullable":false,"name":"opts"}},{{"field_type":"array","is_nullable":false,"name":"parts"}},{{"field_type":"boolean","is_nullable":false,"name":"operable"}},{{"field_type":"unsigned","is_nullable":false,"name":"schema_version"}}],0,true,"memtx",1,""]),
Insert(_pico_index, [{_pico_index},0,"_pico_index_id","tree",[{{"unique":true}}],[["table_id","unsigned",null,false,null],["id","unsigned",null,false,null]],true,0]),
Insert(_pico_index, [{_pico_index},1,"_pico_index_name","tree",[{{"unique":true}}],[["name","string",null,false,null]],true,0]),
Insert(_pico_table, [{_pico_peer_address},"_pico_peer_address",{{"Global":null}},[{{"field_type":"unsigned","is_nullable":false,"name":"raft_id"}},{{"field_type":"string","is_nullable":false,"name":"address"}}],0,true,"memtx",1,""]),
Insert(_pico_index, [{_pico_peer_address},0,"_pico_peer_address_raft_id","tree",[{{"unique":true}}],[["raft_id","unsigned",null,false,null]],true,0]),
Insert(_pico_table, [{_pico_instance},"_pico_instance",{{"Global":null}},[{{"field_type":"string","is_nullable":false,"name":"name"}},{{"field_type":"string","is_nullable":false,"name":"uuid"}},{{"field_type":"unsigned","is_nullable":false,"name":"raft_id"}},{{"field_type":"string","is_nullable":false,"name":"replicaset_name"}},{{"field_type":"string","is_nullable":false,"name":"replicaset_uuid"}},{{"field_type":"array","is_nullable":false,"name":"current_state"}},{{"field_type":"array","is_nullable":false,"name":"target_state"}},{{"field_type":"map","is_nullable":false,"name":"failure_domain"}},{{"field_type":"string","is_nullable":false,"name":"tier"}}],0,true,"memtx",1,""]),
Insert(_pico_index, [{_pico_instance},0,"_pico_instance_name","tree",[{{"unique":true}}],[["name","string",null,false,null]],true,0]),
Insert(_pico_index, [{_pico_instance},1,"_pico_instance_uuid","tree",[{{"unique":true}}],[["uuid","string",null,false,null]],true,0]),
Insert(_pico_index, [{_pico_instance},2,"_pico_instance_raft_id","tree",[{{"unique":true}}],[["raft_id","unsigned",null,false,null]],true,0]),
Insert(_pico_index, [{_pico_instance},3,"_pico_instance_replicaset_name","tree",[{{"unique":false}}],[["replicaset_name","string",null,false,null]],true,0]),
Insert(_pico_table, [{_pico_property},"_pico_property",{{"Global":null}},[{{"field_type":"string","is_nullable":false,"name":"key"}},{{"field_type":"any","is_nullable":false,"name":"value"}}],0,true,"memtx",1,""]),
Insert(_pico_index, [{_pico_property},0,"_pico_property_key","tree",[{{"unique":true}}],[["key","string",null,false,null]],true,0]),
Insert(_pico_table, [{_pico_replicaset},"_pico_replicaset",{{"Global":null}},[{{"field_type":"string","is_nullable":false,"name":"name"}},{{"field_type":"string","is_nullable":false,"name":"uuid"}},{{"field_type":"string","is_nullable":false,"name":"current_master_name"}},{{"field_type":"string","is_nullable":false,"name":"target_master_name"}},{{"field_type":"string","is_nullable":false,"name":"tier"}},{{"field_type":"double","is_nullable":false,"name":"weight"}},{{"field_type":"string","is_nullable":false,"name":"weight_origin"}},{{"field_type":"string","is_nullable":false,"name":"state"}},{{"field_type":"unsigned","is_nullable":false,"name":"current_config_version"}},{{"field_type":"unsigned","is_nullable":false,"name":"target_config_version"}},{{"field_type":"map","is_nullable":false,"name":"promotion_vclock"}}],0,true,"memtx",1,""]),
Insert(_pico_index, [{_pico_replicaset},0,"_pico_replicaset_name","tree",[{{"unique":true}}],[["name","string",null,false,null]],true,0]),
Insert(_pico_index, [{_pico_replicaset},1,"_pico_replicaset_uuid","tree",[{{"unique":true}}],[["uuid","string",null,false,null]],true,0]),
Insert(_pico_table, [{_pico_user},"_pico_user",{{"Global":null}},[{{"field_type":"unsigned","is_nullable":false,"name":"id"}},{{"field_type":"string","is_nullable":false,"name":"name"}},{{"field_type":"unsigned","is_nullable":false,"name":"schema_version"}},{{"field_type":"array","is_nullable":true,"name":"auth"}},{{"field_type":"unsigned","is_nullable":false,"name":"owner"}},{{"field_type":"string","is_nullable":false,"name":"type"}}],0,true,"memtx",1,""]),
Insert(_pico_index, [{_pico_user},0,"_pico_user_id","tree",[{{"unique":true}}],[["id","unsigned",null,false,null]],true,0]),
Insert(_pico_index, [{_pico_user},1,"_pico_user_name","tree",[{{"unique":true}}],[["name","string",null,false,null]],true,0]),
Insert(_pico_index, [{_pico_user},2,"_pico_user_owner_id","tree",[{{"unique":false}}],[["owner","unsigned",null,false,null]],true,0]),
Insert(_pico_table, [{_pico_privilege},"_pico_privilege",{{"Global":null}},[{{"field_type":"unsigned","is_nullable":false,"name":"grantor_id"}},{{"field_type":"unsigned","is_nullable":false,"name":"grantee_id"}},{{"field_type":"string","is_nullable":false,"name":"privilege"}},{{"field_type":"string","is_nullable":false,"name":"object_type"}},{{"field_type":"integer","is_nullable":false,"name":"object_id"}},{{"field_type":"unsigned","is_nullable":false,"name":"schema_version"}}],0,true,"memtx",1,""]),
Insert(_pico_index, [{_pico_privilege},0,"_pico_privilege_primary","tree",[{{"unique":true}}],[["grantee_id","unsigned",null,false,null],["object_type","string",null,false,null],["object_id","integer",null,false,null],["privilege","string",null,false,null]],true,0]),
Insert(_pico_index, [{_pico_privilege},1,"_pico_privilege_object","tree",[{{"unique":false}}],[["object_type","string",null,false,null],["object_id","integer",null,false,null]],true,0]),
Insert(_pico_table, [{_pico_tier},"_pico_tier",{{"Global":null}},[{{"field_type":"string","is_nullable":false,"name":"name"}},{{"field_type":"unsigned","is_nullable":false,"name":"replication_factor"}},{{"field_type":"boolean","is_nullable":false,"name":"can_vote"}},{{"field_type":"unsigned","is_nullable":false,"name":"current_vshard_config_version"}},{{"field_type":"unsigned","is_nullable":false,"name":"target_vshard_config_version"}},{{"field_type":"boolean","is_nullable":false,"name":"vshard_bootstrapped"}}],0,true,"memtx",1,""]),
Insert(_pico_index, [{_pico_tier},0,"_pico_tier_name","tree",[{{"unique":true}}],[["name","string",null,false,null]],true,0]),
Insert(_pico_table, [{_pico_routine},"_pico_routine",{{"Global":null}},[{{"field_type":"unsigned","is_nullable":false,"name":"id"}},{{"field_type":"string","is_nullable":false,"name":"name"}},{{"field_type":"string","is_nullable":false,"name":"kind"}},{{"field_type":"array","is_nullable":false,"name":"params"}},{{"field_type":"array","is_nullable":false,"name":"returns"}},{{"field_type":"string","is_nullable":false,"name":"language"}},{{"field_type":"string","is_nullable":false,"name":"body"}},{{"field_type":"string","is_nullable":false,"name":"security"}},{{"field_type":"boolean","is_nullable":false,"name":"operable"}},{{"field_type":"unsigned","is_nullable":false,"name":"schema_version"}},{{"field_type":"unsigned","is_nullable":false,"name":"owner"}}],0,true,"memtx",1,""]),
Insert(_pico_index, [{_pico_routine},0,"_pico_routine_id","tree",[{{"unique":true}}],[["id","unsigned",null,false,null]],true,0]),
Insert(_pico_index, [{_pico_routine},1,"_pico_routine_name","tree",[{{"unique":true}}],[["name","string",null,false,null]],true,0]),
Insert(_pico_index, [{_pico_routine},2,"_pico_routine_owner_id","tree",[{{"unique":false}}],[["owner","unsigned",null,false,null]],true,0]),
Insert(_pico_table, [{_pico_plugin},"_pico_plugin",{{"Global":null}},[{{"field_type":"string","is_nullable":false,"name":"name"}},{{"field_type":"boolean","is_nullable":false,"name":"enabled"}},{{"field_type":"array","is_nullable":false,"name":"services"}},{{"field_type":"string","is_nullable":false,"name":"version"}},{{"field_type":"string","is_nullable":false,"name":"description"}},{{"field_type":"array","is_nullable":false,"name":"migration_list"}}],0,true,"memtx",1,""]),
Insert(_pico_index, [{_pico_plugin},0,"_pico_plugin_name","tree",[{{"unique":true}}],[["name","string",null,false,null],["version","string",null,false,null]],true,0]),
Insert(_pico_table, [{_pico_service},"_pico_service",{{"Global":null}},[{{"field_type":"string","is_nullable":false,"name":"plugin_name"}},{{"field_type":"string","is_nullable":false,"name":"name"}},{{"field_type":"string","is_nullable":false,"name":"version"}},{{"field_type":"array","is_nullable":false,"name":"tiers"}},{{"field_type":"string","is_nullable":false,"name":"description"}}],0,true,"memtx",1,""]),
Insert(_pico_index, [{_pico_service},0,"_pico_service_name","tree",[{{"unique":true}}],[["plugin_name","string",null,false,null],["name","string",null,false,null],["version","string",null,false,null]],true,0]),
Insert(_pico_table, [{_pico_service_route},"_pico_service_route",{{"Global":null}},[{{"field_type":"string","is_nullable":false,"name":"plugin_name"}},{{"field_type":"string","is_nullable":false,"name":"plugin_version"}},{{"field_type":"string","is_nullable":false,"name":"service_name"}},{{"field_type":"string","is_nullable":false,"name":"instance_name"}},{{"field_type":"boolean","is_nullable":false,"name":"poison"}}],0,true,"memtx",1,""]),
Insert(_pico_index, [{_pico_service_route},0,"_pico_service_routing_key","tree",[{{"unique":true}}],[["plugin_name","string",null,false,null],["plugin_version","string",null,false,null],["service_name","string",null,false,null],["instance_name","string",null,false,null]],true,0]),
Insert(_pico_table, [{_pico_plugin_migration},"_pico_plugin_migration",{{"Global":null}},[{{"field_type":"string","is_nullable":false,"name":"plugin_name"}},{{"field_type":"string","is_nullable":false,"name":"migration_file"}},{{"field_type":"string","is_nullable":false,"name":"hash"}}],0,true,"memtx",1,""]),
Insert(_pico_index, [{_pico_plugin_migration},0,"_pico_plugin_migration_primary_key","tree",[{{"unique":true}}],[["plugin_name","string",null,false,null],["migration_file","string",null,false,null]],true,0]),
Insert(_pico_table, [{_pico_plugin_config},"_pico_plugin_config",{{"Global":null}},[{{"field_type":"string","is_nullable":false,"name":"plugin"}},{{"field_type":"string","is_nullable":false,"name":"version"}},{{"field_type":"string","is_nullable":false,"name":"entity"}},{{"field_type":"string","is_nullable":false,"name":"key"}},{{"field_type":"any","is_nullable":true,"name":"value"}}],0,true,"memtx",1,""]),
Insert(_pico_index, [{_pico_plugin_config},0,"_pico_plugin_config_pk","tree",[{{"unique":true}}],[["plugin","string",null,false,null],["version","string",null,false,null],["entity","string",null,false,null],["key","string",null,false,null]],true,0]),
Insert(_pico_table, [{_pico_db_config},"_pico_db_config",{{"Global":null}},[{{"field_type":"string","is_nullable":false,"name":"key"}},{{"field_type":"any","is_nullable":false,"name":"value"}}],0,true,"memtx",1,""]),
Insert(_pico_index, [{_pico_db_config},0,"_pico_db_config_key","tree",[{{"unique":true}}],[["key","string",null,false,null]],true,0])
)|
|  0  | 1  |AddNode(1)|
|  0  | 2  |-|
|  0  | 2  |BatchDml(
Update(_pico_instance, ["default_1_1"], [["=","target_state",["Online",1]]]),
Update(_pico_replicaset, ["default_1"], [["=","target_config_version",1]]),
Update(_pico_tier, ["default"], [["=","target_vshard_config_version",1]])
)|
|  0  | 2  |Update(_pico_replicaset, ["default_1"], [["=","current_config_version",1]])|
|  0  | 2  |Update(_pico_replicaset, ["default_1"], [["=","weight",1.0], ["=","state","ready"]])|
|  69 | 2  |Update(_pico_tier, ["default"], [["=","current_vshard_config_version",1]])|
|  69 | 2  |Update(_pico_tier, ["default"], [["=","vshard_bootstrapped",true]])|
|  69 | 2  |Update(_pico_instance, ["default_1_1"], [["=","current_state",["Online",1]]])|
+-----+----+--------+
""".format(  # noqa: E501
        p=instance.port,
        b="{}",
        i1_uuid=instance.uuid(),
        r1_uuid=instance.replicaset_uuid(),
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
        _pico_plugin_migration=space_id("_pico_plugin_migration"),
        _pico_plugin_config=space_id("_pico_plugin_config"),
        _pico_db_config=space_id("_pico_db_config"),
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
            f"""
                local replicasets = _G.pico.router["{instance.get_tier()}"]:info().replicasets
                local replica_uuid = replicasets[box.info.cluster.uuid].replica.uuid
                return replica_uuid == box.info.uuid
        """
        )

    # vshard is configured after first start
    check_vshard_configured(instance)

    assert instance.current_state() == dict(variant="Online", incarnation=1)

    instance.restart()
    instance.wait_online()

    # vshard is configured again after restart
    check_vshard_configured(instance)

    assert instance.current_state() == dict(variant="Online", incarnation=2)


def test_proc_version_info(instance: Instance):
    info = instance.call(".proc_version_info")
    assert info.keys() == set(
        [
            "picodata_version",
            "rpc_api_version",
            "build_type",
            "build_profile",
            "tarantool_version",
        ]
    )


def test_proc_instance_info(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
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
        name="storage_1_1",
        uuid=i1.uuid(),
        replicaset_name="storage_1",
        replicaset_uuid=i1.replicaset_uuid(),
        cluster_name=i1.cluster_name,
        current_state=dict(variant="Online", incarnation=1),
        target_state=dict(variant="Online", incarnation=1),
        tier="storage",
    )

    i2_info = i2.call(".proc_instance_info")
    assert i2_info == dict(
        raft_id=2,
        advertise_address=f"{i2.host}:{i2.port}",
        name="router_1_1",
        uuid=i2.uuid(),
        replicaset_name="router_1",
        replicaset_uuid=i2.replicaset_uuid(),
        cluster_name=i1.cluster_name,
        current_state=dict(variant="Online", incarnation=1),
        target_state=dict(variant="Online", incarnation=1),
        tier="router",
    )

    assert i1.call(".proc_instance_info", "storage_1_1") == i1_info
    assert i2.call(".proc_instance_info", "storage_1_1") == i1_info

    assert i1.call(".proc_instance_info", "router_1_1") == i2_info
    assert i2.call(".proc_instance_info", "router_1_1") == i2_info

    with pytest.raises(TarantoolError) as e:
        i1.call(".proc_instance_info", "i3")
    assert e.value.args[:2] == (
        ErrorCode.NoSuchInstance,
        'instance with name "i3" not found',
    )

    # See https://git.picodata.io/picodata/picodata/picodata/-/issues/390
    # Instances of the same replicaset should have the same replicaset_uuid
    i3 = cluster.add_instance(tier="storage")
    i3_info = i3.call(".proc_instance_info")
    assert i3_info["name"] == "storage_1_2"
    assert i3_info["replicaset_name"] == "storage_1"
    assert i3_info["replicaset_uuid"] == i1_info["replicaset_uuid"]


def test_proc_get_vshard_config(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        storage:
            replication_factor: 1
        router:
            replication_factor: 3
"""
    )

    storage_instance = cluster.add_instance(tier="storage")
    router_instance_1 = cluster.add_instance(tier="router")
    router_instance_2 = cluster.add_instance(tier="router")

    storage_sharding = {
        f"{storage_instance.replicaset_uuid()}": {
            "replicas": {
                f"{storage_instance.uuid()}": {
                    "master": True,
                    "name": "storage_1_1",
                    "uri": f"pico_service@{storage_instance.host}:{storage_instance.port}",
                }
            },
            "weight": 1.0,
        }
    }

    router_sharding = {
        f"{router_instance_1.replicaset_uuid()}": {
            "replicas": {
                f"{router_instance_1.uuid()}": {
                    "master": True,
                    "name": "router_1_1",
                    "uri": f"pico_service@{router_instance_1.host}:{router_instance_1.port}",
                },
                f"{router_instance_2.uuid()}": {
                    "master": False,
                    "name": "router_1_2",
                    "uri": f"pico_service@{router_instance_2.host}:{router_instance_2.port}",
                },
            },
            "weight": 0.0,
        }
    }

    space_bucket_id = storage_instance.eval("return box.space._bucket.id")
    total_bucket_count = 3000

    storage_vshard_config_explicit = storage_instance.call(
        ".proc_get_vshard_config", "storage"
    )
    assert storage_vshard_config_explicit == dict(
        discovery_mode="on",
        sharding=storage_sharding,
        space_bucket_id=space_bucket_id,
        bucket_count=total_bucket_count,
    )

    storage_vshard_config_implicit = storage_instance.call(
        ".proc_get_vshard_config", None
    )
    assert storage_vshard_config_explicit == storage_vshard_config_implicit

    router_vshard_config_explicit = router_instance_1.call(
        ".proc_get_vshard_config", "router"
    )
    assert router_vshard_config_explicit == dict(
        discovery_mode="on",
        sharding=router_sharding,
        space_bucket_id=space_bucket_id,
        bucket_count=total_bucket_count,
    )

    router_vshard_config_implicit = router_instance_1.call(
        ".proc_get_vshard_config", None
    )
    assert router_vshard_config_explicit == router_vshard_config_implicit

    assert router_instance_1.call(
        ".proc_get_vshard_config", "router"
    ) == storage_instance.call(".proc_get_vshard_config", "router")

    assert router_instance_1.call(
        ".proc_get_vshard_config", "storage"
    ) == storage_instance.call(".proc_get_vshard_config", "storage")

    with pytest.raises(TarantoolError, match='tier with name "default" not found'):
        router_instance_1.call(".proc_get_vshard_config", "default")


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
    slab_info = instance.call("box.slab.info")

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
            # This is a counter which increases each time governor successfully performs a step.
            # This value may change in the future if we add or remove some of those steps.
            governor_step_counter=6,
        ),
        http=dict(
            host=host,
            port=port,
        ),
        slab_info=slab_info,
        version_info=version_info,
    )


def test_file_shredding(cluster: Cluster, class_tmp_dir):
    tmp_path = class_tmp_dir
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

    def check_files_got_shredded():
        with open(os.path.join(tmp_path, "i1/00000000000000000000.xlog"), "rb") as xlog:
            xlog_after_shred = xlog.read(100)
        with open(os.path.join(tmp_path, "i1/00000000000000000000.snap"), "rb") as snap:
            snap_after_shred = snap.read(100)

        assert xlog_before_shred != xlog_after_shred
        assert snap_before_shred != snap_after_shred

    # There's currently no way to synchronously wait for the snapshot to be
    # generated (and shredded) so we do a retriable call. If this starts getting
    # flaky try increasing the timeout, although it's very sad if we need to
    # wait more than 5 seconds for the shredding to take place...
    Retriable(timeout=5, rps=5).call(check_files_got_shredded)


def test_pico_service_password_security_warning(cluster: Cluster):
    password_file = f"{cluster.instance_dir}/service-password.txt"
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


def test_replication_rpc_protection_from_old_governor(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True)
    i2 = cluster.add_instance(wait_online=True)

    injection_hit = log_crawler(
        i1, "ERROR INJECTION 'BLOCK_REPLICATION_RPC_ON_CLIENT': BLOCKING"
    )
    different_term_error = log_crawler(
        i1,
        "failed calling rpc::replication: server responded with error: "
        "box error #10003: operation request from different term",
    )
    i3_replication_configured = log_crawler(
        i2, "configured replication with instance, instance_name: default_3_1"
    )

    i1.call("pico._inject_error", "BLOCK_REPLICATION_RPC_ON_CLIENT", True)

    i3 = cluster.add_instance(wait_online=False)
    i3.start()

    # wait till governor starts to configure replication and gets to injected failure
    injection_hit.wait_matched()

    # bump term
    i2.promote_or_fail()

    # remove injected error
    i1.call("pico._inject_error", "BLOCK_REPLICATION_RPC_ON_CLIENT", False)

    # check i1 has expected error in the log
    different_term_error.wait_matched(timeout=2)

    # check i3 has replication configured by i2
    i3_replication_configured.wait_matched(timeout=2)


def test_replication_demote_protection_from_old_governor(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_name="r1", wait_online=True)
    i2 = cluster.add_instance(replicaset_name="r1", wait_online=True)

    assert not i1.eval("return box.info.ro"), "i1 should be master initially"
    assert i2.eval("return box.info.ro"), "i2 should be read-only initially"

    # enable an error to block the demotion of the master instance
    i1.call("pico._inject_error", "BLOCK_REPLICATION_DEMOTE", True)

    injection_hit = log_crawler(
        i1, "ERROR INJECTION 'BLOCK_REPLICATION_DEMOTE': BLOCKING"
    )

    term_error = log_crawler(
        i1,
        "failed demoting old master and synchronizing new master: server responded with error: "
        "box error #10003: operation request from different term",
    )

    old_step_counter = i1.governor_step_counter()

    # update the replicaset configuration to set i2 as the new target master
    i1.sql(
        "UPDATE _pico_replicaset SET target_master_name = ? WHERE name = 'r1'", i2.name
    )

    # wait for the error injection to block the demotion process
    injection_hit.wait_matched()

    # promote i2 to master. term is increased, new governor becomes active.
    # starting from this point the cluster should not accept actions from old governors
    i2.promote_or_fail()

    # remove the injected error that blocks the demotion
    i1.call("pico._inject_error", "BLOCK_REPLICATION_DEMOTE", False)

    term_error.wait_matched(timeout=2)

    # wait until governor performs all the necessary actions
    i2.wait_governor_status("idle", old_step_counter=old_step_counter)

    def check_replication():
        # check raft statuses
        i1.assert_raft_status("Follower")
        i2.assert_raft_status("Leader")

        # check that demote indeed changed the master in replicaset
        assert i1.eval("return box.info.ro"), "i1 should become read-only"
        assert not i2.eval("return box.info.ro"), "i2 should become master"

    Retriable(timeout=2).call(check_replication)


def test_stale_governor_replication_requests(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i2 = cluster.add_instance(wait_online=False)

    governor_message = "configuring replication"
    lc = log_crawler(i1, governor_message)

    cluster.wait_online()

    initial_term = i1.raft_term()

    # inject an error to cause a timeout during synchronization before promotion
    i1.call("pico._inject_error", "BLOCK_GOVERNOR_BEFORE_REPLICATION_CALL", True)

    # promote i2 to master
    i2.promote_or_fail()

    new_term = i2.raft_term()
    assert new_term > initial_term, "Term should increase after new leader election"

    # verify that i1 logs the replication configuration attempt
    lc.wait_matched(timeout=10)

    # remove the injected error that caused the synchronization timeout
    i1.call("pico._inject_error", "BLOCK_GOVERNOR_BEFORE_REPLICATION_CALL", False)

    def check_replication():
        i1.assert_raft_status("Follower")
        i2.assert_raft_status("Leader")

    Retriable(timeout=10).call(check_replication)
