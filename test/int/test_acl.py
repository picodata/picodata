from conftest import Cluster, Instance


def propose_create_user(
    instance: Instance,
    id: int,
    name: str,
    password: str,
    timeout: int = 3,
) -> int:
    digest = instance.call("box.internal.prepare_auth", "chap-sha1", password)
    schema_version = instance.next_schema_version()
    op = dict(
        kind="acl",
        op_kind="create_user",
        user_def=dict(
            id=id,
            name=name,
            auth=dict(type="chap-sha1", digest=digest),
            schema_version=schema_version,
        ),
    )
    # TODO: use pico.cas
    return instance.call("pico.raft_propose", op, timeout=timeout)


def propose_drop_user(
    instance: Instance,
    id: int,
    timeout: int = 3,
) -> int:
    schema_version = instance.next_schema_version()
    op = dict(
        kind="acl",
        op_kind="drop_user",
        user_id=id,
        schema_version=schema_version,
    )
    # TODO: use pico.cas
    return instance.call("pico.raft_propose", op, timeout=timeout)


def test_acl_basic(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)

    username = "Bobby"

    for i in cluster.instances:
        assert i.call("box.space._pico_property:get", "global_schema_version")[1] == 0
        assert i.call("box.space._user.index.name:get", username) is None

    user_id = 314
    index = propose_create_user(i1, id=user_id, name=username, password="s3cr3t")

    for i in cluster.instances:
        i.raft_wait_index(index)

    for i in cluster.instances:
        assert i.call("box.space._pico_property:get", "global_schema_version")[1] == 1
        assert i.call("box.space._user.index.name:get", username) is not None

    index = propose_drop_user(i1, id=user_id)

    for i in cluster.instances:
        i.raft_wait_index(index)

    for i in cluster.instances:
        assert i.call("box.space._pico_property:get", "global_schema_version")[1] == 2
        assert i.call("box.space._user.index.name:get", username) is None
