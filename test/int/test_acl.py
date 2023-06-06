from conftest import Cluster, Instance


def propose_create_user(
    instance: Instance,
    id: int,
    name: str,
    password: str,
    wait_index: bool = True,
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


def test_acl_create_user_basic(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)

    username = "Bobby"
    index = propose_create_user(i1, id=314, name=username, password="s3cr3t")

    for i in cluster.instances:
        i.raft_wait_index(index)

    for i in cluster.instances:
        assert i.call("box.space._user.index.name:get", username) is not None
