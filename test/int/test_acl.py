import pytest
from typing import Literal
from conftest import Cluster, Instance, TarantoolError
from tarantool.error import NetworkError


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
            auth=dict(method="chap-sha1", data=digest),
            schema_version=schema_version,
        ),
    )
    # TODO: use pico.cas
    return instance.call("pico.raft_propose", op, timeout=timeout)


def propose_change_password(
    instance: Instance,
    user_id: int,
    password: str,
    timeout: int = 3,
) -> int:
    digest = instance.call("box.internal.prepare_auth", "chap-sha1", password)
    schema_version = instance.next_schema_version()
    op = dict(
        kind="acl",
        op_kind="change_auth",
        user_id=user_id,
        auth=dict(method="chap-sha1", data=digest),
        schema_version=schema_version,
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


def propose_update_privilege(
    instance: Instance,
    user_id: int,
    privilege: str,
    object_type: str,
    object_name: str,
    what: Literal["grant", "revoke"],
    timeout: int = 3,
) -> int:
    schema_version = instance.next_schema_version()
    op = dict(
        kind="acl",
        op_kind=f"{what}_privilege",
        priv_def=dict(
            user_id=user_id,
            privilege=privilege,
            object_type=object_type,
            object_name=object_name,
            schema_version=schema_version,
        ),
    )
    # TODO: use pico.cas
    return instance.call("pico.raft_propose", op, timeout=timeout)


def test_acl_basic(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)

    user = "Bobby"
    password = "s3cr3t"
    user_id = 314
    v = 0

    # Initial state.
    for i in cluster.instances:
        assert i.call("box.space._pico_property:get", "global_schema_version")[1] == v
        assert i.call("box.space._user:get", user_id) is None
        assert (
            i.eval(
                """return box.execute([[
                select count(*) from "_priv" where "grantee" = ?
            ]], {...}).rows""",
                user_id,
            )
            == [[0]]
        )

    #
    #
    # Create user.
    index = propose_create_user(i1, id=user_id, name=user, password=password)
    cluster.raft_wait_index(index)
    v += 1

    # Schema was updated.
    for i in cluster.instances:
        assert i.call("box.space._pico_property:get", "global_schema_version")[1] == v
        assert i.call("box.space._user:get", user_id) is not None
        # Default privileges have been granted.
        assert (
            i.eval(
                """return box.execute([[
                select count(*) from "_priv" where "grantee" = ?
            ]], {...}).rows""",
                user_id,
            )
            != [[0]]
        )

    #
    #
    # Create a space to have something to grant privileges to.
    cluster.create_space(
        dict(
            name="money",
            format=[
                dict(name="owner", type="string", is_nullable=False),
                dict(name="amount", type="number", is_nullable=False),
            ],
            primary_key=["owner"],
            distribution="sharded",
            sharding_key=["owner"],
            sharding_fn="murmur3",
        )
    )
    v += 1

    #
    #
    # Grant some privileges.
    index = propose_update_privilege(
        i1,
        user_id=user_id,
        # Doing anything via remote function execution requires execute access
        # to the "universe"
        privilege="execute",
        object_type="universe",
        object_name="",
        what="grant",
    )
    cluster.raft_wait_index(index)
    v += 1

    index = propose_update_privilege(
        i1,
        user_id=user_id,
        privilege="read",
        object_type="space",
        object_name="money",
        what="grant",
    )
    cluster.raft_wait_index(index)
    v += 1

    # Schema was updated once more.
    for i in cluster.instances:
        assert i.call("box.space._pico_property:get", "global_schema_version")[1] == v
        assert i.call("box.space._user:get", user_id) is not None
        assert (
            i.eval(
                """return box.execute([[
                select count(*) from "_priv" where "grantee" = ?
            ]], {...}).rows""",
                user_id,
            )
            != [[0]]
        )

    # Find replicasets masters.
    # TODO: use picodata's routing facilities.
    masters = [i for i in cluster.instances if not i.eval("return box.info.ro")]

    # Make sure we actually do call to somebody.
    assert len(masters) != 0

    # Try writing into space on behalf of the user.
    for i in masters:
        dummy_bucket_id = 69
        with pytest.raises(
            TarantoolError,
            match="Write access to space 'money' is denied for user 'Bobby'",
        ):
            i.call(
                "box.space.money:insert",
                [user, dummy_bucket_id, 1_000_000],
                user=user,
                password=password,
            )

    # Try reading from space on behalf of the user.
    for i in cluster.instances:
        assert i.call("box.space.money:select", user=user, password=password) == []

    #
    #
    # Revoke the privilege.
    index = propose_update_privilege(
        i1,
        user_id=user_id,
        privilege="read",
        object_type="space",
        object_name="money",
        what="revoke",
    )
    cluster.raft_wait_index(index)
    v += 1

    # Try reading from space on behalf of the user again.
    for i in cluster.instances:
        with pytest.raises(
            TarantoolError,
            match="Read access to space 'money' is denied for user 'Bobby'",
        ):
            assert i.call("box.space.money:select", user=user, password=password) == []

    #
    #
    # Change user's password.
    old_password = password
    new_password = "$3kr3T"
    index = propose_change_password(
        i1,
        user_id=user_id,
        password=new_password,
    )
    cluster.raft_wait_index(index)
    v += 1

    # Old password doesn't work.
    for i in cluster.instances:
        with pytest.raises(
            NetworkError, match="User not found or supplied credentials are invalid"
        ):
            i.eval("return 1", user=user, password=old_password)

    # New password works.
    for i in cluster.instances:
        assert i.eval("return 1", user=user, password=new_password) == 1

    #
    #
    # Drop user.
    index = propose_drop_user(i1, id=user_id)

    for i in cluster.instances:
        i.raft_wait_index(index)

    # Schema was updated yet again.
    v += 1
    for i in cluster.instances:
        assert i.call("box.space._pico_property:get", "global_schema_version")[1] == v
        assert i.call("box.space._user:get", user_id) is None
        assert (
            i.eval(
                """return box.execute([[
                select count(*) from "_priv" where "grantee" = ?
            ]], {...}).rows""",
                user_id,
            )
            == [[0]]
        )

    # User was actually dropped.
    for i in cluster.instances:
        with pytest.raises(
            NetworkError, match="User not found or supplied credentials are invalid"
        ):
            i.eval("return 1", user=user, password=new_password)
