import pytest
from typing import Literal
from conftest import Cluster, Instance, TarantoolError
from tarantool.error import NetworkError  # type: ignore


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
    v = 0

    # Initial state.
    for i in cluster.instances:
        assert i.call("box.space._pico_property:get", "global_schema_version")[1] == v
        assert i.call("box.space._user.index.name:get", user) is None

    #
    #
    # Create user.
    index = i1.call("pico.create_user", user, password)
    cluster.raft_wait_index(index)
    v += 1

    # Schema was updated.
    for i in cluster.instances:
        assert i.call("box.space._pico_property:get", "global_schema_version")[1] == v
        assert i.call("box.space._user.index.name:get", user) is not None

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

    # TODO: remove this
    user_id = i1.eval("return box.space._user.index.name:get(...).id", user)

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
    index = i1.call("pico.change_password", user, new_password)
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
    index = i1.call("pico.drop_user", user)

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


# TODO: test acl get denied when there's an unfinished ddl
