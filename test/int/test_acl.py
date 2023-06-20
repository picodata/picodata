import pytest
from conftest import Cluster, TarantoolError
from tarantool.error import NetworkError  # type: ignore


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
    # Doing anything via remote function execution requires execute access
    # to the "universe"
    index = i1.call("pico.grant_privilege", user, "execute", "universe")
    cluster.raft_wait_index(index)
    v += 1

    index = i1.call("pico.grant_privilege", user, "read", "space", "money")
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
    index = i1.call("pico.revoke_privilege", user, "read", "space", "money")
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
