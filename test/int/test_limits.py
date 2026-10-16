import pytest
from conftest import TIMEOUT_SCALE, Cluster, Instance, TarantoolError


# BOX_USER_MAX in tarantool. Roles do not count towards it.
MAX_USERS = 512
# guest, admin and pico_service.
BUILTIN_USERS = 3
# The IDs from 0 to PICO_SERVICE_ID (32) are kept for system users and
# roles. 6 of them are in use: guest, admin, public, replication, super
# and pico_service. A token is kept for each of the free IDs.
RESERVED_IDS = 33 - 6
# The limit on the users that are not system users.
MAX_OTHER_USERS = MAX_USERS - BUILTIN_USERS - RESERVED_IDS
LIMIT_ERROR = f"limit on the total number of users has been reached: {MAX_USERS}"
# One CREATE USER takes about 0.3 s, and a test creates about 480 users.
FILL_TIMEOUT = int(600 * TIMEOUT_SCALE)


def create_user(instance: Instance, name: str, extra: str = "") -> dict:
    password = "Passw0rd"
    return instance.sql(f"CREATE USER {name} WITH PASSWORD '{password}' USING md5 {extra}")


def fill_users(instance: Instance, count: int):
    for i in range(count):
        assert create_user(instance, f"user{i}")["row_count"] == 1


def tarantool_users(instance: Instance) -> list[str]:
    return instance.eval(
        """
        local names = {}
        for _, t in box.space._user:pairs() do
            if t.type == 'user' then table.insert(names, t.name) end
        end
        table.sort(names)
        return names
        """
    )


def assert_same_users(cluster: Cluster, count: int):
    expected = None
    for instance in cluster.instances:
        instance.check_process_alive()
        users = tarantool_users(instance)
        assert len(users) == count
        if expected is None:
            expected = users
        assert users == expected


@pytest.mark.timeout(FILL_TIMEOUT)
def test_user_limit(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    fill_users(i1, MAX_OTHER_USERS)
    with pytest.raises(TarantoolError, match=LIMIT_ERROR):
        create_user(i1, "user_over")

    # The user exists, so there is nothing to create and the limit
    # does not apply.
    acl = create_user(i1, "IF NOT EXISTS user0")
    assert acl["row_count"] == 0
    with pytest.raises(TarantoolError, match=LIMIT_ERROR):
        create_user(i1, "IF NOT EXISTS user_over")

    # Roles do not count towards the limit.
    for i in range(10):
        assert i1.sql(f"CREATE ROLE role{i}")["row_count"] == 1

    # An upgrade can add a system role. Make the next grantee ID a
    # reserved one to do the same.
    i1.call("pico._inject_error", "NEXT_GRANTEE_ID", "4")
    assert i1.sql("CREATE ROLE sys_role")["row_count"] == 1
    i1.call("pico._inject_error", "NEXT_GRANTEE_ID", False)

    # The role takes a reserved ID, so one less token is kept.
    assert create_user(i1, "user_over")["row_count"] == 1
    with pytest.raises(TarantoolError, match=LIMIT_ERROR):
        create_user(i2, "user_over2")

    # A system user can still be created at the limit.
    i1.call("pico._inject_error", "NEXT_GRANTEE_ID", "5")
    assert create_user(i1, "sys_user")["row_count"] == 1
    i1.call("pico._inject_error", "NEXT_GRANTEE_ID", False)

    # Unlike the role, the system user takes one of the kept tokens,
    # so there is still no room for a regular user.
    with pytest.raises(TarantoolError, match=LIMIT_ERROR):
        create_user(i1, "user_over2")

    # IDs 4 and 5 are not free now, and 2 more users took their tokens.
    assert_same_users(cluster, MAX_USERS - RESERVED_IDS + 2)


@pytest.mark.timeout(FILL_TIMEOUT)
def test_user_limit_with_stale_raft_leader(cluster: Cluster):
    # Two replicasets of two instances.
    instances = cluster.deploy(instance_count=4, init_replication_factor=2)
    voters = instances[0].eval("return box.space._raft_state:get('voters').value")
    rs = {i.name: i.instance_info()["replicaset_name"] for i in instances}
    # i2 is a read-only replica and a voter, i1 is its master,
    # and i3 is the master of the other replicaset.
    i2 = next(i for i in instances if i.raft_id in voters and i.eval("return box.info.ro"))
    i1 = next(i for i in instances if i is not i2 and rs[i.name] == rs[i2.name])
    i3 = next(i for i in instances if rs[i.name] != rs[i2.name] and not i.eval("return box.info.ro"))

    fill_users(i1, MAX_OTHER_USERS - 1)

    # Make i2 the raft leader, and stop the replication from its
    # master. Then i2 cannot apply an ACL entry until the
    # master's DDL is replicated to it.
    cluster.leader().raft_transfer_leadership(i2.raft_id)
    replication = [r["uri"] for r in i2.eval("return box.cfg.replication")]
    i2.eval("box.cfg{replication = {}}")

    # This takes the last free slot. i1 and i3 apply it, i2 does not.
    acl = create_user(i3, "user_last", "WAIT APPLIED LOCALLY")
    assert acl["row_count"] == 1
    i1.raft_wait_index(i3.raft_get_index())

    # i2 still has a free slot, but the cluster has none.
    with pytest.raises(TarantoolError, match=LIMIT_ERROR):
        create_user(i1, "user_over", "WAIT APPLIED LOCALLY")

    i2.eval("box.cfg{replication = {...}}", *replication)
    i2.raft_wait_index(i1.raft_get_index())
    assert_same_users(cluster, MAX_USERS - RESERVED_IDS)
