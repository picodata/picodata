import pytest
from conftest import Cluster, TarantoolError


# The hard upper bound (32) for max users comes from tarantool BOX_USER_MAX
# 32 - sys users in picodata = 26
max_picodata_users = 26


def test_user_limit(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    password = "Passw0rd"

    for i in range(max_picodata_users):
        username = f"USER{i}"

        acl = i1.sql(
            f"""
            create user {username} with password '{password}'
            using md5 option (timeout = 3)
            """
        )
        assert acl["row_count"] == 1

    with pytest.raises(
        TarantoolError,
        match="a limit on the total number of users has been reached: 32",
    ):
        username = f"USER{max_picodata_users}"
        i1.sql(
            f"""
            create user {username} with password '{password}'
            using md5
            """
        )


def test_role_limit(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    for i in range(max_picodata_users):
        role = f"ROLE{i}"

        acl = i1.sql(f"create role {role}")
        assert acl["row_count"] == 1

    with pytest.raises(
        TarantoolError,
        match="a limit on the total number of users has been reached: 32",
    ):
        role = f"ROLE{max_picodata_users}"
        i1.sql(f"create role {role}")
