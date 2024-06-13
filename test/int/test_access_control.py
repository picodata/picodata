import pytest
from typing import TypedDict

from conftest import Cluster, TarantoolError, ReturnError


class AsUser(TypedDict):
    user: str
    password: str


as_alice: AsUser = {
    "user": "alice",
    "password": "T0psecret",
}

as_bob: AsUser = {
    "user": "bob",
    "password": "T0tallysecret",
}

_3_SEC = 3
as_alice = dict(user="alice", password="T0psecret")
as_bob = dict(user="bob", password="T0tallysecret")


def test_access_global_table(cluster: Cluster):
    (i1,) = cluster.deploy(instance_count=1)
    i1.sql("""create user "alice" with password 'T0psecret'""")
    i1.sql("""create user "bob" with password 'T0tallysecret'""")

    create_table_friends_of_peppa = """
        create table "friends_of_peppa" (
            "name" string not null,
            "species" string not null,
            primary key ("name")
        ) using memtx distributed globally
        option (timeout = 3)
    """

    with pytest.raises(
        TarantoolError,
        match="Create access to space 'friends_of_peppa' is denied for user 'alice'",
    ):
        i1.sql(create_table_friends_of_peppa, **as_alice)

    # Alice can create a global table
    i1.sudo_sql("""grant create table to "alice" """)
    assert i1.sql(create_table_friends_of_peppa, **as_alice) == {"row_count": 1}

    # Alice can write it
    _ = cluster.cas("insert", "friends_of_peppa", ["Rebecca", "Rabbit"], **as_alice)
    _ = cluster.cas("insert", "friends_of_peppa", ["Zoe", "Zebra"], **as_alice)
    ret = cluster.cas("insert", "friends_of_peppa", ["Suzy", "Sheep"], **as_alice)
    i1.raft_wait_index(ret, _3_SEC)

    # Alice can read it
    assert i1.sql("""select * from "friends_of_peppa" """, **as_alice)["rows"] == [
        ["Rebecca", "Rabbit"],
        ["Suzy", "Sheep"],
        ["Zoe", "Zebra"],
    ]

    # Bob can't read it by default
    with pytest.raises(
        TarantoolError,
        match="Read access to space 'friends_of_peppa' is denied for user 'bob'",
    ):
        i1.sql("""select * from "friends_of_peppa" """, **as_bob)

    # Alice can grant a read privilege to Bob
    assert i1.sql(
        """grant read on table "friends_of_peppa" to "bob";""", **as_alice
    ) == {"row_count": 1}

    # Now bob can read it
    assert i1.sql(
        """select * from "friends_of_peppa" where "name" = 'Zoe';""", **as_bob
    )["rows"] == [["Zoe", "Zebra"]]

    # But Bob still can't write it
    with pytest.raises(
        ReturnError,
        match="Write access to space 'friends_of_peppa' is denied for user 'bob'",
    ):
        cluster.cas("delete", "friends_of_peppa", key=["Rebecca"], **as_bob)


def test_access_sharded_table(cluster: Cluster):
    (i1,) = cluster.deploy(instance_count=1)
    i1.sql("""create user "alice" with password 'T0psecret'""")
    i1.sql("""create user "bob" with password 'T0tallysecret'""")

    create_table_wonderland = """
        create table "wonderland" (
            "creature" text not null,
            "count" integer,
            primary key ("creature")
        ) using memtx distributed by ("creature")
        option (timeout = 3.0)
    """

    with pytest.raises(
        TarantoolError,
        match="Create access to space 'wonderland' is denied for user 'alice'",
    ):
        i1.sql(create_table_wonderland, **as_alice)

    # Alice can create a global table
    assert i1.sudo_sql("""grant create table to "alice";""") == {"row_count": 1}
    assert i1.sql(create_table_wonderland, **as_alice) == {"row_count": 1}

    # Alice can write it
    i1.sql("""insert into "wonderland" values ('dragon', 13)""", **as_alice)
    i1.sql("""insert into "wonderland" values ('unicorn', 4)""", **as_alice)
    i1.sql("""insert into "wonderland" values ('goblin', null)""", **as_alice)

    # Alice can read it
    assert i1.sql("""select * from "wonderland";""", **as_alice)["rows"] == [
        ["dragon", 13],
        ["goblin", None],
        ["unicorn", 4],
    ]

    # Bob can't read it by default
    with pytest.raises(
        TarantoolError,
        match="Read access to space 'wonderland' is denied for user 'bob'",
    ):
        i1.sql("""select * from "wonderland" """, **as_bob)

    # Alice can grant a read privilege to Bob
    assert i1.sql("""grant read on table "wonderland" to "bob";""", **as_alice) == {
        "row_count": 1
    }

    # Now bob can read it
    assert i1.sql(
        """select * from "wonderland" where "creature" = 'unicorn';""", **as_bob
    )["rows"] == [["unicorn", 4]]

    # But Bob still can't write it
    with pytest.raises(
        TarantoolError,
        match="Write access to space 'wonderland' is denied for user 'bob'",
    ):
        i1.sql("""insert into "wonderland" values ('snail', 1)""", **as_bob)
