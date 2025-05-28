import pytest
from typing import TypedDict

from conftest import Cluster, TarantoolError


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


def test_access_global_table(cluster: Cluster):
    (i1,) = cluster.deploy(instance_count=1)
    i1.sql("""create user "alice" with password 'T0psecret' using chap-sha1""")
    i1.sql("""create user "bob" with password 'T0tallysecret' using chap-sha1""")

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
    i1.sql("""grant create table to "alice" """, sudo=True)
    assert i1.sql(create_table_friends_of_peppa, **as_alice) == {"row_count": 1}

    # Alice can write it
    _ = cluster.cas("insert", "friends_of_peppa", ["Rebecca", "Rabbit"], user="alice")
    _ = cluster.cas("insert", "friends_of_peppa", ["Zoe", "Zebra"], user="alice")
    ret, _ = cluster.cas("insert", "friends_of_peppa", ["Suzy", "Sheep"], user="alice")
    i1.raft_wait_index(ret, _3_SEC)

    # Alice can read it
    assert i1.sql("""select * from "friends_of_peppa" """, **as_alice) == [
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
    assert i1.sql("""grant read on table "friends_of_peppa" to "bob";""", **as_alice) == {"row_count": 1}

    # Now bob can read it
    assert i1.sql("""select * from "friends_of_peppa" where "name" = 'Zoe';""", **as_bob) == [["Zoe", "Zebra"]]

    # But Bob still can't write it
    with pytest.raises(TarantoolError) as e:
        cluster.cas("delete", "friends_of_peppa", key=["Rebecca"], user="bob")
    assert e.value.args[:2] == (
        "ER_ACCESS_DENIED",
        "Write access to space 'friends_of_peppa' is denied for user 'bob'",
    )


def test_access_sharded_table(cluster: Cluster):
    (i1,) = cluster.deploy(instance_count=1)
    i1.sql("""create user "alice" with password 'T0psecret' using chap-sha1""")
    i1.sql("""create user "bob" with password 'T0tallysecret' using chap-sha1""")

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
    assert i1.sql("""grant create table to "alice";""", sudo=True) == {"row_count": 1}
    assert i1.sql(create_table_wonderland, **as_alice) == {"row_count": 1}

    # Alice can write it
    i1.sql("""insert into "wonderland" values ('dragon', 13)""", **as_alice)
    i1.sql("""insert into "wonderland" values ('unicorn', 4)""", **as_alice)
    i1.sql("""insert into "wonderland" values ('goblin', null)""", **as_alice)

    # Alice can read it
    assert i1.sql("""select * from "wonderland";""", **as_alice) == [
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
    assert i1.sql("""grant read on table "wonderland" to "bob";""", **as_alice) == {"row_count": 1}

    # Now bob can read it
    assert i1.sql("""select * from "wonderland" where "creature" = 'unicorn';""", **as_bob) == [["unicorn", 4]]

    # But Bob still can't write it
    with pytest.raises(
        TarantoolError,
        match="Write access to space 'wonderland' is denied for user 'bob'",
    ):
        i1.sql("""insert into "wonderland" values ('snail', 1)""", **as_bob)
