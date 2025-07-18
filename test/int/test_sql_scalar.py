import pathlib
import os

import git
import pytest

from conftest import (
    Cluster,
    Instance,
    TarantoolError,
)


TEST_USER_NAME = "somebody"
TEST_USER_PASS = "T0psecret"
TEST_USER_AUTH = "chap-sha1"


def create_test_data(instance: Instance):
    ddl = instance.sql(
        """
        CREATE TABLE warehouse (
            id INTEGER NOT NULL,
            item TEXT NOT NULL,
            type TEXT NOT NULL,
            PRIMARY KEY (id)
        )
        USING memtx DISTRIBUTED BY (id)
        OPTION (TIMEOUT = 3.0);
        """
    )
    assert ddl["row_count"] == 1

    ddl = instance.sql(
        """
        CREATE TABLE items (
            id INTEGER NOT NULL,
            name TEXT NOT NULL,
            stock INTEGER,
            PRIMARY KEY (id)
        )
        USING memtx DISTRIBUTED BY (id)
        OPTION (TIMEOUT = 3.0);
        """
    )
    assert ddl["row_count"] == 1

    result = instance.sql(
        """
        INSERT INTO warehouse VALUES
            (1, 'bricks', 'heavy'),
            (2, 'bars', 'light'),
            (3, 'blocks', 'heavy'),
            (4, 'piles', 'light'),
            (5, 'panels', 'light');
        """
    )
    assert result["row_count"] == 5


def test_scalar_function_instance_uuid(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    i1_uuid = i1.uuid()
    i2_uuid = i2.uuid()

    cluster.wait_until_buckets_balanced()

    create_test_data(i1)
    i1.create_user(
        with_name=TEST_USER_NAME,
        with_password=TEST_USER_PASS,
        with_auth=TEST_USER_AUTH,
    )

    # instance_uuid avaliable to all users
    query = i1.sql(
        """
        SELECT pico_instance_uuid()
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[i1_uuid]]

    # checking admin
    query = i1.sql(
        """
        SELECT pico_instance_uuid()
        """,
    )
    assert query == [[i1_uuid]]

    # function name format

    # uppercased
    query = i1.sql(
        """
        SELECT PICO_INSTANCE_UUID()
        """,
    )
    assert query == [[i1_uuid]]

    # with quotes, lowercased
    query = i1.sql(
        """
        SELECT "pico_instance_uuid"()
        """,
    )
    assert query == [[i1_uuid]]

    # with quotes, uppercased
    with pytest.raises(TarantoolError, match="SQL function PICO_INSTANCE_UUID not found"):
        i1.sql(
            """
            SELECT "PICO_INSTANCE_UUID"()
            """
        )

    # volatile scalar function call isn't allowed in filters - according to query.pest there is
    # `case filter`, `delete filter`, `window function filter`, `update filter`, `selection filter`,
    # `join filter`
    # and in order by, group by

    # selection filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        i1.sql("SELECT * FROM warehouse WHERE item = pico_instance_uuid()")

    # hacky selection filter
    query = i1.sql(
        """
        SELECT uuid FROM _pico_instance WHERE uuid IN (SELECT pico_instance_uuid())
        """
    )
    assert query == [[i1_uuid]]

    # case filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        i1.sql(
            """
            SELECT
              CASE type
                WHEN pico_instance_uuid()
                THEN '1'
              END
            FROM warehouse;
            """
        )

    # update filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and pico_instance_uuid() types doesn't match
        i1.sql(
            """
            UPDATE warehouse SET item = 'chunks', TYPE = 'light' WHERE id = pico_instance_uuid();
            """
        )

    # delete filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and pico_instance_uuid() types doesn't match
        i1.sql(
            """
            DELETE FROM warehouse WHERE id = pico_instance_uuid();
            """
        )

    # window filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and pico_instance_uuid() types doesn't match
        i1.sql(
            """
           SELECT
               id,
               item,
               type,
               COUNT(*) FILTER (WHERE item = pico_instance_uuid()) OVER (PARTITION BY type) AS box_count_per_type
           FROM warehouse
           """
        )

    # join filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and pico_instance_uuid() types doesn't match
        i1.sql(
            """
           SELECT *

           FROM warehouse
           JOIN items
                ON warehouse.item = pico_instance_uuid()
           """
        )

    # order by
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and pico_instance_uuid() types doesn't match
        i1.sql(
            """
            SELECT * FROM items
            ORDER BY pico_instance_uuid()
            """
        )

    # group by
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and pico_instance_uuid() types doesn't match
        i1.sql(
            """
            SELECT type, COUNT(*) FROM warehouse
            WHERE id < 5
            GROUP BY pico_instance_uuid();
            """
        )

    # misc

    # attempt to use distinct in argument list
    with pytest.raises(TarantoolError, match='"distinct" is not allowed inside VOLATILE function call'):
        i1.sql(
            """
            SELECT instance_uuid(distinct );
            """
        )

    # in case of distributed plan, function call return different
    # result on different instances
    query = i1.sql(
        """
        SELECT DISTINCT pico_instance_uuid() FROM warehouse
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert sorted(query) == sorted([[i1_uuid], [i2_uuid]])

    # local execution
    query = i1.sql(
        """
        SELECT pico_instance_uuid() FROM (VALUES(1, 2))
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[i1_uuid]]

    query = i2.sql(
        """
        SELECT pico_instance_uuid() FROM (VALUES(1, 2))
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[i2_uuid]]

    # same with global tables
    i1.sql(
        """
        CREATE TABLE "global" ("id" unsigned not null, primary key("id"))
        USING MEMTX
        DISTRIBUTED GLOBALLY
        WAIT APPLIED GLOBALLY
        """
    )

    # empty tables
    query = i1.sql(
        """
        SELECT pico_instance_uuid() FROM global
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == []

    query = i2.sql(
        """
        SELECT pico_instance_uuid() FROM global
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == []

    query = i1.sql(
        """
        INSERT INTO global VALUES(1)
        """
    )

    # same with global tables
    query = i1.retriable_sql(
        """
        SELECT pico_instance_uuid() FROM global
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[i1_uuid]]

    query = i2.retriable_sql(
        """
        SELECT pico_instance_uuid() FROM global
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[i2_uuid]]


def test_scalar_function_raft_leader_uuid(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    raft_leader_uuid = i1.raft_leader_uuid()

    cluster.wait_until_buckets_balanced()

    create_test_data(i1)
    i1.create_user(
        with_name=TEST_USER_NAME,
        with_password=TEST_USER_PASS,
        with_auth=TEST_USER_AUTH,
    )

    # pico_raft_leader_uuid avaliable only to admins
    query = i1.sql(
        """
        SELECT pico_raft_leader_uuid()
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[raft_leader_uuid]]

    # checking admin
    query = i1.sql(
        """
        SELECT pico_raft_leader_uuid()
        """,
    )
    assert query == [[raft_leader_uuid]]

    # function name format

    # uppercased
    query = i1.sql(
        """
        SELECT PICO_RAFT_LEADER_UUID()
        """,
    )
    assert query == [[raft_leader_uuid]]

    # with quotes, lowercased
    query = i1.sql(
        """
        SELECT "pico_raft_leader_uuid"()
        """,
    )
    assert query == [[raft_leader_uuid]]

    # with quotes, uppercased
    with pytest.raises(TarantoolError, match="SQL function PICO_RAFT_LEADER_UUID not found"):
        i1.sql(
            """
            SELECT "PICO_RAFT_LEADER_UUID"()
            """
        )

    # volatile scalar function call isn't allowed in filters - according to query.pest there is
    # `case filter`, `delete filter`, `window function filter`, `update filter`, `selection filter`,
    # `join filter`
    # and in order by, group by

    # selection filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        i1.sql("SELECT * FROM warehouse WHERE item = pico_raft_leader_uuid()")

    # hacky selection filter
    query = i1.sql(
        """
        SELECT uuid FROM _pico_instance WHERE uuid IN (SELECT pico_raft_leader_uuid())
        """
    )
    assert query == [[raft_leader_uuid]]

    # case filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        i1.sql(
            """
            SELECT
              CASE type
                WHEN pico_raft_leader_uuid()
                THEN '1'
              END
            FROM warehouse;
            """
        )

    # update filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and pico_raft_leader_uuid() types doesn't match
        i1.sql(
            """
            UPDATE warehouse SET item = 'chunks', TYPE = 'light' WHERE id = pico_raft_leader_uuid();
            """
        )

    # delete filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and pico_raft_leader_uuid() types doesn't match
        i1.sql(
            """
            DELETE FROM warehouse WHERE id = pico_raft_leader_uuid();
            """
        )

    # window filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and pico_raft_leader_uuid() types doesn't match
        i1.sql(
            """
           SELECT
               id,
               item,
               type,
               COUNT(*) FILTER (WHERE item = pico_raft_leader_uuid()) OVER (PARTITION BY type) AS box_count_per_type
           FROM warehouse
           """
        )

    # join filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and pico_raft_leader_uuid() types doesn't match
        i1.sql(
            """
           SELECT *

           FROM warehouse
           JOIN items
                ON warehouse.item = pico_raft_leader_uuid()
           """
        )

    # order by
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and pico_raft_leader_uuid() types doesn't match
        i1.sql(
            """
            SELECT * FROM items
            ORDER BY pico_raft_leader_uuid()
            """
        )

    # group by
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and pico_raft_leader_uuid() types doesn't match
        i1.sql(
            """
            SELECT type, COUNT(*) FROM warehouse
            WHERE id < 5
            GROUP BY pico_raft_leader_uuid();
            """
        )

    # misc

    # attempt to use distinct in argument list
    with pytest.raises(TarantoolError, match='"distinct" is not allowed inside VOLATILE function call'):
        i1.sql(
            """
            SELECT pico_raft_leader_uuid(distinct );
            """
        )

    # in case of distributed plan, function call return different
    # result on different instances
    query = i1.sql(
        """
        SELECT DISTINCT pico_raft_leader_uuid() FROM warehouse
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[raft_leader_uuid]]

    # local execution
    query = i1.sql(
        """
        SELECT pico_raft_leader_uuid() FROM (VALUES(1, 2))
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[raft_leader_uuid]]

    query = i2.sql(
        """
        SELECT pico_raft_leader_uuid() FROM (VALUES(1, 2))
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[raft_leader_uuid]]

    # same with global tables
    i1.sql(
        """
        CREATE TABLE "global" ("id" unsigned not null, primary key("id"))
        USING MEMTX
        DISTRIBUTED GLOBALLY
        WAIT APPLIED GLOBALLY
        """
    )

    # empty tables
    query = i1.sql(
        """
        SELECT pico_raft_leader_uuid() FROM global
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == []

    query = i2.sql(
        """
        SELECT pico_raft_leader_uuid() FROM global
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == []

    query = i1.sql(
        """
        INSERT INTO global VALUES(1)
        """
    )

    # same with global tables
    query = i1.retriable_sql(
        """
        SELECT pico_raft_leader_uuid() FROM global
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[raft_leader_uuid]]

    query = i2.retriable_sql(
        """
        SELECT pico_raft_leader_uuid() FROM global
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[raft_leader_uuid]]


def test_scalar_function_raft_leader_id(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    raft_leader_id = i1.raft_leader_id()

    cluster.wait_until_buckets_balanced()

    create_test_data(i1)
    i1.create_user(
        with_name=TEST_USER_NAME,
        with_password=TEST_USER_PASS,
        with_auth=TEST_USER_AUTH,
    )

    # pico_raft_leader_id avaliable only to admins
    query = i1.sql(
        """
        SELECT pico_raft_leader_id()
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[raft_leader_id]]

    # checking admin
    query = i1.sql(
        """
        SELECT pico_raft_leader_id()
        """,
    )
    assert query == [[raft_leader_id]]

    # function name format

    # uppercased
    query = i1.sql(
        """
        SELECT PICO_RAFT_LEADER_ID()
        """,
    )
    assert query == [[raft_leader_id]]

    # with quotes, lowercased
    query = i1.sql(
        """
        SELECT "pico_raft_leader_id"()
        """,
    )
    assert query == [[raft_leader_id]]

    # with quotes, uppercased
    with pytest.raises(TarantoolError, match="SQL function PICO_RAFT_LEADER_ID not found"):
        i1.sql(
            """
            SELECT "PICO_RAFT_LEADER_ID"()
            """
        )

    # volatile scalar function call isn't allowed in filters - according to query.pest there is
    # `case filter`, `delete filter`, `window function filter`, `update filter`, `selection filter`,
    # `join filter`
    # and in order by, group by

    # selection filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        i1.sql("SELECT * FROM warehouse WHERE item = pico_raft_leader_id()")

    # hacky selection filter
    query = i1.sql(
        """
        SELECT raft_id FROM _pico_instance WHERE raft_id = (SELECT pico_raft_leader_id())
        """
    )
    assert query == [[raft_leader_id]]

    # case filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        i1.sql(
            """
            SELECT
              CASE type
                WHEN pico_raft_leader_id()
                THEN '1'
              END
            FROM warehouse;
            """
        )

    # update filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and pico_raft_leader_id() types doesn't match
        i1.sql(
            """
            UPDATE warehouse SET item = 'chunks', TYPE = 'light' WHERE id = pico_raft_leader_id();
            """
        )

    # delete filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and pico_raft_leader_id() types doesn't match
        i1.sql(
            """
            DELETE FROM warehouse WHERE id = pico_raft_leader_id();
            """
        )

    # window filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and pico_raft_leader_id() types doesn't match
        i1.sql(
            """
           SELECT
               id,
               item,
               type,
               COUNT(*) FILTER (WHERE item = pico_raft_leader_id()) OVER (PARTITION BY type) AS box_count_per_type
           FROM warehouse
           """
        )

    # join filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and pico_raft_leader_id() types doesn't match
        i1.sql(
            """
           SELECT *

           FROM warehouse
           JOIN items
                ON warehouse.item = pico_raft_leader_id()
           """
        )

    # order by
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and pico_raft_leader_id() types doesn't match
        i1.sql(
            """
            SELECT * FROM items
            ORDER BY pico_raft_leader_id()
            """
        )

    # group by
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        # no matter that id and pico_raft_leader_id() types doesn't match
        i1.sql(
            """
            SELECT type, COUNT(*) FROM warehouse
            WHERE id < 5
            GROUP BY pico_raft_leader_id();
            """
        )

    # misc

    # attempt to use distinct in argument list
    with pytest.raises(TarantoolError, match='"distinct" is not allowed inside VOLATILE function call'):
        i1.sql(
            """
            SELECT pico_raft_leader_id(distinct );
            """
        )

    # in case of distributed plan, function call return different
    # result on different instances
    query = i1.sql(
        """
        SELECT DISTINCT pico_raft_leader_id() FROM warehouse
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[raft_leader_id]]

    # local execution
    query = i1.sql(
        """
        SELECT pico_raft_leader_id() FROM (VALUES(1, 2))
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[raft_leader_id]]

    query = i2.sql(
        """
        SELECT pico_raft_leader_id() FROM (VALUES(1, 2))
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[raft_leader_id]]

    # same with global tables
    i1.sql(
        """
        CREATE TABLE "global" ("id" unsigned not null, primary key("id"))
        USING MEMTX
        DISTRIBUTED GLOBALLY
        WAIT APPLIED GLOBALLY
        """
    )

    # empty tables
    query = i1.sql(
        """
        SELECT pico_raft_leader_id() FROM global
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == []

    query = i2.sql(
        """
        SELECT pico_raft_leader_id() FROM global
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == []

    query = i1.sql(
        """
        INSERT INTO global VALUES(1)
        """
    )

    # same with global tables
    query = i1.retriable_sql(
        """
        SELECT pico_raft_leader_id() FROM global
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[raft_leader_id]]

    query = i2.retriable_sql(
        """
        SELECT pico_raft_leader_id() FROM global
        """,
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[raft_leader_id]]


def test_scalar_function_version(cluster: Cluster):
    current_path = pathlib.Path(os.getcwd())
    git_version = git.Git(current_path).describe()

    i1 = cluster.add_instance()
    i2 = cluster.add_instance()

    assert i1.sql("SELECT version()") == [[git_version]]
    assert i2.sql("SELECT VERSION()") == [[git_version]]
