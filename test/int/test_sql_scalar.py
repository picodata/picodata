from typing import Any, Callable
from uuid import uuid4

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


def assert_common_volatile_function_behavior(
    i1: Instance,
    i2: Instance,
    func_name: str,
    expected_i1: str,
    expected_i2: str,
    *,
    hacky_selection_sql: str | None = None,
    allowed_for_non_admin: bool = True,
    distributed_values_equal: bool = False,
    requires_volatility_modifier: bool,
):
    volatility_modifier = "pico_instance_uuid()" if requires_volatility_modifier else ""

    # 1) access (non-admin and admin)
    if allowed_for_non_admin:
        query = i1.sql(f"SELECT {func_name}({volatility_modifier})", TEST_USER_NAME, TEST_USER_PASS)
        assert query == [[expected_i1]]

    query = i1.sql(f"SELECT {func_name}({volatility_modifier})")
    assert query == [[expected_i1]]

    # 2) name format
    # uppercased
    upper = func_name.upper()
    assert i1.sql(f"SELECT {upper}({volatility_modifier})") == [[expected_i1]]
    # with quotes, lowercased
    assert i1.sql(f'SELECT "{func_name}"({volatility_modifier})') == [[expected_i1]]
    # with quotes, uppercased — should fail
    with pytest.raises(TarantoolError, match=f"SQL function {upper} not found"):
        i1.sql(f'SELECT "{upper}"({volatility_modifier})')

    # 3) VOLATILE restrictions
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        i1.sql(f"SELECT * FROM warehouse WHERE item = {func_name}({volatility_modifier})")

    # optional selection using a subquery — helps assert usage under a SELECT
    if hacky_selection_sql is not None:
        query = i1.sql(hacky_selection_sql.format(function=func_name, modifier=volatility_modifier))
        assert query == [[expected_i1]]

    # case filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        i1.sql(
            f"""
            SELECT
              CASE type
                WHEN {func_name}({volatility_modifier})
                THEN '1'
              END
            FROM warehouse;
            """
        )

    # update filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        i1.sql(
            f"""
            UPDATE warehouse SET item = 'chunks', TYPE = 'light' WHERE id = {func_name}({volatility_modifier});
            """
        )

    # delete filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        i1.sql(
            f"""
            DELETE FROM warehouse WHERE id = {func_name}({volatility_modifier});
            """
        )

    # window filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        i1.sql(
            f"""
           SELECT
               id,
               item,
               type,
                COUNT(*) FILTER (WHERE item = {func_name}({volatility_modifier})) OVER (PARTITION BY type) AS box_count_per_type
           FROM warehouse
           """
        )

    # join filter
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        i1.sql(
            f"""
           SELECT *

           FROM warehouse
           JOIN items
                ON warehouse.item = {func_name}({volatility_modifier})
           """
        )

    # order by
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        i1.sql(
            f"""
            SELECT * FROM items
            ORDER BY {func_name}({volatility_modifier})
            """
        )

    # group by
    with pytest.raises(TarantoolError, match="volatile function is not allowed in filter clause not implemented"):
        i1.sql(
            f"""
            SELECT type, COUNT(*) FROM warehouse
            WHERE id < 5
            GROUP BY {func_name}({volatility_modifier});
            """
        )

    if not requires_volatility_modifier:
        # misc: DISTINCT inside args is forbidden
        with pytest.raises(TarantoolError, match='"distinct" is not allowed inside VOLATILE function call'):
            i1.sql(f"SELECT {func_name}(distinct );")

    # 4) distributed plan behavior
    if distributed_values_equal:
        query = i1.sql(
            "SELECT DISTINCT " + func_name + f"({volatility_modifier}) FROM warehouse", TEST_USER_NAME, TEST_USER_PASS
        )
        assert query == [[expected_i1]]
    else:
        query = i1.sql(
            "SELECT DISTINCT " + func_name + f"({volatility_modifier}) FROM warehouse", TEST_USER_NAME, TEST_USER_PASS
        )
        assert sorted(query) == sorted([[expected_i1], [expected_i2]])

    # 5) local execution
    q = "SELECT " + func_name + f"({volatility_modifier}) FROM (VALUES(1, 2))"
    assert i1.sql(q, TEST_USER_NAME, TEST_USER_PASS) == [[expected_i1]]
    assert i2.sql(q, TEST_USER_NAME, TEST_USER_PASS) == [[expected_i2]]

    # 6) global tables behavior
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
        f"SELECT {func_name}({volatility_modifier}) FROM global",
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == []

    query = i2.sql(
        f"SELECT {func_name}({volatility_modifier}) FROM global",
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == []

    i1.sql("INSERT INTO global VALUES(1)")

    # same with global tables
    query = i1.retriable_sql(
        f"SELECT {func_name}({volatility_modifier}) FROM global",
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[expected_i1]]

    query = i2.retriable_sql(
        f"SELECT {func_name}({volatility_modifier}) FROM global",
        TEST_USER_NAME,
        TEST_USER_PASS,
    )
    assert query == [[expected_i2]]

    if requires_volatility_modifier:
        first = i1.sql(f"SELECT {func_name}({volatility_modifier})")
        second = i2.sql(f"SELECT {func_name}({volatility_modifier})")

        not_equal = first != second
        both_none = first == [[None]] and second == [[None]]
        both_default = first == [["default"]] and second == [["default"]]
        assert not_equal or both_none or both_default

    if requires_volatility_modifier:
        existing_instance_uuid = i1.sql("SELECT instance_uuid()")[0][0]
        random_instance_uuid = uuid4()
        assert existing_instance_uuid != random_instance_uuid, (
            "exceptional chance occurred: a UUID identical to an existing one was generated"
        )

        request = i1.sql(f"SELECT {func_name}('{random_instance_uuid}')")
        assert request == [[None]], "selecting from non-existing instance uuid should return NULL"


@pytest.mark.parametrize(
    "func_name, expected_getter, hacky_selection_sql, allowed_for_non_admin, distributed_values_equal, requires_volatility_modifier",
    [
        pytest.param(
            "pico_instance_uuid",
            lambda i1, i2: (i1.uuid(), i2.uuid()),
            "SELECT uuid FROM _pico_instance WHERE uuid IN (SELECT {function}({modifier}))",
            True,
            False,
            False,
            marks=pytest.mark.flaky(reruns=3),
        ),
        pytest.param(
            "pico_raft_leader_uuid",
            lambda i1, i2: (i1.raft_leader_uuid(), i2.raft_leader_uuid()),
            "SELECT uuid FROM _pico_instance WHERE uuid IN (SELECT {function}({modifier}))",
            False,
            True,
            False,
        ),
        pytest.param(
            "pico_raft_leader_id",
            lambda i1, i2: (i1.raft_leader_id(), i2.raft_leader_id()),
            "SELECT raft_id FROM _pico_instance WHERE raft_id = (SELECT {function}({modifier}))",
            False,
            True,
            False,
        ),
        pytest.param(
            "pico_instance_name",
            lambda i1, i2: (i1.name, i2.name),
            "SELECT name FROM _pico_instance WHERE name IN (SELECT {function}({modifier}))",
            True,
            False,
            True,
            marks=pytest.mark.flaky(reruns=3),
        ),
        pytest.param(
            "pico_replicaset_name",
            lambda i1, i2: (i1.replicaset_name, i2.replicaset_name),
            "SELECT name FROM _pico_replicaset WHERE name IN (SELECT {function}({modifier}))",
            True,
            None,
            True,
            marks=pytest.mark.flaky(reruns=3),
        ),
        pytest.param(
            "pico_tier_name",
            lambda i1, i2: (i1.get_tier(), i2.get_tier()),
            "SELECT name FROM _pico_tier WHERE name IN (SELECT {function}({modifier}))",
            True,
            None,
            True,
            marks=pytest.mark.flaky(reruns=3),
        ),
        pytest.param(
            "pico_instance_dir",
            lambda i1, i2: (str(i1.instance_dir), str(i2.instance_dir)),
            None,
            True,
            None,
            True,
            marks=pytest.mark.flaky(reruns=3),
        ),
        pytest.param(
            "pico_config_file_path",
            lambda i1, i2: (i1.config_path or None, i2.config_path or None),
            None,
            True,
            None,
            True,
        ),
    ],
)
def test_scalar_function_common(
    cluster: Cluster,
    func_name: str,
    expected_getter: Callable[[Instance, Instance], tuple[Any, Any]],
    hacky_selection_sql: str | None,
    allowed_for_non_admin: bool,
    distributed_values_equal: bool | None,
    requires_volatility_modifier: bool,
):
    i1, i2 = cluster.deploy(instance_count=2)

    cluster.wait_until_buckets_balanced()

    create_test_data(i1)
    i1.create_user(
        with_name=TEST_USER_NAME,
        with_password=TEST_USER_PASS,
        with_auth=TEST_USER_AUTH,
    )

    expected_i1, expected_i2 = expected_getter(i1, i2)
    dist_equal = (expected_i1 == expected_i2) if distributed_values_equal is None else distributed_values_equal

    assert_common_volatile_function_behavior(
        i1,
        i2,
        func_name,
        expected_i1,
        expected_i2,
        hacky_selection_sql=hacky_selection_sql,
        allowed_for_non_admin=allowed_for_non_admin,
        distributed_values_equal=dist_equal,
        requires_volatility_modifier=requires_volatility_modifier,
    )


def test_scalar_function_version(cluster: Cluster):
    current_path = pathlib.Path(os.getcwd())
    git_version = git.Git(current_path).describe()

    i1 = cluster.add_instance()
    i2 = cluster.add_instance()

    assert i1.sql("SELECT version()") == [[git_version]]
    assert i2.sql("SELECT VERSION()") == [[git_version]]
