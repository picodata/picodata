from conftest import Cluster


def test_secondary_index_sort_order(cluster: Cluster):
    [instance] = cluster.deploy(instance_count=1)

    instance.sql(
        """
        CREATE TABLE secondary_sort_order (
            id INT PRIMARY KEY,
            a INT NOT NULL,
            b INT NOT NULL
        ) DISTRIBUTED GLOBALLY
        """
    )
    instance.sql(
        """
        CREATE INDEX secondary_sort_order_idx
        ON secondary_sort_order USING TREE (a ASC, b DESC)
        """
    )
    instance.sql(
        """
        INSERT INTO secondary_sort_order VALUES
            (1, 1, 10),
            (2, 1, 30),
            (3, 1, 20),
            (4, 2, 20),
            (5, 2, 40)
        """
    )

    rows = instance.eval("return box.space.secondary_sort_order.index.secondary_sort_order_idx:select()")
    assert rows == [
        [2, 1, 30],
        [3, 1, 20],
        [1, 1, 10],
        [5, 2, 40],
        [4, 2, 20],
    ]

    [[parts]] = instance.sql("SELECT parts FROM _pico_index WHERE name = 'secondary_sort_order_idx'")
    assert parts == [
        ["a", "integer", None, False, None],
        ["b", "integer", None, False, None, "desc"],
    ]


def _assert_primary_index_sort_order(instance, table_name, columns, values, expected_rows, expected_parts):
    instance.sql(
        f"""
        CREATE TABLE {table_name} (
            {columns}
        ) DISTRIBUTED GLOBALLY
        """
    )
    instance.sql(f"INSERT INTO {table_name} VALUES {values}")

    rows = instance.eval(f"return box.space.{table_name}.index[0]:select()")
    assert rows == expected_rows

    [[table_id]] = instance.sql(f"SELECT id FROM _pico_table WHERE name = '{table_name}'")
    [[parts]] = instance.sql(f"SELECT parts FROM _pico_index WHERE table_id = {table_id} AND id = 0")
    assert parts == expected_parts


def test_table_primary_key_sort_order(cluster: Cluster):
    [instance] = cluster.deploy(instance_count=1)
    _assert_primary_index_sort_order(
        instance,
        "table_primary_sort_order",
        """
            a INT NOT NULL,
            b INT NOT NULL,
            payload INT,
            PRIMARY KEY (a DESC, b ASC)
        """,
        """
            (1, 30, 1),
            (2, 20, 2),
            (2, 10, 3),
            (1, 10, 4)
        """,
        [[2, 10, 3], [2, 20, 2], [1, 10, 4], [1, 30, 1]],
        [
            ["a", "integer", None, False, None, "desc"],
            ["b", "integer", None, False, None],
        ],
    )


def test_table_primary_key_sort_order_for_bucket_id(cluster: Cluster):
    [instance] = cluster.deploy(instance_count=1)

    instance.sql(
        """
        CREATE TABLE table_primary_bucket_sort_order (
            a INT NOT NULL,
            payload INT,
            PRIMARY KEY (bucket_id DESC, a ASC)
        ) DISTRIBUTED BY (a)
        """
    )

    [[table_id, table_format, opts]] = instance.sql(
        "SELECT id, format, opts FROM _pico_table WHERE name = 'table_primary_bucket_sort_order'"
    )
    assert table_format[0] == {
        "name": "bucket_id",
        "field_type": "unsigned",
        "is_nullable": False,
    }
    assert [field["name"] for field in table_format].count("bucket_id") == 1
    assert opts == []

    indexes = instance.sql(f"SELECT id, parts FROM _pico_index WHERE table_id = {table_id}")
    assert indexes == [
        [
            0,
            [
                ["bucket_id", "unsigned", None, False, None, "desc"],
                ["a", "integer", None, False, None],
            ],
        ]
    ]
    assert len(instance.eval(f"return box.space._index:select({table_id})")) == 1


def test_inline_primary_key_sort_order(cluster: Cluster):
    [instance] = cluster.deploy(instance_count=1)
    _assert_primary_index_sort_order(
        instance,
        "inline_primary_sort_order",
        """
            id INT PRIMARY KEY DESC,
            payload INT
        """,
        """
            (1, 10),
            (3, 30),
            (2, 20)
        """,
        [[3, 30], [2, 20], [1, 10]],
        [["id", "integer", None, False, None, "desc"]],
    )
