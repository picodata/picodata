from conftest import Cluster


def test_secondary_index_sort_order(cluster: Cluster):
    cluster.deploy(instance_count=1)
    instance = cluster.instances[0]

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
