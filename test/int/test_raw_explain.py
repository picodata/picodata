import pytest

from conftest import (
    Cluster,
    TarantoolError,
)


def test_raw_explain(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    ddl = i1.sql(
        """
        CREATE TABLE testing_space ("id" int primary key, "name" string, "product_units" int);
    """
    )
    assert ddl["row_count"] == 1

    data = i1.sql("""
        EXPLAIN (RAW) SELECT * from testing_space WHERE "id" = 1;
    """)
    assert data == [
        "1. Query (FILTERED STORAGE):",
        'SELECT "testing_space"."id", "testing_space"."name", "testing_space"."product_units" FROM "testing_space" WHERE "testing_space"."id" = CAST(1 AS int)',
        "+----------+-------+------+--------------------------------------------------------------+",
        "| selectid | order | from | detail                                                       |",
        "+========================================================================================+",
        "| 0        | 0     | 0    | SEARCH TABLE testing_space USING PRIMARY KEY (id=?) (~1 row) |",
        "+----------+-------+------+--------------------------------------------------------------+",
        "",
    ]
    data = i1.sql("""
        EXPLAIN (RAW) SELECT * from testing_space WHERE "id" = 1 ORDER BY 1 LIMIT 1;
    """)
    assert data == [
        "1. Query (FILTERED STORAGE):",
        'SELECT "testing_space"."id", "testing_space"."name", "testing_space"."product_units" FROM "testing_space" WHERE "testing_space"."id" = CAST(1 AS int)',
        "+----------+-------+------+--------------------------------------------------------------+",
        "| selectid | order | from | detail                                                       |",
        "+========================================================================================+",
        "| 0        | 0     | 0    | SEARCH TABLE testing_space USING PRIMARY KEY (id=?) (~1 row) |",
        "+----------+-------+------+--------------------------------------------------------------+",
        "",
        "2. Query (ROUTER):",
        'SELECT "COL_0" as "id", "COL_1" as "name", "COL_2" as "product_units" FROM ( SELECT "COL_0", "COL_1", "COL_2" FROM "TMP_2890830351568476674_0136" ) ORDER BY 1 LIMIT 1',
        "+----------+-------+------+---------------------------------------------------------+",
        "| selectid | order | from | detail                                                  |",
        "+===================================================================================+",
        "| 0        | 0     | 0    | SCAN TABLE TMP_2890830351568476674_0136 (~1048576 rows) |",
        "|----------+-------+------+---------------------------------------------------------|",
        "| 0        | 0     | 0    | USE TEMP B-TREE FOR ORDER BY                            |",
        "+----------+-------+------+---------------------------------------------------------+",
        "",
    ]
    data = i1.sql(
        """
        EXPLAIN (RAW) SELECT * from testing_space WHERE "id" = 1 GROUP BY 1, 2, 3 ORDER BY 1 LIMIT 1;
    """
    )
    assert data == [
        "1. Query (FILTERED STORAGE):",
        'SELECT "testing_space"."id" as "gr_expr_1", "testing_space"."name" as "gr_expr_2", "testing_space"."product_units" as "gr_expr_3" FROM "testing_space" WHERE "testing_space"."id" = CAST(1 AS int) GROUP BY "testing_space"."id", "testing_space"."name", "testing_space"."product_units"',
        "+----------+-------+------+--------------------------------------------------------------+",
        "| selectid | order | from | detail                                                       |",
        "+========================================================================================+",
        "| 0        | 0     | 0    | SEARCH TABLE testing_space USING PRIMARY KEY (id=?) (~1 row) |",
        "+----------+-------+------+--------------------------------------------------------------+",
        "",
        "2. Query (ROUTER):",
        'SELECT "id", "name", "product_units" FROM ( SELECT "COL_0" as "id", "COL_1" as "name", "COL_2" as "product_units" FROM ( SELECT "COL_0", "COL_1", "COL_2" FROM "TMP_16437131905997475581_0136" ) GROUP BY "COL_0", "COL_1", "COL_2" ) ORDER BY 1 LIMIT 1',
        "+----------+-------+------+----------------------------------------------------------+",
        "| selectid | order | from | detail                                                   |",
        "+====================================================================================+",
        "| 0        | 0     | 0    | SCAN TABLE TMP_16437131905997475581_0136 (~1048576 rows) |",
        "|----------+-------+------+----------------------------------------------------------|",
        "| 0        | 0     | 0    | USE TEMP B-TREE FOR GROUP BY                             |",
        "|----------+-------+------+----------------------------------------------------------|",
        "| 0        | 0     | 0    | USE TEMP B-TREE FOR ORDER BY                             |",
        "+----------+-------+------+----------------------------------------------------------+",
        "",
    ]

    data = i1.sql(
        """
        EXPLAIN (RAW, FMT) SELECT * from testing_space JOIN testing_space ON true GROUP BY 1, 2, 3, 4, 5, 6 ORDER BY 1 LIMIT 1;
    """
    )
    assert data == [
        "1. Query (STORAGE):",
        "SELECT",
        '  "testing_space"."id",',
        '  "testing_space"."bucket_id",',
        '  "testing_space"."name",',
        '  "testing_space"."product_units"',
        "FROM",
        '  "testing_space"',
        "+----------+-------+------+------------------------------------------+",
        "| selectid | order | from | detail                                   |",
        "+====================================================================+",
        "| 0        | 0     | 0    | SCAN TABLE testing_space (~1048576 rows) |",
        "+----------+-------+------+------------------------------------------+",
        "",
        "2. Query (STORAGE):",
        "SELECT",
        '  "testing_space"."id" as "gr_expr_1",',
        '  "testing_space"."name" as "gr_expr_2",',
        '  "testing_space"."product_units" as "gr_expr_3",',
        '  "testing_space"."COL_0" as "gr_expr_4",',
        '  "testing_space"."COL_2" as "gr_expr_5",',
        '  "testing_space"."COL_3" as "gr_expr_6"',
        "FROM",
        '  "testing_space"',
        "  INNER JOIN (",
        "    SELECT",
        '      "COL_0",',
        '      "COL_1",',
        '      "COL_2",',
        '      "COL_3"',
        "    FROM",
        '      "TMP_12096455301584828679_0136"',
        '  ) as "testing_space" ON CAST(true AS bool)',
        "GROUP BY",
        '  "testing_space"."id",',
        '  "testing_space"."name",',
        '  "testing_space"."product_units",',
        '  "testing_space"."COL_0",',
        '  "testing_space"."COL_2",',
        '  "testing_space"."COL_3"',
        "+----------+-------+------+----------------------------------------------------------+",
        "| selectid | order | from | detail                                                   |",
        "+====================================================================================+",
        "| 0        | 0     | 0    | SCAN TABLE testing_space (~1048576 rows)                 |",
        "|----------+-------+------+----------------------------------------------------------|",
        "| 0        | 1     | 1    | SCAN TABLE TMP_12096455301584828679_0136 (~1048576 rows) |",
        "|----------+-------+------+----------------------------------------------------------|",
        "| 0        | 0     | 0    | USE TEMP B-TREE FOR GROUP BY                             |",
        "+----------+-------+------+----------------------------------------------------------+",
        "",
        "3. Query (ROUTER):",
        "SELECT",
        '  "id",',
        '  "name",',
        '  "product_units",',
        '  "id",',
        '  "name",',
        '  "product_units"',
        "FROM",
        "  (",
        "    SELECT",
        '      "COL_0" as "id",',
        '      "COL_1" as "name",',
        '      "COL_2" as "product_units",',
        '      "COL_3" as "id",',
        '      "COL_4" as "name",',
        '      "COL_5" as "product_units"',
        "    FROM",
        "      (",
        "        SELECT",
        '          "COL_0",',
        '          "COL_1",',
        '          "COL_2",',
        '          "COL_3",',
        '          "COL_4",',
        '          "COL_5"',
        "        FROM",
        '          "TMP_13902639316269636821_0136"',
        "      )",
        "    GROUP BY",
        '      "COL_0",',
        '      "COL_1",',
        '      "COL_2",',
        '      "COL_3",',
        '      "COL_4",',
        '      "COL_5"',
        "  )",
        "ORDER BY",
        "  1",
        "LIMIT",
        "  1",
        "+----------+-------+------+----------------------------------------------------------+",
        "| selectid | order | from | detail                                                   |",
        "+====================================================================================+",
        "| 0        | 0     | 0    | SCAN TABLE TMP_13902639316269636821_0136 (~1048576 rows) |",
        "|----------+-------+------+----------------------------------------------------------|",
        "| 0        | 0     | 0    | USE TEMP B-TREE FOR GROUP BY                             |",
        "|----------+-------+------+----------------------------------------------------------|",
        "| 0        | 0     | 0    | USE TEMP B-TREE FOR ORDER BY                             |",
        "+----------+-------+------+----------------------------------------------------------+",
        "",
    ]

    error_message = "sbroad: unsupported plan: " + "EXPLAIN QUERY PLAN is not supported for DML queries"
    with pytest.raises(
        TarantoolError,
        match=error_message,
    ):
        i1.sql("""EXPLAIN (RAW) INSERT INTO testing_space VALUES (1, '1', 1);""")
