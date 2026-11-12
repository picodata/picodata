from typing import Any

import pytest
from conftest import Cluster, TarantoolError

# The type ANY (and its array ANY[]) accepts a value of every type. It is
# for the tables of system migrations: only the system catalog upgrade may
# declare a column of it, not even admin. The tests use an error injection.


def allow_any_ddl(*instances):
    """Let a user declare ANY columns; only the catalog upgrade can without
    the injection."""
    for instance in instances:
        # The variable arms the injection again after a restart.
        instance.env["PICODATA_ERROR_INJECTION_ALLOW_USER_ANY_DDL"] = "1"
        instance.call("pico._inject_error", "ALLOW_USER_ANY_DDL", True)


def test_any_columns(cluster: Cluster):
    i1 = cluster.deploy(instance_count=1)[0]
    allow_any_ddl(i1)

    # The injection declares columns of type ANY and ANY[] ...
    i1.sql(
        """
        CREATE TABLE t (
            id INT PRIMARY KEY,
            val ANY,
            vals ANY[] NOT NULL
        )
        DISTRIBUTED GLOBALLY
        """
    )
    # ... and the format keeps them as `any` and `array`. A column of type
    # ANY is always nullable: tarantool checks no nullability of it.
    [[fmt]] = i1.sql("SELECT format FROM _pico_table WHERE name = 't'", sudo=True)
    assert fmt == [
        {"name": "id", "field_type": "integer", "is_nullable": False},
        {"name": "val", "field_type": "any", "is_nullable": True},
        {"name": "vals", "field_type": "array", "is_nullable": False},
    ]

    # A column of type ANY holds values of every type, NULL included.
    i1.sql("INSERT INTO t VALUES (1, 42, ARRAY[1, 2])")
    i1.sql("INSERT INTO t VALUES (2, 'one', ARRAY['a'])")
    i1.sql("INSERT INTO t VALUES (3, NULL, ARRAY[true])")
    assert i1.sql("SELECT val FROM t WHERE id = 1") == [[42]]
    assert i1.sql("SELECT val FROM t WHERE id = 2") == [["one"]]
    assert i1.sql("SELECT vals FROM t WHERE id = 3") == [[[True]]]
    assert i1.sql("SELECT count(*) FROM t") == [[3]]
    # The NOT NULL of an ANY[] column is enforced, as of any array: the
    # NULL is rejected (the message names the type of the analysis hint,
    # not NULL).
    with pytest.raises(TarantoolError, match="INSERT column at position 3"):
        i1.sql("INSERT INTO t VALUES (4, 1, NULL)")

    # An expression of type any needs a cast where the type decides, as
    # overload resolution does. A cast is by value.
    with pytest.raises(TarantoolError, match="could not resolve function overload"):
        i1.sql("SELECT count(val) FROM t")
    assert i1.sql("SELECT count(val::int) FROM t WHERE id = 1") == [[1]]

    # A parameter into a column of type ANY keeps the type it comes with:
    # the column gives the analysis no desired type, so the parameter is
    # inferred as text.
    i1.sql("INSERT INTO t VALUES (4, ?, ARRAY['x'])", "param")
    assert i1.sql("SELECT val FROM t WHERE id = 4") == [["param"]]

    # The type system knows nothing of assignments to ANY columns: an
    # expression over one is analyzed as usual, outside it.
    assert i1.sql("SELECT id FROM t WHERE val IS NULL") == [[3]]

    # A column of type ANY takes a value of every type: UPDATE analyzes the
    # expression with no desired type, and the value keeps the type it is
    # inferred with.
    i1.sql("ALTER TABLE t ADD COLUMN extra ANY")
    i1.sql("UPDATE t SET extra = ARRAY[1] WHERE id = 1")
    assert i1.sql("SELECT extra FROM t WHERE id = 1") == [[[1]]]
    i1.sql("UPDATE t SET val = 42, vals = ARRAY[2] WHERE id = 2")
    assert i1.sql("SELECT val, vals FROM t WHERE id = 2") == [[42, [2]]]


def test_any_columns_are_for_migrations_only(cluster: Cluster):
    i1 = cluster.deploy(instance_count=1)[0]
    i1.sql("CREATE USER alice WITH PASSWORD 'T0psecret' USING chap-sha1", sudo=True)
    i1.sql("GRANT CREATE TABLE TO alice", sudo=True)
    alice: dict[str, Any] = dict(user="alice", password="T0psecret")

    # Not even admin declares a column of type ANY outside a system
    # migration ...
    with pytest.raises(TarantoolError, match="column val: the type ANY is available only to system migrations"):
        i1.sql("CREATE TABLE t (id INT PRIMARY KEY, val ANY) DISTRIBUTED GLOBALLY")
    # ... and neither does a user, with any spelling ...
    with pytest.raises(TarantoolError, match="column val: the type ANY is available only to system migrations"):
        i1.sql("CREATE TABLE t (id INT PRIMARY KEY, val ANY) DISTRIBUTED GLOBALLY", **alice)
    with pytest.raises(TarantoolError, match="column vals: the type ANY is available only to system migrations"):
        i1.sql("CREATE TABLE t (id INT PRIMARY KEY, vals ANY[]) DISTRIBUTED GLOBALLY", **alice)
    with pytest.raises(TarantoolError, match="column vals: the type ANY is available only to system migrations"):
        i1.sql("CREATE TABLE t (id INT PRIMARY KEY, vals ANY ARRAY) DISTRIBUTED GLOBALLY", **alice)

    # ... and not through ALTER TABLE either.
    i1.sql("CREATE TABLE t (id INT PRIMARY KEY) DISTRIBUTED GLOBALLY", sudo=True)
    with pytest.raises(TarantoolError, match="column extra: the type ANY is available only to system migrations"):
        i1.sql("ALTER TABLE t ADD COLUMN extra ANY", sudo=True)
    with pytest.raises(TarantoolError, match="column extras: the type ANY is available only to system migrations"):
        i1.sql("ALTER TABLE t ADD COLUMN extras ANY[]", **alice)

    # The injection is the only other way to declare such a column.
    allow_any_ddl(i1)
    assert i1.sql("CREATE TABLE u (id INT PRIMARY KEY, val ANY) DISTRIBUTED GLOBALLY", **alice)["row_count"] == 1

    # A user still creates tables and columns of other types.
    assert i1.sql("CREATE TABLE v (id INT PRIMARY KEY, b TEXT) DISTRIBUTED GLOBALLY", **alice)["row_count"] == 1
    assert i1.sql("ALTER TABLE v ADD COLUMN c INT", **alice)["row_count"] == 1
    i1.sql("INSERT INTO v VALUES (1, 'one', 2)", **alice)
    assert i1.sql("SELECT b FROM v WHERE id = 1", **alice) == [["one"]]

    # A user reads and writes the ANY columns of any table.
    i1.sql("INSERT INTO u VALUES (1, 'one')", **alice)
    assert i1.sql("SELECT val FROM u WHERE id = 1", **alice) == [["one"]]


def test_any_columns_cannot_be_not_null_or_indexed(cluster: Cluster):
    i1 = cluster.deploy(instance_count=1)[0]
    # The permission check comes before the NOT NULL check.
    allow_any_ddl(i1)
    i1.sql("CREATE TABLE t (id INT PRIMARY KEY) DISTRIBUTED GLOBALLY", sudo=True)

    # ANY accepts NULL whatever the declaration says. Nothing would
    # enforce a NOT NULL constraint, so it is rejected.
    with pytest.raises(TarantoolError, match="column val: a column of type ANY cannot be NOT NULL"):
        i1.sql(
            "CREATE TABLE bad (id INT PRIMARY KEY, val ANY NOT NULL) DISTRIBUTED GLOBALLY",
            sudo=True,
        )
    with pytest.raises(TarantoolError, match="column extra: a column of type ANY cannot be NOT NULL"):
        i1.sql("ALTER TABLE t ADD COLUMN extra ANY NOT NULL", sudo=True)

    # ANY columns are not indexable, as ARRAY columns are not: without the
    # check the statement reaches DDL and fails there with a raw tarantool
    # message about the unsupported field type.
    with pytest.raises(
        TarantoolError,
        match="ANY columns are not indexable: column id cannot be part of a primary key",
    ):
        i1.sql("CREATE TABLE bad (id ANY PRIMARY KEY, b INT) DISTRIBUTED GLOBALLY", sudo=True)
    with pytest.raises(
        TarantoolError,
        match="ANY columns are not indexable: column b cannot be part of a primary key",
    ):
        i1.sql("CREATE TABLE bad (a INT, b ANY, PRIMARY KEY (b)) DISTRIBUTED GLOBALLY", sudo=True)


def test_any_columns_support_no_operations(cluster: Cluster):
    # No operation consumes `any`, not even a comparison with another
    # `any`. Only a cast unlocks the value. The test uses its own table, so
    # no check depends on the type of a catalog column.
    i1 = cluster.deploy(instance_count=1)[0]
    allow_any_ddl(i1)
    i1.sql("CREATE TABLE t (id INT PRIMARY KEY, val ANY, vals ANY[]) DISTRIBUTED GLOBALLY")
    i1.sql("INSERT INTO t VALUES (1, 42, ARRAY[1])")

    for column, any_error, concrete_value in [
        ("val", "unexpected expression of type any", "42"),
        ("vals", r"unexpected expression of type array\(any\)", "ARRAY[1]"),
    ]:
        # A comparison with another `any` is as invalid as one with a
        # concrete value: the comparison unifies the operands, and `any`
        # cannot be a desired type.
        with pytest.raises(TarantoolError, match=any_error):
            i1.sql(f"SELECT id FROM t WHERE {column} = {column}")
        with pytest.raises(TarantoolError, match=any_error):
            i1.sql(f"SELECT id FROM t WHERE {column} = {concrete_value}")
        # No operator and no function takes `any`.
        with pytest.raises(TarantoolError, match="could not resolve operator overload"):
            i1.sql(f"SELECT {column} + 1 FROM t")
        with pytest.raises(TarantoolError, match="could not resolve function overload"):
            i1.sql(f"SELECT upper({column}) FROM t")

    # Reading is allowed: the value passes around until it is cast.
    assert i1.sql("SELECT val, vals FROM t WHERE id = 1") == [[42, [1]]]
    assert i1.sql("SELECT id FROM t WHERE val IS NULL") == []
    assert i1.sql("SELECT val::int FROM t WHERE id = 1") == [[42]]
    assert i1.sql("SELECT id FROM t WHERE val::int = 42") == [[1]]
    i1.sql("CREATE TABLE copy (id INT PRIMARY KEY, val ANY, vals ANY[]) DISTRIBUTED GLOBALLY")
    i1.sql("INSERT INTO copy SELECT id, val, vals FROM t")
    assert i1.sql("SELECT val, vals FROM copy WHERE id = 1") == [[42, [1]]]


def test_any_columns_work_after_a_cast(cluster: Cluster):
    # The other side of the contract: a cast to a concrete type names the
    # actual type of the value, and every expression works from then on.
    i1 = cluster.deploy(instance_count=1)[0]
    allow_any_ddl(i1)
    i1.sql("CREATE TABLE t (id INT PRIMARY KEY, val ANY, vals ANY[]) DISTRIBUTED GLOBALLY")
    i1.sql("INSERT INTO t VALUES (1, 42, ARRAY[1])")
    i1.sql("INSERT INTO t VALUES (2, 'one', ARRAY['a'])")

    # The cast feeds operators, functions and comparisons. A cast is by
    # value, so a row where the value is 'one' does not pass `val::int`:
    # the `id` predicate keeps the scan away from it.
    assert i1.sql("SELECT val::int + 1 FROM t WHERE id = 1") == [[43]]
    assert i1.sql("SELECT upper(val::text) FROM t WHERE id = 2") == [["ONE"]]
    assert i1.sql("SELECT id FROM t WHERE id = 1 AND val::int = 42") == [[1]]
    assert i1.sql("SELECT id FROM t WHERE val::text = 'one'") == [[2]]
    assert i1.sql("SELECT coalesce(val::int, 0) FROM t WHERE id = 1") == [[42]]
    assert i1.sql("SELECT count(val::int) FROM t WHERE id = 1") == [[1]]
    # An array of type `any` casts to a typed array.
    assert i1.sql("SELECT vals::text[] FROM t WHERE id = 2") == [[["a"]]]


def test_any_pointwise_lookups_and_inequality(cluster: Cluster):
    # A cast of ANY applies row-wise, over every value that fits it.
    i1 = cluster.deploy(instance_count=1)[0]
    allow_any_ddl(i1)
    i1.sql("CREATE TABLE t (id INT PRIMARY KEY, val ANY) DISTRIBUTED GLOBALLY")
    i1.sql("INSERT INTO t VALUES (1, 42)")
    i1.sql("INSERT INTO t VALUES (2, 43)")

    assert i1.sql("SELECT id FROM t WHERE val::int = 42") == [[1]]
    assert i1.sql("SELECT id FROM t WHERE val::int >= 42 ORDER BY id") == [[1], [2]]
    assert i1.sql("SELECT id FROM t WHERE val::int > 42") == [[2]]
    assert i1.sql("SELECT id FROM t WHERE val::int < 43 ORDER BY id") == [[1]]
    assert i1.sql("SELECT id FROM t WHERE val::int <> 42") == [[2]]
    assert i1.sql("SELECT id FROM t WHERE val::int BETWEEN 42 AND 43 ORDER BY id") == [
        [1],
        [2],
    ]

    # A value that does not fit the cast fails the query with its name.
    i1.sql("INSERT INTO t VALUES (3, 'one')")
    with pytest.raises(TarantoolError, match="can not convert any\\('one'\\) to integer"):
        i1.sql("SELECT id FROM t WHERE val::int = 42")


def test_any_columns_reject_on_conflict_update(cluster: Cluster):
    # ON CONFLICT DO UPDATE has only the forms `col = col op value`, and no
    # operator takes `any`.
    i1 = cluster.deploy(instance_count=1)[0]
    allow_any_ddl(i1)
    i1.sql("CREATE TABLE t (id INT PRIMARY KEY, val ANY, vals ANY[]) DISTRIBUTED GLOBALLY")
    i1.sql("INSERT INTO t VALUES (1, 42, ARRAY[1])")

    for update, error in [
        ("val = val + 1", "unexpected expression of type any"),
        ("val = val - 1", "unexpected expression of type any"),
        ("val = val + 'abc'", "unexpected expression of type any"),
        ("val = val OR true", "unexpected expression of type any"),
        ("vals = vals + 1", r"unexpected expression of type array\(any\)"),
    ]:
        with pytest.raises(TarantoolError, match=error):
            i1.sql(f"INSERT INTO t VALUES (1, 43, ARRAY[2]) ON CONFLICT (id) DO UPDATE SET {update}")
    assert i1.sql("SELECT val, vals FROM t WHERE id = 1") == [[42, [1]]]


def test_any_column_checks_order_and_index(cluster: Cluster):
    i1 = cluster.deploy(instance_count=1)[0]

    # Without the permission, the NOT NULL error must not hide the
    # permission error.
    with pytest.raises(TarantoolError, match="column val: the type ANY is available only to system migrations"):
        i1.sql("CREATE TABLE t (id INT PRIMARY KEY, val ANY NOT NULL) DISTRIBUTED GLOBALLY", sudo=True)

    allow_any_ddl(i1)
    i1.sql("CREATE TABLE t (id INT PRIMARY KEY, val ANY, vals ANY[]) DISTRIBUTED GLOBALLY", sudo=True)
    with pytest.raises(TarantoolError, match="index type tree does not support column type any"):
        i1.sql("CREATE INDEX i ON t (val)", sudo=True)
    with pytest.raises(TarantoolError, match="index type tree does not support column type array"):
        i1.sql("CREATE INDEX i ON t (vals)", sudo=True)
