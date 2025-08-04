import pytest
from conftest import Instance, TarantoolError


def test_sql_vdbe_opcode_max_and_sql_motion_row_max_options(instance: Instance):
    instance.sql("ALTER SYSTEM SET sql_motion_row_max = 10")

    # the system option by itself is fine
    instance.sql("SELECT * FROM (VALUES (1), (2))")
    # overriding it in the query fails the query
    with pytest.raises(
        TarantoolError,
        match=r"Exceeded maximum number of rows \(1\) in virtual table: 2",
    ):
        instance.sql("SELECT * FROM (VALUES (1), (2)) OPTION (sql_motion_row_max = 1)")

    instance.sql("ALTER SYSTEM SET sql_motion_row_max = 1")

    # the system option by itself fails the query
    with pytest.raises(
        TarantoolError,
        match=r"Exceeded maximum number of rows \(1\) in virtual table: 2",
    ):
        instance.sql("SELECT * FROM (VALUES (1), (2))")
    # overriding it in the query makes it work
    instance.sql("SELECT * FROM (VALUES (1), (2)) OPTION (sql_motion_row_max = 10)")

    # restore sql_motion_row_max
    instance.sql("ALTER SYSTEM SET sql_motion_row_max = 10")

    # same for the sql_vdbe_opcode_max
    instance.sql("ALTER SYSTEM SET sql_vdbe_opcode_max = 100")
    # the system option by itself is fine
    instance.sql("SELECT * FROM (VALUES (1), (2))")
    # overriding it in the query fails the query
    with pytest.raises(
        TarantoolError,
        match=r"Reached a limit on max executed vdbe opcodes. Limit: 1",
    ):
        instance.sql("SELECT * FROM (VALUES (1), (2)) OPTION (sql_vdbe_opcode_max = 1)")
    instance.sql("ALTER SYSTEM SET sql_vdbe_opcode_max = 1")
    # the system option by itself fails the query
    with pytest.raises(
        TarantoolError,
        match=r"Reached a limit on max executed vdbe opcodes. Limit: 1",
    ):
        instance.sql("SELECT * FROM (VALUES (1), (2))")
    # overriding it in the query makes it work
    instance.sql("SELECT * FROM (VALUES (1), (2)) OPTION (sql_vdbe_opcode_max = 100)")


def test_repeated_sql_options(instance: Instance):
    instance.sql("""
                 SELECT * FROM (VALUES (1), (2)) OPTION (
                   sql_motion_row_max = 1,
                   sql_motion_row_max = 10
                 )""")
    with pytest.raises(
        TarantoolError,
        match=r"unexpected number of values: Exceeded maximum number of rows \(1\) in virtual table: 2",
    ):
        instance.sql("""
                     SELECT * FROM (VALUES (1), (2)) OPTION (
                       sql_motion_row_max = 10,
                       sql_motion_row_max = 1
                     )""")

    instance.sql("""
                 SELECT * FROM (VALUES (1), (2)) OPTION (
                   sql_vdbe_opcode_max = 1,
                   sql_vdbe_opcode_max = 100
                 )""")
    with pytest.raises(
        TarantoolError,
        match=r"Reached a limit on max executed vdbe opcodes. Limit: 1",
    ):
        instance.sql("""
                     SELECT * FROM (VALUES (1), (2)) OPTION (
                       sql_vdbe_opcode_max = 10,
                       sql_vdbe_opcode_max = 1
                     )""")


def test_parametrized_sql_options(instance: Instance):
    q1 = """
         SELECT * FROM (VALUES (1), (2)) OPTION (
           sql_motion_row_max = $1,
           sql_vdbe_opcode_max = $2
         )
         """

    with pytest.raises(
        TarantoolError,
        match=r"Exceeded maximum number of rows \(1\) in virtual table: 2",
    ):
        instance.sql(q1, 1, 200)
    with pytest.raises(
        TarantoolError,
        match=r"Reached a limit on max executed vdbe opcodes. Limit: 1",
    ):
        instance.sql(q1, 200, 1)

    instance.sql("CREATE TABLE t (a INT PRIMARY KEY, b INT)")

    q2 = """
        INSERT INTO t VALUES (-1, 1), (-2, 2), (-3, 3) OPTION (sql_motion_row_max = $1)
    """

    # using a parametrized option does limit the number of rows on insertion
    with pytest.raises(
        TarantoolError,
        match=r"sbroad: unexpected number of values: Exceeded maximum number of rows \(1\) in virtual table: 3",
    ):
        instance.sql(q2, 1)
    # and it can also pass
    instance.sql(q2, 100)


def test_invalid_parametrized_sql_options(instance: Instance):
    q1 = """
         SELECT * FROM (VALUES (1), (2)) OPTION (
           sql_motion_row_max = $1
         )
         """

    with pytest.raises(
        TarantoolError,
        match=r"""sbroad: invalid query: expected 1 values for parameters, got 0""",
    ):
        instance.sql(q1)

    # extra arguments work (they are ignored)
    instance.sql(q1, 42, "ネコが鳴く")

    # NOTE: these errors are only triggerable when using parameters, because literals are limited to be unsigned at parse time
    with pytest.raises(
        TarantoolError,
        match=r"""sbroad: invalid OptionSpec: expected option sql_motion_row_max to be a non-negative integer, got: String\("1"\)""",
    ):
        # passing a string for an integer doesn't work, even if it's a valid textual representation
        instance.sql(q1, "1")
    with pytest.raises(
        TarantoolError,
        match=r"""sbroad: invalid OptionSpec: expected option sql_motion_row_max to be a non-negative integer, got: String\("много"\)""",
    ):
        # a string straight up also doesn't work
        instance.sql(q1, "много")
    with pytest.raises(
        TarantoolError,
        match=r"sbroad: invalid OptionSpec: expected option sql_motion_row_max to be a non-negative integer, got: Integer\(-1\)",
    ):
        # negative integers do not work
        instance.sql(q1, -1)
