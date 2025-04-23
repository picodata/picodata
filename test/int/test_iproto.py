import pytest

from conftest import Instance, Connection, MalformedAPI, TarantoolError, ErrorCode


USER_NAME = "kelthuzad"
USER_PASS = "g$$dP4ss"
TABLE_NAME = "warehouse"


def create_connection(instance: Instance, use_call_16: bool = False):
    dcl = instance.sql(
        f"""
        CREATE USER {USER_NAME} WITH PASSWORD '{USER_PASS}' USING chap-sha1
    """
    )
    assert dcl["row_count"] == 1

    conn = Connection(
        instance.host,
        instance.port,
        user=USER_NAME,
        password=USER_PASS,
        connect_now=True,
        reconnect_max_attempts=0,
        call_16=use_call_16,
    )
    assert conn

    return conn


def test_iproto_forbidden(instance: Instance):
    conn = create_connection(instance, use_call_16=True)

    with pytest.raises(TarantoolError) as data:
        conn.call("fake_function")
    assert data.value.args[:2] == (
        "ER_UNSUPPORTED",
        "picodata does not support IPROTO_CALL_16 request type",
    )


def test_iproto_execute(instance: Instance):
    # https://docs.picodata.io/picodata/stable/reference/legend/#create_test_tables
    ddl = instance.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (
            id INTEGER NOT NULL,
            item TEXT NOT NULL,
            type TEXT NOT NULL,
            PRIMARY KEY (id))
        USING memtx DISTRIBUTED BY (id)
        OPTION (TIMEOUT = 3.0)
    """
    )
    assert ddl["row_count"] == 1

    # https://docs.picodata.io/picodata/stable/reference/legend/#populate_test_tables
    data = instance.sql(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
            (1, 'bricks', 'heavy'),
            (2, 'panels', 'light')
    """
    )
    assert data["row_count"] == 2

    conn = create_connection(instance)

    with pytest.raises(TarantoolError) as data:
        conn.execute(f"SELECT * FROM {TABLE_NAME}")
    assert data.value.args[:2] == (
        "ER_ACCESS_DENIED",
        f"Read access to space '{TABLE_NAME}' is denied for user '{USER_NAME}'",
    )

    acl = instance.sql(f"GRANT READ ON TABLE {TABLE_NAME} TO {USER_NAME}")
    assert acl["row_count"] == 1

    with pytest.raises(MalformedAPI) as dql:
        conn.execute(f"SELECT * FROM {TABLE_NAME}")
    assert dql.value.args == ([1, "bricks", "heavy"], [2, "panels", "light"])

    with pytest.raises(TarantoolError) as dql:  # type: ignore
        conn.execute(f"SELECT * FRUM {TABLE_NAME}")
    assert dql.value.args[:2] == (
        ErrorCode.Other,
        f"sbroad: rule parsing error:  --> 1:10\n  |\n1 | SELECT * FRUM {TABLE_NAME}\n  |          ^---\n  |\n  = expected EOI, OrderBy, Limit, UnionOp, ExceptOp, UnionAllOp, or DqlOption",  # noqa: E501
    )

    acl = instance.sql(f"GRANT WRITE ON TABLE {TABLE_NAME} TO {USER_NAME}")
    assert acl["row_count"] == 1

    dml = conn.execute(f"DELETE FROM {TABLE_NAME} WHERE id = 1")
    assert dml["row_count"] == 1
