import socket

import pytest

from conftest import Postgres, find_routed_pk
from pgproto.copy_test_utils import (
    authenticate_md5,
    connect_admin,
    count_rows,
    create_test_table_via_instance,
    parse_error_fields,
    recv_message,
    recv_until_ready,
    set_admin_password,
    send_bind,
    send_copy_data_message,
    send_copy_done,
    send_copy_fail,
    send_execute,
    send_flush,
    send_parse,
    send_query,
    send_sync,
    send_terminate,
)


def _local_row_counts(postgres: Postgres, table_name: str, instance_count: int) -> list[int]:
    return [
        postgres.cluster.instances[index].eval(f'return box.space["{table_name}"]:count()')
        for index in range(instance_count)
    ]

def _startup_copy_session(postgres: Postgres, table_name: str) -> socket.socket:
    user = "admin"
    password = "P@ssw0rd"
    set_admin_password(postgres, password)

    sock = socket.create_connection((postgres.host, postgres.port), timeout=5)
    sock.settimeout(5)
    authenticate_md5(sock, user, password)
    send_query(sock, f'COPY "{table_name}" ("id", "value") FROM STDIN')

    message_type, payload = recv_message(sock)
    assert message_type == b"G", f"expected CopyInResponse, got {message_type!r}, payload={payload!r}"

    return sock


def _assert_error_and_ready(sock: socket.socket) -> dict[str, str]:
    error_fields = None
    unexpected_command_complete = None
    for message_type, payload in recv_until_ready(sock):
        if message_type == b"E":
            error_fields = parse_error_fields(payload)
        elif message_type == b"C":
            unexpected_command_complete = payload
    assert error_fields is not None
    assert unexpected_command_complete is None, (
        f"unexpected CommandComplete after COPY failure: {unexpected_command_complete!r}"
    )
    return error_fields


def test_copy_raw_wire_success_sequence(postgres: Postgres):
    create_test_table_via_instance(postgres, "copy_proto_success")

    sock = _startup_copy_session(postgres, "copy_proto_success")
    try:
        send_copy_data_message(sock, b"1\talpha\n2\tbeta\n")
        send_copy_done(sock)

        messages = recv_until_ready(sock)
        message_types = [message_type for message_type, _ in messages]
        assert message_types[-2:] == [b"C", b"Z"]
        assert messages[-2][1].startswith(b"COPY 2\x00")
    finally:
        send_terminate(sock)
        sock.close()

    with connect_admin(postgres) as conn:
        rows = conn.execute('SELECT "id", "value" FROM "copy_proto_success" ORDER BY "id"').fetchall()
        assert rows == [(1, "alpha"), (2, "beta")]


def test_copy_text_crlf_split_across_copydata_messages(postgres: Postgres):
    create_test_table_via_instance(postgres, "copy_proto_crlf_split")

    sock = _startup_copy_session(postgres, "copy_proto_crlf_split")
    try:
        send_copy_data_message(sock, b"1\talpha\r")
        send_copy_data_message(sock, b"\n2\tbeta\r\n")
        send_copy_done(sock)

        messages = recv_until_ready(sock)
        message_types = [message_type for message_type, _ in messages]
        assert message_types[-2:] == [b"C", b"Z"]
        assert messages[-2][1].startswith(b"COPY 2\x00")
    finally:
        send_terminate(sock)
        sock.close()

    with connect_admin(postgres) as conn:
        rows = conn.execute('SELECT "id", "value" FROM "copy_proto_crlf_split" ORDER BY "id"').fetchall()
        assert rows == [(1, "alpha"), (2, "beta")]


def test_copy_text_escaped_newline_split_across_copydata_messages(postgres: Postgres):
    create_test_table_via_instance(postgres, "copy_proto_escaped_newline_split")

    sock = _startup_copy_session(postgres, "copy_proto_escaped_newline_split")
    try:
        send_copy_data_message(sock, b"1\thello\\")
        send_copy_data_message(sock, b"\nworld\n")
        send_copy_done(sock)

        messages = recv_until_ready(sock)
        message_types = [message_type for message_type, _ in messages]
        assert message_types[-2:] == [b"C", b"Z"]
        assert messages[-2][1].startswith(b"COPY 1\x00")
    finally:
        send_terminate(sock)
        sock.close()

    with connect_admin(postgres) as conn:
        rows = conn.execute(
            'SELECT "id", "value" FROM "copy_proto_escaped_newline_split" ORDER BY "id"'
        ).fetchall()
        assert rows == [(1, "hello\nworld")]


def test_copy_extended_query_success_sequence(postgres: Postgres):
    create_test_table_via_instance(postgres, "copy_proto_extended")

    user = "admin"
    password = "P@ssw0rd"
    set_admin_password(postgres, password)

    with socket.create_connection((postgres.host, postgres.port), timeout=5) as sock:
        sock.settimeout(5)
        authenticate_md5(sock, user, password)

        send_parse(sock, "copy_stmt", 'COPY "copy_proto_extended" ("id", "value") FROM STDIN')
        send_bind(sock, portal_name="copy_portal", statement_name="copy_stmt")
        send_execute(sock, portal_name="copy_portal")

        assert recv_message(sock)[0] == b"1"
        assert recv_message(sock)[0] == b"2"
        assert recv_message(sock)[0] == b"G"

        send_copy_data_message(sock, b"1\talpha\n2\tbeta\n")
        send_copy_done(sock)
        send_sync(sock)

        messages = recv_until_ready(sock)
        message_types = [message_type for message_type, _ in messages]
        assert message_types[-2:] == [b"C", b"Z"]
        assert messages[-2][1].startswith(b"COPY 2\x00")

        send_terminate(sock)

    with connect_admin(postgres) as conn:
        rows = conn.execute('SELECT "id", "value" FROM "copy_proto_extended" ORDER BY "id"').fetchall()
        assert rows == [(1, "alpha"), (2, "beta")]


def test_copy_extended_query_sharded_remote_conflict_recovers_pipeline(postgres: Postgres):
    create_test_table_via_instance(postgres, "copy_proto_extended_conflict")
    remote_instance = postgres.cluster.add_instance(
        wait_online=True, replicaset_name="copy_proto_conflict_rs2"
    )
    for instance in (postgres.instance, remote_instance):
        postgres.cluster.wait_until_instance_has_this_many_active_buckets(
            instance, 1500, max_retries=20
        )

    with connect_admin(postgres) as conn:
        local_pk = find_routed_pk(postgres.instance, is_local=True)
        remote_pk = find_routed_pk(postgres.instance, is_local=False, start=local_pk + 1)
        conn.execute(
            'INSERT INTO "copy_proto_extended_conflict" ("id", "value") VALUES (%s, %s)',
            (remote_pk, "existing-remote"),
        )

    user = "admin"
    password = "P@ssw0rd"
    set_admin_password(postgres, password)

    with socket.create_connection((postgres.host, postgres.port), timeout=5) as sock:
        sock.settimeout(5)
        authenticate_md5(sock, user, password)

        send_parse(
            sock,
            "copy_stmt",
            'COPY "copy_proto_extended_conflict" ("id", "value") '
            "FROM STDIN WITH (SESSION_FLUSH_ROWS = 2, DESTINATION_FLUSH_ROWS = 2)",
        )
        send_bind(sock, portal_name="copy_portal", statement_name="copy_stmt")
        send_execute(sock, portal_name="copy_portal")

        assert recv_message(sock)[0] == b"1"
        assert recv_message(sock)[0] == b"2"
        assert recv_message(sock)[0] == b"G"

        send_copy_data_message(sock, f"{local_pk}\tlocal-prefix\n{remote_pk}\tremote-conflict\n".encode())
        send_copy_done(sock)
        send_sync(sock)

        error_fields = _assert_error_and_ready(sock)
        assert error_fields.get("C") == "XX000"
        assert "Duplicate key exists" in error_fields.get("M", "")

        send_query(sock, "SELECT 1")
        messages = recv_until_ready(sock)
        assert [message_type for message_type, _ in messages][-2:] == [b"C", b"Z"]

        send_terminate(sock)

    with connect_admin(postgres) as conn:
        rows = conn.execute(
            'SELECT "id", "value" FROM "copy_proto_extended_conflict" ORDER BY "id"'
        ).fetchall()
        assert rows == [
            (local_pk, "local-prefix"),
            (remote_pk, "existing-remote"),
        ]
    assert sorted(_local_row_counts(postgres, "copy_proto_extended_conflict", 2)) == [1, 1]


@pytest.mark.parametrize(
    ("message_sender", "expected_snippet"),
    [
        pytest.param(lambda sock: send_query(sock, "SELECT 1"), "Query", id="query"),
        pytest.param(lambda sock: send_parse(sock, "", "SELECT 1"), "Parse", id="parse"),
        pytest.param(lambda sock: send_bind(sock), "Bind", id="bind"),
        pytest.param(lambda sock: send_execute(sock), "Execute", id="execute"),
    ],
)
def test_copy_rejects_invalid_messages_during_copy_mode(
    postgres: Postgres,
    message_sender,
    expected_snippet: str,
):
    create_test_table_via_instance(postgres, f"copy_invalid_message_{expected_snippet.lower()}")

    sock = _startup_copy_session(postgres, f"copy_invalid_message_{expected_snippet.lower()}")
    try:
        message_sender(sock)
        error_fields = _assert_error_and_ready(sock)
        assert error_fields.get("C") == "08P01"
        assert expected_snippet in error_fields.get("M", "")
    finally:
        send_terminate(sock)
        sock.close()


@pytest.mark.parametrize(
    ("sender", "expected_snippet"),
    [
        pytest.param(lambda sock: send_copy_data_message(sock, b"1\toutside_copy\n"), "CopyData", id="copy_data"),
        pytest.param(lambda sock: send_copy_done(sock), "CopyDone", id="copy_done"),
        pytest.param(lambda sock: send_copy_fail(sock, "fail outside copy"), "CopyFail", id="copy_fail"),
    ],
)
def test_copy_messages_outside_copy_mode_fail_cleanly(postgres: Postgres, sender, expected_snippet: str):
    user = "admin"
    password = "P@ssw0rd"
    set_admin_password(postgres, password)

    with socket.create_connection((postgres.host, postgres.port), timeout=5) as sock:
        sock.settimeout(5)
        authenticate_md5(sock, user, password)
        sender(sock)

        error_fields = _assert_error_and_ready(sock)
        assert error_fields.get("C") == "08P01"
        assert expected_snippet in error_fields.get("M", "")

        send_terminate(sock)


def test_copy_ignores_sync_during_copy_mode(postgres: Postgres):
    create_test_table_via_instance(postgres, "copy_sync_ignored")

    sock = _startup_copy_session(postgres, "copy_sync_ignored")
    try:
        send_sync(sock)
        send_copy_data_message(sock, b"1\talpha\n")
        send_copy_done(sock)

        messages = recv_until_ready(sock)
        message_types = [message_type for message_type, _ in messages]
        assert message_types[-2:] == [b"C", b"Z"]
        assert messages[-2][1].startswith(b"COPY 1\x00")
    finally:
        send_terminate(sock)
        sock.close()

    with connect_admin(postgres) as conn:
        rows = conn.execute('SELECT "id", "value" FROM "copy_sync_ignored" ORDER BY "id"').fetchall()
        assert rows == [(1, "alpha")]


def test_copy_fail_aborts_without_persisting_rows(postgres: Postgres):
    create_test_table_via_instance(postgres, "copy_fail_state")

    sock = _startup_copy_session(postgres, "copy_fail_state")
    try:
        send_copy_fail(sock, "client aborted copy")
        error_fields = _assert_error_and_ready(sock)
        assert "COPY from stdin failed" in error_fields.get("M", "")
    finally:
        send_terminate(sock)
        sock.close()

    with connect_admin(postgres) as conn:
        assert count_rows(conn, "copy_fail_state") == 0


def test_copy_server_side_error_recovers_connection_without_persisting_rows(postgres: Postgres):
    create_test_table_via_instance(postgres, "copy_server_error_state")

    sock = _startup_copy_session(postgres, "copy_server_error_state")
    try:
        send_copy_data_message(sock, b"bad\tboom\n")
        send_copy_done(sock)
        error_fields = _assert_error_and_ready(sock)
        assert error_fields.get("C") == "22P02"

        send_query(sock, "SELECT 1")
        messages = recv_until_ready(sock)
        assert [message_type for message_type, _ in messages][-2:] == [b"C", b"Z"]
    finally:
        send_terminate(sock)
        sock.close()

    with connect_admin(postgres) as conn:
        assert count_rows(conn, "copy_server_error_state") == 0


@pytest.mark.parametrize(
    "send_stale_message",
    [
        pytest.param(send_copy_done, id="copy_done"),
        pytest.param(lambda sock: send_copy_data_message(sock, b"2\tstale\n"), id="copy_data"),
        pytest.param(lambda sock: send_copy_fail(sock, "stale client abort"), id="copy_fail"),
        pytest.param(send_flush, id="flush"),
        pytest.param(send_sync, id="sync"),
    ],
)
def test_copy_server_side_error_drops_stale_messages_during_unwind(
    postgres: Postgres,
    send_stale_message,
):
    create_test_table_via_instance(postgres, "copy_server_error_unwind")

    sock = _startup_copy_session(postgres, "copy_server_error_unwind")
    try:
        send_copy_data_message(sock, b"1\n")
        error_fields = _assert_error_and_ready(sock)
        assert error_fields.get("C") == "22P04"

        send_stale_message(sock)
        send_query(sock, "SELECT 1")
        messages = recv_until_ready(sock)
        assert [message_type for message_type, _ in messages][-2:] == [b"C", b"Z"]
    finally:
        send_terminate(sock)
        sock.close()

    with connect_admin(postgres) as conn:
        assert count_rows(conn, "copy_server_error_unwind") == 0


def test_copy_rejects_oversized_row_in_single_copydata_message(postgres: Postgres):
    create_test_table_via_instance(postgres, "copy_proto_oversized_row")

    sock = _startup_copy_session(postgres, "copy_proto_oversized_row")
    try:
        oversized_row = b"1\t" + (b"x" * (1024 * 1024)) + b"\n"
        send_copy_data_message(sock, oversized_row)
        error_fields = _assert_error_and_ready(sock)
        assert error_fields.get("C") == "22P04"
        assert "COPY row exceeds maximum size" in error_fields.get("M", "")

        send_query(sock, "SELECT 1")
        messages = recv_until_ready(sock)
        assert [message_type for message_type, _ in messages][-2:] == [b"C", b"Z"]
    finally:
        send_terminate(sock)
        sock.close()

    with connect_admin(postgres) as conn:
        assert count_rows(conn, "copy_proto_oversized_row") == 0


def test_copy_rejects_oversized_row_split_across_copydata_messages(postgres: Postgres):
    create_test_table_via_instance(postgres, "copy_proto_oversized_row_split")

    sock = _startup_copy_session(postgres, "copy_proto_oversized_row_split")
    try:
        send_copy_data_message(sock, b"1\t" + (b"x" * (700 * 1024)))
        send_copy_data_message(sock, b"x" * (400 * 1024))
        error_fields = _assert_error_and_ready(sock)
        assert error_fields.get("C") == "22P04"
        assert "COPY row exceeds maximum size" in error_fields.get("M", "")

        send_query(sock, "SELECT 1")
        messages = recv_until_ready(sock)
        assert [message_type for message_type, _ in messages][-2:] == [b"C", b"Z"]
    finally:
        send_terminate(sock)
        sock.close()

    with connect_admin(postgres) as conn:
        assert count_rows(conn, "copy_proto_oversized_row_split") == 0


def test_copy_rejects_mixed_line_endings_with_bad_copy_file_format(postgres: Postgres):
    create_test_table_via_instance(postgres, "copy_proto_mixed_line_endings")

    sock = _startup_copy_session(postgres, "copy_proto_mixed_line_endings")
    try:
        send_copy_data_message(sock, b"1\talpha\n2\tbeta\r\n")
        error_fields = _assert_error_and_ready(sock)
        assert error_fields.get("C") == "22P04"
        assert "mixed line endings" in error_fields.get("M", "")

        send_query(sock, "SELECT 1")
        messages = recv_until_ready(sock)
        assert [message_type for message_type, _ in messages][-2:] == [b"C", b"Z"]
    finally:
        send_terminate(sock)
        sock.close()

    with connect_admin(postgres) as conn:
        assert count_rows(conn, "copy_proto_mixed_line_endings") == 0


def test_copy_rejects_trailing_escape_with_bad_copy_file_format(postgres: Postgres):
    create_test_table_via_instance(postgres, "copy_proto_trailing_escape")

    sock = _startup_copy_session(postgres, "copy_proto_trailing_escape")
    try:
        send_copy_data_message(sock, b"1\tbroken\\")
        send_copy_done(sock)
        error_fields = _assert_error_and_ready(sock)
        assert error_fields.get("C") == "22P04"
        assert "ended inside an escape sequence" in error_fields.get("M", "")

        send_query(sock, "SELECT 1")
        messages = recv_until_ready(sock)
        assert [message_type for message_type, _ in messages][-2:] == [b"C", b"Z"]
    finally:
        send_terminate(sock)
        sock.close()

    with connect_admin(postgres) as conn:
        assert count_rows(conn, "copy_proto_trailing_escape") == 0


def test_copy_rejects_stale_schema_before_apply(postgres: Postgres):
    create_test_table_via_instance(postgres, "copy_stale_schema")

    sock = _startup_copy_session(postgres, "copy_stale_schema")
    try:
        send_copy_data_message(sock, b"1\talpha\n")

        conn = connect_admin(postgres)
        try:
            conn.execute('ALTER TABLE "copy_stale_schema" ADD COLUMN "extra" STRING')
        finally:
            conn.close()

        send_copy_done(sock)
        error_fields = _assert_error_and_ready(sock)
        assert "schema changed during execution" in error_fields.get("M", "")

        send_query(sock, "SELECT 1")
        messages = recv_until_ready(sock)
        assert [message_type for message_type, _ in messages][-2:] == [b"C", b"Z"]
    finally:
        send_terminate(sock)
        sock.close()

    with connect_admin(postgres) as conn:
        assert count_rows(conn, "copy_stale_schema") == 0
