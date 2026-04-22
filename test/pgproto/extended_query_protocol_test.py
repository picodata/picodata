import socket

from conftest import Postgres
from pgproto.copy_test_utils import (
    authenticate_md5,
    parse_error_fields,
    recv_message,
    recv_until_ready,
    send_bind,
    send_parse,
    send_query,
    set_admin_password,
    send_sync,
)


def test_extended_query_error_without_sync_recovers_after_many_messages_before_sync(
    postgres: Postgres,
):
    user = "admin"
    password = "P@ssw0rd"
    set_admin_password(postgres, password)

    with socket.create_connection((postgres.host, postgres.port), timeout=5) as sock:
        sock.settimeout(5)
        authenticate_md5(sock, user, password)

        send_parse(sock, "missing_sync_stmt", "SELECT $1")
        send_bind(sock, statement_name="missing_sync_stmt")

        assert recv_message(sock)[0] == b"1"
        message_type, payload = recv_message(sock)
        assert message_type == b"E"

        error_fields = parse_error_fields(payload)
        assert error_fields.get("C") == "08P01"
        assert "requires 1" in error_fields.get("M", "")

        sock.sendall(b"H\x00\x00\x00\x04" * 2048)
        send_sync(sock)

        messages = recv_until_ready(sock)
        assert [message_type for message_type, _ in messages][-1] == b"Z"

        send_query(sock, "SELECT 1")
        messages = recv_until_ready(sock)
        assert [message_type for message_type, _ in messages][-2:] == [b"C", b"Z"]
