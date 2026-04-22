import hashlib
import socket
import struct

import psycopg

from conftest import Postgres

ADMIN_USER = "admin"
ADMIN_PASSWORD = "P@ssw0rd"


def _create_test_table_sql(table_name: str) -> str:
    return f"""
        create table "{table_name}" (
            "id" integer not null,
            "value" string,
            primary key ("id")
        )
        using memtx distributed by ("id")
        option (timeout = 3);
        """


def connect_admin(postgres: Postgres):
    user = ADMIN_USER
    password = ADMIN_PASSWORD
    set_admin_password(postgres, password)
    conn = psycopg.connect(
        f"user={user} password={password} host={postgres.host} port={postgres.port} sslmode=disable"
    )
    conn.autocommit = True
    return conn


def create_test_table(conn: psycopg.Connection, table_name: str):
    conn.execute(_create_test_table_sql(table_name))


def create_test_table_via_instance(postgres: Postgres, table_name: str):
    postgres.instance.sql(_create_test_table_sql(table_name))


def count_rows(conn: psycopg.Connection, table_name: str) -> int:
    row = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
    assert row is not None
    return int(row[0])


def recv_exact(sock: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise RuntimeError("unexpected EOF while reading pgproto frame")
        data.extend(chunk)
    return bytes(data)


def recv_message(sock: socket.socket) -> tuple[bytes, bytes]:
    message_type = recv_exact(sock, 1)
    message_len = struct.unpack("!I", recv_exact(sock, 4))[0]
    payload = recv_exact(sock, message_len - 4)
    return message_type, payload


def parse_error_fields(payload: bytes) -> dict[str, str]:
    fields: dict[str, str] = {}
    idx = 0
    while idx < len(payload):
        field_code = payload[idx]
        if field_code == 0:
            break
        idx += 1
        end = payload.index(0, idx)
        fields[chr(field_code)] = payload[idx:end].decode("utf-8", errors="replace")
        idx = end + 1
    return fields


def md5_password(password: str, username: str, salt: bytes) -> str:
    step1 = hashlib.md5((password + username).encode("utf-8")).hexdigest().encode("utf-8")
    step2 = hashlib.md5(step1 + salt).hexdigest()
    return "md5" + step2


def send_startup(sock: socket.socket, user: str):
    params = (
        b"user\x00"
        + user.encode("utf-8")
        + b"\x00database\x00postgres\x00client_encoding\x00UTF8\x00\x00"
    )
    body = struct.pack("!I", 196608) + params
    packet = struct.pack("!I", len(body) + 4) + body
    sock.sendall(packet)


def send_password(sock: socket.socket, password: str):
    payload = password.encode("utf-8") + b"\x00"
    packet = b"p" + struct.pack("!I", len(payload) + 4) + payload
    sock.sendall(packet)


def authenticate_md5(sock: socket.socket, user: str, password: str):
    send_startup(sock, user)
    auth_ok = False

    while True:
        message_type, payload = recv_message(sock)

        if message_type == b"R":
            auth_code = struct.unpack("!I", payload[:4])[0]
            if auth_code == 0:
                auth_ok = True
            elif auth_code == 5:
                salt = payload[4:8]
                send_password(sock, md5_password(password, user, salt))
            else:
                raise AssertionError(f"unexpected auth method code: {auth_code}")
            continue

        if message_type == b"E":
            fields = parse_error_fields(payload)
            raise AssertionError(f"authentication failed: {fields.get('M', 'unknown error')}")

        if message_type == b"Z":
            assert auth_ok, "got ReadyForQuery before authentication completed"
            return


def set_admin_password(postgres: Postgres, password: str = ADMIN_PASSWORD):
    postgres.instance.sql(f"ALTER USER \"{ADMIN_USER}\" WITH PASSWORD '{password}' USING md5")


def send_frame(sock: socket.socket, message_type: bytes, payload: bytes = b""):
    assert len(message_type) == 1
    sock.sendall(message_type + struct.pack("!I", len(payload) + 4) + payload)


def send_query(sock: socket.socket, query: str):
    send_frame(sock, b"Q", query.encode("utf-8") + b"\x00")


def send_copy_data_message(sock: socket.socket, payload: bytes):
    send_frame(sock, b"d", payload)


def send_copy_done(sock: socket.socket):
    send_frame(sock, b"c")


def send_copy_fail(sock: socket.socket, message: str):
    send_frame(sock, b"f", message.encode("utf-8") + b"\x00")


def send_flush(sock: socket.socket):
    send_frame(sock, b"H")


def send_sync(sock: socket.socket):
    send_frame(sock, b"S")


def send_parse(sock: socket.socket, name: str, query: str):
    payload = (
        name.encode("utf-8")
        + b"\x00"
        + query.encode("utf-8")
        + b"\x00"
        + struct.pack("!H", 0)
    )
    send_frame(sock, b"P", payload)


def send_bind(sock: socket.socket, portal_name: str = "", statement_name: str = ""):
    payload = (
        portal_name.encode("utf-8")
        + b"\x00"
        + statement_name.encode("utf-8")
        + b"\x00"
        + struct.pack("!H", 0)
        + struct.pack("!H", 0)
        + struct.pack("!H", 0)
    )
    send_frame(sock, b"B", payload)


def send_execute(sock: socket.socket, portal_name: str = "", max_rows: int = 0):
    payload = portal_name.encode("utf-8") + b"\x00" + struct.pack("!I", max_rows)
    send_frame(sock, b"E", payload)


def send_terminate(sock: socket.socket):
    sock.sendall(b"X\x00\x00\x00\x04")


def recv_until_ready(sock: socket.socket) -> list[tuple[bytes, bytes]]:
    messages: list[tuple[bytes, bytes]] = []
    while True:
        message = recv_message(sock)
        messages.append(message)
        if message[0] == b"Z":
            return messages
