from __future__ import annotations

import struct
from typing import TYPE_CHECKING, Any, Tuple

import pg8000.native  # type: ignore
from prometheus_client import Metric

if TYPE_CHECKING:
    # conftest imports this module, so import Instance only for type checks.
    from conftest import Instance


class PgError(Exception):
    """
    Raised when the pgproto server responds with an ErrorResponse.
    """

    pass


def _pg_cstring(data: bytes, pos: int) -> Tuple[str, int]:
    end = data.index(b"\0", pos)
    return data[pos:end].decode(), end + 1


def _pg_counter(metrics: dict[str, Metric], name: str) -> int:
    return int(sum(s.value for family in metrics.values() for s in family.samples if s.name == name))


class PgStorage:
    """
    Statements and portals of pgproto clients on a single instance.
    """

    def __init__(self, instance: Instance):
        self.instance: Instance = instance
        self.user = "admin"
        self.password = "P@ssw0rd"
        self.instance.sql(f"ALTER USER \"{self.user}\" WITH PASSWORD '{self.password}'")
        self.clients: list[PgClient] = []
        self.baseline = self._live_counts()

    def _live_counts(self) -> Tuple[int, int]:
        metrics = self.instance.get_metrics()
        statements = _pg_counter(metrics, "pico_pgproto_statements_opened_total") - _pg_counter(
            metrics, "pico_pgproto_statements_closed_total"
        )
        portals = _pg_counter(metrics, "pico_pgproto_portals_opened_total") - _pg_counter(
            metrics, "pico_pgproto_portals_closed_total"
        )
        return statements, portals

    def counts(self) -> Tuple[int, int]:
        """
        Number of statements and portals of all clients that the test opened.
        """
        statements, portals = self._live_counts()
        return statements - self.baseline[0], portals - self.baseline[1]

    def new_client(self) -> PgClient:
        client = PgClient(self)
        self.clients.append(client)
        return client

    def close(self):
        for client in self.clients:
            client.close()


class PgClient:
    """
    Sends extended query messages one by one and reads the replies.

    Each request ends with Flush and not Sync, because Sync closes all
    portals of the client and tests keep portals between requests.
    """

    def __init__(self, storage: PgStorage):
        self.storage = storage
        instance = storage.instance
        self.conn = pg8000.native.Connection(
            storage.user, password=storage.password, host=instance.pg_host, port=instance.pg_port
        )
        # Fail and do not hang if the server does not reply.
        self.conn._usock.settimeout(instance.default_rpc_timeout)
        self.sock = self.conn._sock

    @property
    def instance(self) -> Instance:
        return self.storage.instance

    def close(self):
        self.conn.close()

    def _send(self, code: bytes, payload: bytes = b""):
        self.sock.write(code + struct.pack("!i", len(payload) + 4) + payload)

    def _read(self) -> Tuple[bytes, bytes]:
        header = self.sock.read(5)
        (length,) = struct.unpack("!i", header[1:])
        return header[:1], self.sock.read(length - 4)

    def _request(self, code: bytes, payload: bytes, last: bytes) -> list[Tuple[bytes, bytes]]:
        """
        Send a message and return the replies up to one with a code from `last`.
        """
        self._send(code, payload)
        self._send(b"H")
        self.sock.flush()

        replies = []
        while True:
            reply, data = self._read()
            if reply == b"E":
                # After an error the server skips messages until Sync.
                self._send(b"S")
                self.sock.flush()
                while self._read()[0] != b"Z":
                    pass
                raise PgError(self._error_message(data))
            if reply == b"N":
                continue
            replies.append((reply, data))
            if reply in last:
                return replies

    @staticmethod
    def _error_message(data: bytes) -> str:
        pos = 0
        while data[pos] != 0:
            field = data[pos : pos + 1]
            value, pos = _pg_cstring(data, pos + 1)
            if field == b"M":
                return value
        return ""

    @staticmethod
    def _columns(reply: bytes, data: bytes) -> list[Tuple[str, int]] | None:
        if reply == b"n":
            return None
        (count,) = struct.unpack_from("!h", data)
        pos, columns = 2, []
        for _ in range(count):
            name, pos = _pg_cstring(data, pos)
            (oid,) = struct.unpack_from("!i", data, pos + 6)
            columns.append((name, oid))
            pos += 18
        return columns

    def parse(self, name: str, sql: str, param_oids: list[int] | None = None):
        param_oids = param_oids if param_oids is not None else []
        payload = name.encode() + b"\0" + sql.encode() + b"\0"
        payload += struct.pack(f"!h{len(param_oids)}i", len(param_oids), *param_oids)
        self._request(b"P", payload, last=b"1")

    def bind(self, stmt: str, portal: str, params: list):
        # No parameter or result format codes: all values are text, and
        # execute decodes only text.
        payload = portal.encode() + b"\0" + stmt.encode() + b"\0" + struct.pack("!hh", 0, len(params))
        for param in params:
            if param is None:
                payload += struct.pack("!i", -1)
            else:
                value = str(param).encode()
                payload += struct.pack("!i", len(value)) + value
        payload += struct.pack("!h", 0)
        self._request(b"B", payload, last=b"2")

    def describe_stmt(self, name: str) -> dict:
        """
        Return parameter oids and `(name, oid)` of the columns, or None for NoData.
        """
        replies = self._request(b"D", b"S" + name.encode() + b"\0", last=b"Tn")
        (params, data), last = replies[0], replies[-1]
        assert params == b"t"
        (count,) = struct.unpack_from("!h", data)
        param_oids = list(struct.unpack_from(f"!{count}i", data, 2))
        return {"param_oids": param_oids, "columns": self._columns(*last)}

    def describe_portal(self, name: str) -> list[Tuple[str, int]] | None:
        return self._columns(*self._request(b"D", b"P" + name.encode() + b"\0", last=b"Tn")[-1])

    def execute(self, portal: str, max_rows: int = 0) -> dict:
        """
        Execute a portal and decode text values of booleans, integers and strings.
        """
        columns = self.describe_portal(portal) or []
        payload = portal.encode() + b"\0" + struct.pack("!i", max_rows)
        replies = self._request(b"E", payload, last=b"CsI")

        rows = []
        for reply, data in replies[:-1]:
            assert reply == b"D"
            pos = 2
            row: list[Any] = []
            for _, oid in columns:
                (length,) = struct.unpack_from("!i", data, pos)
                pos += 4
                if length == -1:
                    row.append(None)
                    continue
                value = data[pos : pos + length]
                pos += length
                if oid == 16:
                    row.append(value == b"t")
                elif oid in (20, 21, 23):
                    row.append(int(value))
                else:
                    row.append(value.decode())
            rows.append(row)

        last, data = replies[-1]
        tag = _pg_cstring(data, 0)[0] if last == b"C" else None
        return {"rows": rows, "tag": tag, "is_finished": last != b"s"}

    def close_stmt(self, name: str):
        self._request(b"C", b"S" + name.encode() + b"\0", last=b"3")

    def close_portal(self, name: str):
        self._request(b"C", b"P" + name.encode() + b"\0", last=b"3")

    def has_statement(self, name: str) -> bool:
        """
        Check if the client has the statement. A missing one closes the
        client's portals, because the error ends with Sync.
        """
        try:
            self.describe_stmt(name)
        except PgError as e:
            assert "Couldn't find statement" in str(e)
            return False
        return True
