"""The append workload's operations, in the form elle-cli reads."""

import json
import threading
import time
from pathlib import Path
from typing import IO

from .workload import Append


def unread(value: list[list]) -> list[list]:
    """A transaction's micro-operations with read results unknown."""
    return [[f, key, None] if f == "r" else [f, key, arg] for f, key, arg in value]


# Errors raised before the block could have run. Anything else leaves the
# outcome unknown, and recording a committed append as failed would make Elle
# report an aborted read.
FAILED_BEFORE_EXECUTION = (
    "the connection is closed",
    "sending query and params failed",
    "DML was sent to the replica during master switchover",
    "instance is read-only: ",
)


def outcome_of(error: Exception) -> str:
    text = str(error)
    return "fail" if any(marker in text for marker in FAILED_BEFORE_EXECUTION) else "info"


class History:
    """The operations streamed as the JSON array elle-cli reads.

    A block's invoke and completion are written together once it completes:
    a serializable check only needs each invoke before its completion, and it
    keeps the lock off the network call. Blocks still running when the
    workload stops are written as info and whatever they return later is
    dropped, so nothing lands after the final read.
    """

    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.Lock()
        self.file: IO[str] = path.open("w")
        self.file.write("[")
        self.count = 0
        self.in_flight: dict[int, dict] = {}
        self.accepting = True

    def _write(self, record: dict):
        self.file.write(("\n" if self.count == 0 else ",\n") + json.dumps(record))
        self.count += 1

    def size(self) -> int:
        with self.lock:
            return self.count

    def begin(self, process: int, op: Append, node: str) -> bool:
        with self.lock:
            if not self.accepting:
                return False
            self.in_flight[process] = {
                "type": "invoke",
                "f": "txn",
                "process": process,
                "time": time.monotonic_ns(),
                "value": op.invoke_value(),
                "node": node,
                "params": list(op.params),
            }
            return True

    def finish(self, process: int, outcome: str, value: list[list]):
        with self.lock:
            invoke = self.in_flight.pop(process, None)
            if invoke is None:
                return
            self._write(invoke)
            self._write({**invoke, "type": outcome, "time": time.monotonic_ns(), "value": value})

    def stop_workers(self):
        with self.lock:
            self.accepting = False
            for invoke in self.in_flight.values():
                self._write(invoke)
                self._write({**invoke, "type": "info", "time": time.monotonic_ns(), "value": unread(invoke["value"])})
            self.in_flight.clear()

    def record_read(self, process: int, value: list[list], node: str):
        """A read made by the harness itself, after the workers are gone."""
        with self.lock:
            invoke = {"type": "invoke", "f": "txn", "process": process, "time": time.monotonic_ns()}
            invoke |= {"value": unread(value), "node": node, "params": []}
            self._write(invoke)
            self._write({**invoke, "type": "ok", "time": time.monotonic_ns(), "value": value})

    def close(self):
        with self.lock:
            if self.file.closed:
                return
            self.file.write("\n]\n")
            self.file.close()
