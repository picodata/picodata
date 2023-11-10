import io
import json
import os
import re
import socket
import sys
import time
import threading
from types import SimpleNamespace

import yaml  # type: ignore
import pytest
import signal
import subprocess
import msgpack  # type: ignore
from rand.params import generate_seed
from functools import reduce
from datetime import datetime
from shutil import rmtree
from typing import Any, Callable, Literal, Generator, Iterator, Dict, List, Tuple, Type
from itertools import count
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from tarantool.connection import Connection  # type: ignore
from tarantool.error import (  # type: ignore
    tnt_strerror,
    DatabaseError,
)

# From raft.rs:
# A constant represents invalid id of raft.
# pub const INVALID_ID: u64 = 0;
INVALID_RAFT_ID = 0
BASE_HOST = "127.0.0.1"
BASE_PORT = 3300
PORT_RANGE = 200


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def pytest_addoption(parser: pytest.Parser):
    parser.addoption(
        "--seed", action="store", default=None, help="Seed for randomized tests"
    )
    parser.addoption(
        "--delay",
        action="store",
        default=None,
        help="Delay between steps for randomized tests",
    )
    parser.addoption(
        "--with-flamegraph",
        action="store_true",
        default=False,
        help="Whether gather flamegraphs or not (for benchmarks only)",
    )


@pytest.fixture(scope="session")
def port_range(xdist_worker_number: int) -> tuple[int, int]:
    """
    Return pair (base_port, max_port) available for current pytest subprocess.
    Ensures that all ports in this range are not in use.
    Executes once due scope="session".

    Note: this function has a side-effect.
    """

    assert isinstance(xdist_worker_number, int)
    assert xdist_worker_number >= 0
    base_port = BASE_PORT + xdist_worker_number * PORT_RANGE

    max_port = base_port + PORT_RANGE - 1
    assert max_port <= 65535

    for port in range(base_port, max_port + 1):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((BASE_HOST, port))
        s.close()

    return (base_port, max_port)


@pytest.fixture(scope="session")
def seed(pytestconfig):
    """Return a seed for randomized tests. Unless passed via
    command-line options it is generated automatically.
    """
    seed = pytestconfig.getoption("seed")
    return seed if seed else generate_seed()


@pytest.fixture(scope="session")
def delay(pytestconfig):
    return pytestconfig.getoption("delay")


@pytest.fixture(scope="session")
def with_flamegraph(pytestconfig):
    return bool(pytestconfig.getoption("with_flamegraph"))


@pytest.fixture(scope="session")
def xdist_worker_number(worker_id: str) -> int:
    """
    Identify xdist worker by an integer instead of a string.
    This is used for parallel testing.
    See also: https://pypi.org/project/pytest-xdist/
    """

    if worker_id == "master":
        return 0

    match = re.fullmatch(r"gw(\d+)", worker_id)
    assert match, f"unexpected worker id: {worker_id}"

    return int(match.group(1))


class TarantoolError(Exception):
    """
    Raised when Tarantool responds with an IPROTO_ERROR.
    """

    pass


class ReturnError(Exception):
    """
    Raised when Tarantool returns `nil, err`.
    """

    pass


class MalformedAPI(Exception):
    """
    Raised when Tarantool returns some data (IPROTO_OK),
    but it's neither `return value` nor `return nil, err`.

    The actual returned data is contained in `self.args`.
    """

    pass


def normalize_net_box_result(func):
    """
    Convert lua-style responses to be more python-like.
    This also fixes some of the connector API inconveniences.
    """

    def inner(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except DatabaseError as exc:
            if getattr(exc, "errno", None):
                # Error handling in Tarantool connector is awful.
                # It wraps NetworkError in DatabaseError.
                # We, instead, convert it to a native OSError.
                strerror = os.strerror(exc.errno)
                raise OSError(exc.errno, strerror) from exc

            match exc.args:
                case (int(code), arg):
                    # Error handling in Tarantool connector is awful.
                    # It returns error codes as raw numbers.
                    # Here we format them for easier use in pytest assertions.
                    raise TarantoolError(tnt_strerror(code)[0], arg) from exc
                case _:
                    raise exc from exc

        match result.data:
            case []:
                return None
            case [x] | [x, None]:
                return x
            case [None, err]:
                raise ReturnError(err)
            case [*args]:
                raise MalformedAPI(*args)
            case _:
                raise RuntimeError("unreachable")

    return inner


@dataclass
class KeyPart:
    fieldno: int
    type: str
    is_nullable: bool = False

    def __str__(self):
        return """{{ fieldno = {}, type = "{}", is_nullable = {} }}""".format(
            self.fieldno, self.type, self.is_nullable
        )


@dataclass
class KeyDef:
    parts: list[KeyPart]

    def __str__(self):
        parts = ", ".join(str(part) for part in self.parts)
        return """{{ {} }}""".format(parts)


@dataclass(frozen=True)
class RaftStatus:
    id: int
    raft_state: str
    term: int
    main_loop_status: str
    leader_id: int | None = None


class CasRange:
    key_min = dict(kind="unbounded", key=None)
    key_max = dict(kind="unbounded", key=None)
    repr_min = "unbounded"
    repr_max = "unbounded"

    @property
    def key_min_packed(self) -> dict:
        key = self.key_min.copy()
        key["key"] = msgpack.packb([key["key"][0]])  # type: ignore
        return key

    @property
    def key_max_packed(self) -> dict:
        key = self.key_max.copy()
        key["key"] = msgpack.packb([key["key"][0]])  # type: ignore
        return key

    def __repr__(self):
        return f"CasRange({self.repr_min}, {self.repr_max})"

    def __init__(self, gt=None, ge=None, lt=None, le=None, eq=None):
        """
        Creates a CasRange from the specified bounds.

        To specify a range for exactly one key use only: `eq`.
        Example: `CasRange(eq=1) # [1,1]`

        To specify a range between lower and upper bound use:
        1. `gt` - greater then - an exclusive lower bound
        2. `ge` - greater or equal - an inclusive lower bound
        3. `lt` - less then - an exclusive upper bound
        4. `le` - less or equal - an inclusive upper bound
        Example: `CasRange(ge=1, lt=3) # [1,3)`

        If only one lower or upper bound is specified, the other bound will be assumed `unbounded`.
        Example: `CasRange(ge=1) # [1, +infinity)`
        """
        if gt is not None:
            self.key_min = dict(kind="excluded", key=(gt,))
            self.repr_min = f'gt="{gt}"'
        if ge is not None:
            self.key_min = dict(kind="included", key=(ge,))
            self.repr_min = f'ge="{ge}"'

        if lt is not None:
            self.key_max = dict(kind="excluded", key=(lt,))
            self.repr_max = f'lt="{lt}"'
        if le is not None:
            self.key_max = dict(kind="included", key=(le,))
            self.repr_max = f'le="{le}"'
        if eq is not None:
            self.key_min = dict(kind="included", key=(eq,))
            self.key_max = dict(kind="included", key=(eq,))
            self.repr_min = f'ge="{eq}"'
            self.repr_max = f'le="{eq}"'


color = SimpleNamespace(
    **{
        f"{prefix}{color}": f"\033[{ansi_color_code}{ansi_effect_code}m{{0}}\033[0m".format
        for color, ansi_color_code in {
            "grey": 30,
            "red": 31,
            "green": 32,
            "yellow": 33,
            "blue": 34,
            "magenta": 35,
            "cyan": 36,
            "white": 37,
        }.items()
        for prefix, ansi_effect_code in {
            "": "",
            "intense_": ";1",
        }.items()
    }
)
# Usage:
assert color.green("text") == "\x1b[32mtext\x1b[0m"
assert color.intense_red("text") == "\x1b[31;1mtext\x1b[0m"


class Retriable:
    """A utility class for handling retries.

    Example:

        def guess_fizzbuzz():
            x = random.randint(1, 20)
            assert x % 3 == 0
            assert x % 5 == 0
            return x

        fizzuzz = Retriable(timeout=3, rps=5).call(guess_fizzbuzz)

    It calls `guess_fizzbuzz` function repeatedly during 3 seconds until
    it succeeds to find a random number that satisfies criteria. The
    calling rate is limited by 5 times a second.

    By default it suppresses and retries all exceptions subclassed
    from `Exception` type, except those specified in `fatal` parameter.
    """

    def __init__(
        self,
        timeout: int | float,
        rps: int | float,
        fatal: Type[Exception] | Tuple[Exception, ...] = (),
    ) -> None:
        """
        Build the retriable call context

        Args:
            timeout (int | float): total time limit.
            rps (int | float): retries per second.
            fatal (Exception | Tuple[Exception, ...], default=()):
                unsuppressed exception class or a tuple of classes
                that should never be retried.
        """

        now = time.monotonic()
        self.deadline = now + timeout
        self.retry_period = 1 / rps
        self.next_retry = now
        self.fatal = fatal

    def call(self, func, *args, **kwargs):
        """
        Calls a function repeatedly until it succeeds
        or timeout expires.
        """
        while self._next_try():
            try:
                return func(*args, **kwargs)
            except self.fatal as e:
                raise e from e
            except Exception as e:
                self._suppress(e)
                continue

    def _next_try(self) -> bool:
        """
        Suspend execution until it's time to make the next attempt.

        Raises:
            AssertionError: if timeout expired. This usually shouldn't
            be a case as `next_try` usually preceeds with `suppress`
            which does the same check.
        """

        now = time.monotonic()
        assert now <= self.deadline, "timeout"

        if now < self.next_retry:
            time.sleep(self.next_retry - now)
            now = time.monotonic()

        self.next_retry = now + self.retry_period
        return True

    def _suppress(self, e: Exception) -> None:
        """
        Suppress the exception unless timeout expired. Raises
        the same exception if timout did expire.
        """
        now = time.monotonic()
        if now > self.deadline:
            raise e from e


OUT_LOCK = threading.Lock()

POSITION_IN_SPACE_INSTANCE_ID = 3


@dataclass
class Instance:
    binary_path: str
    cluster_id: str
    data_dir: str
    peers: list[str]
    host: str
    port: int

    color: Callable[[str], str]

    tier: str | None = None
    init_replication_factor: int | None = None
    init_cfg_path: str | None = None
    instance_id: str | None = None
    replicaset_id: str | None = None
    failure_domain: dict[str, str] = field(default_factory=dict)
    env: dict[str, str] = field(default_factory=dict)
    process: subprocess.Popen | None = None
    raft_id: int = INVALID_RAFT_ID
    _on_output_callbacks: list[Callable[[str], None]] = field(default_factory=list)

    @property
    def listen(self):
        return f"{self.host}:{self.port}"

    def current_grade(self, instance_id=None):
        if instance_id is None:
            instance_id = self.instance_id
        return self.call("pico.instance_info", instance_id)["current_grade"]

    def instance_uuid(self):
        return self.eval("return box.info.uuid")

    def replicaset_uuid(self):
        return self.eval("return box.info.cluster.uuid")

    @property
    def command(self):
        # fmt: off
        return [
            self.binary_path, "run",
            "--cluster-id", self.cluster_id,
            *([f"--instance-id={self.instance_id}"] if self.instance_id else []),
            *([f"--replicaset-id={self.replicaset_id}"] if self.replicaset_id else []),
            "--data-dir", self.data_dir,
            "--listen", self.listen,
            "--peer", ','.join(self.peers),
            *(f"--failure-domain={k}={v}" for k, v in self.failure_domain.items()),
            *(["--init-replication-factor", f"{self.init_replication_factor}"]
              if self.init_replication_factor is not None else []),
            *(["--init-cfg", self.init_cfg_path]
              if self.init_cfg_path is not None else []),
            *(["--tier", self.tier] if self.tier is not None else []),
        ]
        # fmt: on

    def __repr__(self):
        return f"Instance({self.instance_id}, listen={self.listen})"

    @contextmanager
    def connect(
        self, timeout: int | float, user: str | None = None, password: str | None = None
    ):
        c = Connection(
            self.host,
            self.port,
            user=user,
            password=password,
            socket_timeout=timeout,
            connection_timeout=timeout,
            connect_now=True,
            fetch_schema=False,
        )
        try:
            yield c
        finally:
            c.close()

    @normalize_net_box_result
    def call(
        self,
        fn,
        *args,
        user: str | None = None,
        password: str | None = None,
        timeout: int | float = 1,
    ):
        with self.connect(timeout, user=user, password=password) as conn:
            return conn.call(fn, args)

    @normalize_net_box_result
    def eval(
        self,
        expr,
        *args,
        user: str | None = None,
        password: str | None = None,
        timeout: int | float = 1,
    ):
        with self.connect(timeout, user=user, password=password) as conn:
            return conn.eval(expr, *args)

    def kill(self):
        """Kill the instance brutally with SIGKILL"""
        if self.process is None:
            # Be idempotent
            return

        pid = self.process.pid
        with suppress(ProcessLookupError, PermissionError):
            os.killpg(pid, signal.SIGKILL)
            with suppress(ChildProcessError):
                os.waitpid(pid, 0)
            eprint(f"{self} killed")
        self.process = None

    def hash(self, tup: tuple, key_def: KeyDef) -> int:
        tup_str = "{{ {} }}".format(", ".join(str(x) for x in tup))
        lua = """
            return require("key_def").new({kd}):hash(box.tuple.new({t}))
        """.format(
            t=tup_str, kd=str(key_def)
        )
        return self.eval(lua)

    def sql(self, sql: str, *params, timeout: int | float = 1) -> dict:
        """Run SQL query and return result"""
        return self.call("pico.sql", sql, params, timeout=timeout)

    def terminate(self, kill_after_seconds=10) -> int | None:
        """Terminate the instance gracefully with SIGTERM"""
        if self.process is None:
            # Be idempotent
            return None

        with suppress(ProcessLookupError, PermissionError):
            os.killpg(self.process.pid, signal.SIGCONT)

        self.process.terminate()

        try:
            rc = self.process.wait(timeout=kill_after_seconds)
            eprint(f"{self} terminated: rc = {rc}")
            self.process = None
            return rc
        finally:
            self.kill()

    def _process_output(self, src, out):
        id = self.instance_id or f":{self.port}"
        prefix = f"{id:<3} | "

        if sys.stdout.isatty():
            prefix = self.color(prefix)

        for line in io.TextIOWrapper(src, line_buffering=True):
            with OUT_LOCK:
                out.write(prefix)
                out.write(line)
                out.flush()
                for cb in self._on_output_callbacks:
                    cb(line)

    def on_output_line(self, cb: Callable[[str], None]):
        self._on_output_callbacks.append(cb)

    def start(self, peers=[]):
        if self.process:
            # Be idempotent
            return

        eprint(f"{self} starting...")

        if peers != []:
            self.peers = map(lambda i: i.listen, peers)

        env = self.env
        if not os.environ.get("PICODATA_LOG_LEVEL") and "PICODATA_LOG_LEVEL" not in env:
            env.update(PICODATA_LOG_LEVEL="verbose")

        if os.environ.get("RUST_BACKTRACE") is not None:
            env.update(RUST_BACKTRACE=str(os.environ.get("RUST_BACKTRACE")))

        self.process = subprocess.Popen(
            self.command,
            env=env or None,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            # Picodata instance consists of two processes: a supervisor
            # and a child. Pytest manages it at the level of linux
            # process groups that is a collection of related processes
            # which can all be signalled at once.
            #
            # According to `man 2 kill`:
            # > If `pid` is less than -1, then `sig` is sent to every
            # > process in the process group whose ID is `-pid`.
            #
            # For `os.killpg()` to work properly (killing this
            # particular instance and not others) we want each instance
            # to form it's own process group. The easiest way to
            # accomplish it is to start a new session. With this flag
            # pytest implicitly calls `setsid()`.
            start_new_session=True,
        )

        for src, out in [
            (self.process.stdout, sys.stdout),
            (self.process.stderr, sys.stderr),
        ]:
            threading.Thread(
                target=self._process_output,
                args=(src, out),
                daemon=True,
            ).start()

        # Assert a new process group is created
        assert os.getpgid(self.process.pid) == self.process.pid

    def fail_to_start(self, timeout: int = 5):
        assert self.process is None
        self.start()
        assert self.process
        try:
            rc = self.process.wait(timeout)

            # Wait for all the output to be handled in the separate threads
            while not self.process.stdout.closed or not self.process.stderr.closed:  # type: ignore
                time.sleep(0.1)

            self.process = None
            assert rc != 0
        except Exception as e:
            self.kill()
            raise e from e

    def restart(self, kill: bool = False, remove_data: bool = False):
        if kill:
            self.kill()
        else:
            self.terminate()

        if remove_data:
            self.remove_data()

        self.start()

    def remove_data(self):
        rmtree(self.data_dir)

    def _raft_status(self) -> RaftStatus:
        status = self.call("pico.raft_status")
        assert isinstance(status, dict)
        return RaftStatus(**status)

    def raft_propose_nop(self):
        return self.call("pico.raft_propose_nop")

    def raft_compact_log(self, up_to: int = 2**64 - 1) -> int:
        """
        Trim raft log up to the given index (excluding the index
        itself). By default all entries are compacted so the log
        remains empty.

        Return the new first_index after compaction.
        """
        return self.call("pico.raft_compact_log", up_to)

    def raft_first_index(self) -> int:
        """
        Return the raft first_index value that is `(compacted_index or 0) + 1`.
        """

        return self.call("pico.raft_compact_log", 0)

    def space_id(self, space: str | int) -> int:
        """
        Get space id by space name.
        If id is supplied instead it is just returned back.

        This method is useful in functions which can take both id and space name.
        """
        match space:
            case int():
                return space
            case str():
                return self.eval("return box.space[...].id", space)
            case _:
                raise TypeError("space must be str or int")

    def cas(
        self,
        op_kind: Literal["insert", "replace", "delete"],
        table: str | int,
        tuple: Tuple | List | None = None,
        index: int | None = None,
        term: int | None = None,
        ranges: List[CasRange] | None = None,
    ) -> int:
        """
        Performs a clusterwide compare and swap operation.

        E.g. it checks the `predicate` on leader and if no conflicting entries were found
        appends the `op` to the raft log and returns its index.

        ASSUMPTIONS
        It is assumed that this operation is called on leader.
        Failing to do so will result in an error.
        """
        if index is None:
            index = self.raft_read_index()
            term = self.raft_term_by_index(index)
        elif term is None:
            term = self.raft_term_by_index(index)

        table_id = self.space_id(table)

        predicate_ranges = []
        if ranges is not None:
            for range in ranges:
                predicate_ranges.append(
                    dict(
                        table=table_id,
                        key_min=range.key_min_packed,
                        key_max=range.key_max_packed,
                    )
                )

        predicate = dict(
            index=index,
            term=term,
            ranges=predicate_ranges,
        )

        if op_kind in ["insert", "replace"]:
            op = dict(
                kind="dml",
                op_kind=op_kind,
                table=table_id,
                tuple=msgpack.packb(tuple),
            )
        elif op_kind == "delete":
            op = dict(
                kind="dml",
                op_kind=op_kind,
                table=table_id,
                key=msgpack.packb(tuple),
            )
        else:
            raise Exception(f"unsupported {op_kind=}")

        # guest has super privs for now by default this should be equal
        # to ADMIN_USER_ID on the rust side
        as_user = 1

        eprint(f"CaS:\n  {predicate=}\n  {op=}")
        return self.call(".proc_cas", self.cluster_id, predicate, op, as_user)[0][
            "index"
        ]

    def pico_property(self, key: str):
        tup = self.call("box.space._pico_property:get", key)
        if tup is None:
            return None

        return tup[1]

    def next_schema_version(self) -> int:
        return self.pico_property("next_schema_version") or 1

    def create_table(self, params: dict, timeout: float = 3.0) -> int:
        """
        Creates a space. Returns a raft index at which a newly created space
        has to exist on all peers.

        Works through Lua API in difference to `propose_create_space`,
        which is more low level and directly proposes a raft entry.
        """
        params["timeout"] = timeout
        index = self.call("pico.create_table", params, timeout, timeout=timeout + 0.5)
        return index

    def drop_space(self, space: int | str, timeout: float = 3.0):
        """
        Drops the space. Returns a raft index at which the space has to be
        dropped on all peers.
        """
        index = self.call(
            "pico.drop_table", space, dict(timeout=timeout), timeout=timeout + 0.5
        )
        return index

    def abort_ddl(self, timeout: float = 3.0) -> int:
        """
        Aborts a pending DDL operation and waits for abort to be committed localy.
        If `timeout` is reached earlier returns an error.

        Returns an index of the corresponding DdlAbort raft entry, or an error if
        there is no pending DDL operation.
        """
        index = self.call("pico.abort_ddl", timeout, timeout=timeout + 0.5)
        return index

    def propose_create_space(
        self, space_def: Dict[str, Any], wait_index: bool = True, timeout: int = 3
    ) -> int:
        """
        Proposes a space creation ddl prepare operation. Returns the index of
        the corresponding finilazing ddl commit or ddl abort entry.

        If `wait_index` is `True` will wait for the finilazing entry
        to be applied for `timeout` seconds.
        """
        op = dict(
            kind="ddl_prepare",
            schema_version=self.next_schema_version(),
            ddl=dict(kind="create_space", **space_def),
        )
        # TODO: rewrite the test using pico.cas
        index = self.call("pico.raft_propose", op, timeout=timeout)
        # Index of the corresponding ddl abort / ddl commit entry
        index_fin = index + 1
        if wait_index:
            self.raft_wait_index(index_fin, timeout)

        return index_fin

    def assert_raft_status(self, state, leader_id=None):
        status = self._raft_status()

        if leader_id is None:
            leader_id = status.leader_id

        assert {
            "raft_state": status.raft_state,
            "leader_id": status.leader_id,
        } == {"raft_state": state, "leader_id": leader_id}

    def wait_online(
        self, timeout: int | float = 6, rps: int | float = 5, expected_incarnation=None
    ):
        """Wait until instance attains Online grade

        Args:
            timeout (int | float, default=6): total time limit
            rps (int | float, default=5): retries per second

        Raises:
            AssertionError: if doesn't succeed
        """

        class ProcessDead(Exception):
            pass

        if self.process is None:
            raise ProcessDead("process was not started")

        def fetch_info():
            try:
                exit_code = self.process.wait(timeout=0)
            except subprocess.TimeoutExpired:
                # it's fine, the process is still running
                pass
            else:
                raise ProcessDead(f"process exited unexpectedly, {exit_code=}")

            whoami = self.call("pico.whoami")
            assert isinstance(whoami, dict)
            assert isinstance(whoami["raft_id"], int)
            assert isinstance(whoami["instance_id"], str)
            self.raft_id = whoami["raft_id"]
            self.instance_id = whoami["instance_id"]

            myself = self.call("pico.instance_info", self.instance_id)
            assert isinstance(myself, dict)
            assert isinstance(myself["current_grade"], dict)
            assert myself["current_grade"]["variant"] == "Online"
            if expected_incarnation is not None:
                assert myself["current_grade"]["incarnation"] == expected_incarnation

        Retriable(timeout, rps, fatal=ProcessDead).call(fetch_info)
        eprint(f"{self} is online")

    def raft_term(self) -> int:
        """Get current raft `term`"""

        return self.eval("return box.space._raft_state:get('term').value")

    def raft_term_by_index(self, index: int) -> int:
        """Get raft `term` for entry with supplied `index`"""
        term = self.eval(
            """
            local entry = box.space._raft_log:get(...)
            if entry then
                return entry.term
            else
                return box.space._raft_state:get('term').value
            end
            """,
            index,
        )
        return term

    def raft_get_index(self) -> int:
        """Get current applied raft index"""
        return self.call("pico.raft_get_index")

    def raft_read_index(self, timeout: int | float = 1) -> int:
        """
        Perform the quorum read operation.
        See `crate::traft::node::Node::read_index`.
        """

        return self.call(
            "pico.raft_read_index",
            timeout,  # this timeout is passed as an argument
            timeout=timeout + 1,  # this timeout is for network call
        )

    def raft_wait_index(self, target: int, timeout: int | float = 1) -> int:
        """
        Wait until instance applies the `target` raft index.
        See `crate::traft::node::Node::wait_index`.
        """

        return self.call(
            "pico.raft_wait_index",
            target,
            timeout,  # this timeout is passed as an argument
            timeout=timeout + 1,  # this timeout is for network call
        )

    def get_vclock(self) -> int:
        """Get current vclock"""

        return self.call("pico.get_vclock")

    def wait_vclock(self, target: int, timeout: int | float = 1) -> int:
        """
        Wait until Tarantool vclock reaches the `target`. Returns the
        actual vclock. It can be equal to or greater than the target one.

        Raises:
            TarantoolError: on timeout
        """

        return self.call(
            "pico.wait_vclock",
            target,
            timeout,  # this timeout is passed as an argument
            timeout=timeout + 1,  # this timeout is for network call
        )

    def promote_or_fail(self):
        attempt = 0

        def make_attempt(timeout, rps):
            nonlocal attempt
            attempt = attempt + 1
            eprint(f"{self} is trying to become a leader, {attempt=}")

            # 1. Force the node to campaign.
            self.call("pico.raft_timeout_now")

            # 2. Wait until the miracle occurs.
            Retriable(timeout, rps).call(self.assert_raft_status, "Leader")

        Retriable(timeout=3, rps=1).call(make_attempt, timeout=1, rps=10)
        eprint(f"{self} is a leader now")


CLUSTER_COLORS = (
    color.cyan,
    color.yellow,
    color.green,
    color.magenta,
    color.blue,
    color.intense_cyan,
    color.intense_yellow,
    color.intense_green,
    color.intense_magenta,
    color.intense_blue,
)


@dataclass
class Cluster:
    binary_path: str
    id: str
    data_dir: str
    base_host: str
    base_port: int
    max_port: int
    instances: list[Instance] = field(default_factory=list)
    cfg_path: str | None = None

    def __repr__(self):
        return f'Cluster("{self.base_host}:{self.base_port}", n={len(self.instances)})'

    def __getitem__(self, item: int) -> Instance:
        return self.instances[item]

    def deploy(
        self,
        *,
        instance_count: int,
        init_replication_factor: int | None = None,
        tier: str | None = None,
    ) -> list[Instance]:
        assert not self.instances, "Already deployed"

        for _ in range(instance_count):
            self.add_instance(
                wait_online=False,
                tier=tier,
                init_replication_factor=init_replication_factor,
            )

        for instance in self.instances:
            instance.start()

        for instance in self.instances:
            instance.wait_online()

        eprint(f" {self} deployed ".center(80, "="))
        return self.instances

    def set_init_cfg(self, cfg: dict):
        assert self.cfg_path is None
        self.cfg_path = self.data_dir + "/tier.yaml"
        with open(self.cfg_path, "w") as yaml_file:
            dump = yaml.dump(cfg, default_flow_style=False)
            yaml_file.write(dump)

    def add_instance(
        self,
        wait_online=True,
        peers: list[str] | None = None,
        instance_id: str | bool = True,
        replicaset_id: str | None = None,
        failure_domain=dict(),
        init_replication_factor: int | None = None,
        tier: str | None = None,
    ) -> Instance:
        """Add an `Instance` into the list of instances of the cluster and wait
        for it to attain Online grade unless `wait_online` is `False`.

        `instance_id` specifies how the instance's id is generated in the
        following way:

        - if `instance_id` is a string, it will be used as a value for the
          `--instance-id` command-line option.

        - If `instance_id` is `True` (default), the `--instance-id` command-line
          option will be generated by the pytest according to the instances
          sequence number in cluster.

        - If `instance_id` is `False`, the instance will be started
          without the `--instance-id` command-line option and the particular value
          will be generated by the cluster.
        """
        i = 1 + len(self.instances)

        generated_instance_id: str | None
        match instance_id:
            case str() as iid:
                generated_instance_id = iid
            case True:
                generated_instance_id = f"i{i}"
            case False:
                generated_instance_id = None
            case _:
                raise Exception("unreachable")

        port = self.base_port + i
        assert self.base_port <= port <= self.max_port

        instance = Instance(
            binary_path=self.binary_path,
            cluster_id=self.id,
            instance_id=generated_instance_id,
            replicaset_id=replicaset_id,
            data_dir=f"{self.data_dir}/i{i}",
            host=self.base_host,
            port=port,
            peers=peers or [f"{self.base_host}:{self.base_port + 1}"],
            color=CLUSTER_COLORS[len(self.instances) % len(CLUSTER_COLORS)],
            failure_domain=failure_domain,
            init_replication_factor=init_replication_factor,
            tier=tier,
            init_cfg_path=self.cfg_path,
        )

        self.instances.append(instance)

        if wait_online:
            instance.start()
            instance.wait_online()

        return instance

    def fail_to_add_instance(
        self,
        peers=None,
        instance_id: str | bool = True,
        failure_domain=dict(),
        init_replication_factor: int | None = None,
        tier: str = "storage",
    ):
        instance = self.add_instance(
            wait_online=False,
            peers=peers,
            instance_id=instance_id,
            failure_domain=failure_domain,
            init_replication_factor=init_replication_factor,
            tier=tier,
        )
        self.instances.remove(instance)
        instance.fail_to_start()

    def kill(self):
        for instance in self.instances:
            instance.kill()

    def terminate(self):
        errors = []
        for instance in self.instances:
            try:
                instance.terminate()
            except Exception as e:
                errors.append(e)
        if errors:
            raise Exception(errors)

    def remove_data(self):
        rmtree(self.data_dir)

    def expel(self, target: Instance, peer: Instance | None = None):
        peer = peer if peer else target

        # fmt: off
        command = [
            self.binary_path, "expel",
            "--peer", peer.listen,
            "--cluster-id", target.cluster_id,
            "--instance-id", target.instance_id,
        ]
        # fmt: on

        subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def raft_wait_index(self, index: int, timeout: float = 3.0):
        """
        Waits for all peers to commit an entry with index `index`.
        """
        import time

        deadline = time.time() + timeout
        for instance in self.instances:
            if instance.process is not None:
                timeout = deadline - time.time()
                if timeout < 0:
                    timeout = 0
                instance.raft_wait_index(index, timeout)

    def create_table(self, params: dict, timeout: float = 3.0):
        """
        Creates a space. Waits for all online peers to be aware of it.
        """
        index = self.instances[0].create_table(params, timeout)
        self.raft_wait_index(index, timeout)

    def drop_space(self, space: int | str, timeout: float = 3.0):
        """
        Drops the space. Waits for all online peers to be aware of it.
        """
        index = self.instances[0].drop_space(space, timeout)
        self.raft_wait_index(index, timeout)

    def abort_ddl(self, timeout: float = 3.0):
        """
        Aborts a pending ddl. Waits for all peers to be aware of it.
        """
        index = self.instances[0].abort_ddl(timeout)
        self.raft_wait_index(index, timeout)

    def cas(
        self,
        dml_kind: Literal["insert", "replace", "delete"],
        table: str,
        tuple: Tuple | List,
        index: int | None = None,
        term: int | None = None,
        ranges: List[CasRange] | None = None,
        # If specified send CaS through this instance
        instance: Instance | None = None,
        user: str | None = None,
        password: str | None = None,
    ) -> int:
        """
        Performs a clusterwide compare and swap operation.

        E.g. it checks the `predicate` on leader and if no conflicting entries were found
        appends the `op` to the raft log and returns its index.

        Calling this operation will route CaS request to a leader.
        """
        if instance is None:
            instance = self.instances[0]

        predicate_ranges = []
        if ranges is not None:
            for range in ranges:
                predicate_ranges.append(
                    dict(
                        table=table,
                        key_min=range.key_min,
                        key_max=range.key_max,
                    )
                )

        predicate = dict(
            index=index,
            term=term,
            ranges=predicate_ranges,
        )
        if dml_kind in ["insert", "replace", "delete"]:
            dml = dict(
                table=table,
                kind=dml_kind,
                tuple=tuple,
            )
        else:
            raise Exception(f"unsupported {dml_kind=}")

        eprint(f"CaS:\n  {predicate=}\n  {dml=}")
        return instance.call("pico.cas", dml, predicate, user=user, password=password)


@dataclass
class PortalStorage:
    instance: Instance

    @property
    def descriptors(self):
        return self.instance.call("pico.pg_portals")

    def bind(self, *params):
        return self.instance.call("pico.pg_bind", *params, False)

    def close(self, descriptor: int):
        return self.instance.call("pico.pg_close", descriptor)

    def describe(self, descriptor: int) -> dict:
        return self.instance.call("pico.pg_describe", descriptor, False)

    def execute(self, descriptor: int) -> dict:
        return self.instance.call("pico.pg_execute", descriptor, False)

    def flush(self):
        for descriptor in self.descriptors["available"]:
            self.close(descriptor)

    def parse(self, sql: str) -> int:
        return self.instance.call("pico.pg_parse", sql, False)


@pytest.fixture(scope="session")
def binary_path() -> str:
    """Path to the picodata binary, e.g. "./target/debug/picodata"."""
    assert subprocess.call(["cargo", "build"]) == 0, "cargo build failed"
    metadata = subprocess.check_output(["cargo", "metadata", "--format-version=1"])
    target = json.loads(metadata)["target_directory"]
    return os.path.realpath(os.path.join(target, "debug/picodata"))


@pytest.fixture(scope="session")
def path_to_binary_with_webui() -> str:
    """Path to the picodata binary built with webui feature, e.g. "./target/debug/picodata"."""
    assert (
        subprocess.call(["cargo", "build", "--features", "webui"]) == 0
    ), "cargo build failed"
    metadata = subprocess.check_output(["cargo", "metadata", "--format-version=1"])
    target = json.loads(metadata)["target_directory"]
    return os.path.realpath(os.path.join(target, "debug/picodata"))


@pytest.fixture(scope="session")
def cluster_ids(xdist_worker_number) -> Iterator[str]:
    """Unique `clister_id` generator."""
    return (f"cluster-{xdist_worker_number}-{i}" for i in count())


@pytest.fixture
def cluster(
    binary_path, tmpdir, cluster_ids, port_range
) -> Generator[Cluster, None, None]:
    """Return a `Cluster` object capable of deploying test clusters."""
    base_port, max_port = port_range
    cluster = Cluster(
        binary_path=binary_path,
        id=next(cluster_ids),
        data_dir=tmpdir,
        base_host=BASE_HOST,
        base_port=base_port,
        max_port=max_port,
    )
    yield cluster
    cluster.kill()


@pytest.fixture
def cluster_with_webui(
    path_to_binary_with_webui, tmpdir, cluster_ids, port_range
) -> Generator[Cluster, None, None]:
    """Return a `Cluster` object capable of deploying test clusters with webui feature enabled."""
    base_port, max_port = port_range
    cluster = Cluster(
        binary_path=path_to_binary_with_webui,
        id=next(cluster_ids),
        data_dir=tmpdir,
        base_host=BASE_HOST,
        base_port=base_port,
        max_port=max_port,
    )
    yield cluster
    cluster.kill()


@pytest.fixture
def instance(cluster: Cluster) -> Generator[Instance, None, None]:
    """Returns a deployed instance forming a single-node cluster."""
    cluster.deploy(instance_count=1)
    yield cluster[0]


@pytest.fixture
def pg_portals(instance: Instance) -> Generator[PortalStorage, None, None]:
    """Returns a PG portal storage on a single instance."""
    portals = PortalStorage(instance)
    yield portals
    portals.flush()


def retrying(fn, timeout=3):
    # Usage example:
    #   retrying(lambda: assert(value == 1))
    #   retrying(lambda: assert(value == 1), timeout = 5)
    start = datetime.now()
    while True:
        try:
            return fn()
        except AssertionError as ex:
            if (datetime.now() - start).seconds > timeout:
                raise ex from ex


def pid_alive(pid):
    """Check for the existence of a unix pid."""
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


def pgrep_tree(pid):
    command = f"exec pgrep -P{pid}"
    try:
        ps = subprocess.check_output(command, shell=True)
        ps = ps.strip().split()
        ps = [int(p) for p in ps]
        subps = map(lambda p: pgrep_tree(p), ps)  # list of lists of pids
        subps = reduce(lambda acc, p: [*acc, *p], subps, [])  # list of pids
        return [pid, *subps]
    except subprocess.SubprocessError:
        return [pid]
