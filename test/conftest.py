import io
import json
import os
import re
import filecmp
import shutil
import socket
import sys
import time
import threading
from types import SimpleNamespace
from http.server import BaseHTTPRequestHandler, HTTPServer

import yaml as yaml_lib  # type: ignore
import pytest
import signal
import subprocess
import msgpack  # type: ignore
from rand.params import generate_seed
from functools import reduce
from typing import (
    Any,
    Callable,
    Literal,
    Generator,
    Iterator,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
)
from itertools import count
from contextlib import closing, contextmanager, suppress
from dataclasses import dataclass, field
import tarantool
from tarantool.error import (  # type: ignore
    tnt_strerror,
    DatabaseError,
)
from multiprocessing import Process, Queue
from pathlib import Path


# From raft.rs:
# A constant represents invalid id of raft.
# pub const INVALID_ID: u64 = 0;
INVALID_RAFT_ID = 0
BASE_HOST = "127.0.0.1"
PORT_RANGE = 200
METRICS_PORT = 7500

MAX_LOGIN_ATTEMPTS = 4
PICO_SERVICE_ID = 32

CLI_TIMEOUT = 10  # seconds


# Note: our tarantool.error.tnt_strerror only knows about first 113 error codes..
class ErrorCode:
    Loading = 116
    Other = 10000
    NotALeader = 10001
    StorageCorrupted = 10002
    TermMismatch = 10003
    RaftLogUnavailable = 10004
    RaftLogCompacted = 10005
    CasNoSuchRaftIndex = 10006
    CasConflictFound = 10007
    CasEntryTermMismatch = 10008
    CasTableNotAllowed = 10009
    CasInvalidOpKind = 10010
    NoSuchInstance = 10016
    NoSuchReplicaset = 10017
    LeaderUnknown = 10018
    PluginError = 10019

    # Make sure this matches this list in
    # picodata_plugin::error_code::ErrorCode::is_retriable_for_cas
    retriable_for_cas = set(
        [
            LeaderUnknown,
            NotALeader,
            TermMismatch,
            RaftLogCompacted,
            RaftLogUnavailable,
            CasEntryTermMismatch,
            CasConflictFound,
        ]
    )

    @classmethod
    def is_retriable_for_cas(cls, code):
        return code in cls.retriable_for_cas


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def assert_starts_with(actual_string: str, expected_prefix: str):
    """Using this function results in a better pytest output in case of assertion failure."""
    assert actual_string[: len(expected_prefix)] == expected_prefix


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
    parser.addoption(
        "--with-webui",
        action="store_true",
        default=False,
        help="Whether to run Web UI tests",
    )
    parser.addoption(
        "--base-port",
        type=int,
        action="store",
        default=3300,
        help="Base socket port which determines the range of ports used for picodata instances spawned for testing.",  # noqa: E501
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]):
    # https://docs.pytest.org/en/7.4.x/how-to/writing_hook_functions.html
    # https://docs.pytest.org/en/7.4.x/example/simple.html#control-skipping-of-tests-according-to-command-line-option

    if not config.getoption("--with-webui"):
        skip = pytest.mark.skip(reason="run: pytest --with-webui")
        for item in items:
            if "webui" in item.keywords:
                item.add_marker(skip)


def can_bind(port):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((BASE_HOST, port))
        except socket.error:
            eprint(f"Cant bind to {port}. Skipping")
            return False
        return True


class PortDistributor:
    def __init__(self, start: int, end: int) -> None:
        self.gen = iter(range(start, end))

    def get(self) -> int:
        for port in self.gen:
            if can_bind(port):
                return port

        raise Exception(
            "No more free ports left in configured range, consider enlarging it"
        )


@pytest.fixture(scope="session")
def port_distributor(xdist_worker_number: int, pytestconfig) -> PortDistributor:
    """
    Return pair (base_port, max_port) available for current pytest subprocess.
    Ensures that all ports in this range are not in use.
    Executes once due scope="session".

    Note: this function has a side-effect.
    """
    assert isinstance(xdist_worker_number, int)
    assert xdist_worker_number >= 0
    global_base_port = pytestconfig.getoption("--base-port")
    base_port = global_base_port + xdist_worker_number * PORT_RANGE

    max_port = base_port + PORT_RANGE
    assert max_port <= 65535

    return PortDistributor(start=base_port, end=max_port)


@pytest.fixture(scope="session")
def seed(pytestconfig):
    """Return a seed for randomized tests. Unless passed via
    command-line options it is generated automatically.
    """
    seed = pytestconfig.getoption("--seed")
    return seed if seed else generate_seed()


@pytest.fixture(scope="session")
def delay(pytestconfig):
    return pytestconfig.getoption("--delay")


@pytest.fixture(scope="session")
def with_flamegraph(pytestconfig):
    return bool(pytestconfig.getoption("--with-flamegraph"))


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


def is_caused_by_timeout(e: BaseException) -> bool:
    """
    Check if there's a `TimeoutError` somewhere in the cause-chain of this exception.
    """
    if isinstance(e, TimeoutError):
        return True

    if not e.__context__:
        return False

    return is_caused_by_timeout(e.__context__)


def normalize_net_box_result(func):
    """
    Convert lua-style responses to be more python-like.
    This also fixes some of the connector API inconveniences.
    """

    def inner(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except DatabaseError as exc:
            if is_caused_by_timeout(exc):
                raise TimeoutError from exc

            if getattr(exc, "errno", None):
                # Error handling in Tarantool connector is awful.
                # It wraps NetworkError in DatabaseError.
                # We, instead, convert it to a native OSError.
                strerror = os.strerror(exc.errno)
                raise OSError(exc.errno, strerror) from exc

            source_location = None
            if exc.extra_info:
                source_location = f"{exc.extra_info.file}:{exc.extra_info.line}"

            match exc.args:
                case (int(code), arg):
                    # Error handling in Tarantool connector is awful.
                    # It returns error codes as raw numbers.
                    # Here we format them for easier use in pytest assertions.
                    error_info = tnt_strerror(code)
                    match error_info:
                        case (str(error_type), _):
                            raise TarantoolError(
                                error_type, arg, source_location
                            ) from exc
                        case "UNDEFINED":
                            raise TarantoolError(code, arg, source_location) from exc
                        case _:
                            raise RuntimeError("unreachable")
                case _:
                    raise exc from exc

        # This is special case for Connection.__init__
        if result is None:
            return

        # This is special case for non-SELECT or non-VALUES IPROTO_EXECUTE requests
        if hasattr(result, "affected_row_count") and result.data is None:
            return {"row_count": result.affected_row_count}

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


class CasRange:
    # FIXME: these values are associated with the class, not the object.
    # All objects have access to the same variables
    key_min = dict(kind="unbounded", key=None)
    key_max = dict(kind="unbounded", key=None)
    repr_min = "unbounded"
    repr_max = "unbounded"
    table = None

    def __repr__(self):
        return f"CasRange({self.repr_min}, {self.repr_max})"

    def __init__(self, table=None, gt=None, ge=None, lt=None, le=None, eq=None):
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
        self.table = table
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


class ProcessDead(Exception):
    pass


class NotALeader(Exception):
    pass


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
        timeout: int | float = 10,
        rps: int | float = 4,
        fatal: Type[Exception] | Tuple[Exception, ...] = ProcessDead,
        fatal_predicate: Callable[[Exception], bool] = lambda x: False,
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
        self.fatal_predicate = fatal_predicate

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
                if self.fatal_predicate(e):
                    raise e from e
                now = time.monotonic()
                if now > self.deadline:
                    raise e from e

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


OUT_LOCK = threading.Lock()

POSITION_IN_SPACE_INSTANCE_NAME = 3


class Connection(tarantool.Connection):  # type: ignore
    @normalize_net_box_result
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @normalize_net_box_result
    def call(self, func_name, *args, on_push=None, on_push_ctx=None):
        return super().call(func_name, *args, on_push=on_push, on_push_ctx=on_push_ctx)

    @normalize_net_box_result
    def eval(self, expr, *args, on_push=None, on_push_ctx=None):
        return super().eval(expr, *args, on_push=on_push, on_push_ctx=on_push_ctx)

    @normalize_net_box_result
    def execute(self, query, params=None):
        return super().execute(query, params)

    def sql(self, sql: str, *params, options=None, sudo=False) -> dict[str, list]:
        """Run SQL query and return result"""
        if sudo:
            old_euid = self.eval(
                """
                local before = box.session.euid()
                box.session.su('admin')
                return before
                """
            )

        options = options or {}
        try:
            result = self.call(
                ".proc_sql_dispatch",
                sql,
                params,
                options.get("query_id"),
                options.get("traceable"),
            )[0]
        finally:
            if sudo:
                self.eval("box.session.su(...)", old_euid)

        return result


@dataclass
class Instance:
    binary_path: str
    cwd: str
    color: Callable[[str], str]

    plugin_dir: str | None = None
    cluster_name: str | None = None
    _data_dir: str | None = None
    peers: list[str] = field(default_factory=list)
    host: str | None = None
    port: int | None = None

    audit: str | bool = True
    tier: str | None = None
    init_replication_factor: int | None = None
    config_path: str | None = None
    name: str | None = None
    replicaset_name: str | None = None
    failure_domain: dict[str, str] = field(default_factory=dict)
    service_password_file: str | None = None
    env: dict[str, str] = field(default_factory=dict)
    process: subprocess.Popen | None = None
    raft_id: int = INVALID_RAFT_ID
    _on_output_callbacks: list[Callable[[bytes], None]] = field(default_factory=list)

    @property
    def data_dir(self):
        assert self._data_dir
        return self._data_dir

    @property
    def listen(self):
        if self.host is None or self.port is None:
            return None
        return f"{self.host}:{self.port}"

    def current_state(self, instance_name=None):
        if instance_name is None:
            instance_name = self.name
        return self.call(".proc_instance_info", instance_name)["current_state"]

    def get_tier(self):
        return "default" if self.tier is None else self.tier

    def uuid(self):
        return self.eval("return box.info.uuid")

    def replicaset_uuid(self):
        return self.eval("return box.info.cluster.uuid")

    @property
    def audit_flag_value(self):
        """
        This property abstracts away peculiarities of the audit config.
        This is the value we're going to pass via `--audit`, or `None`
        if audit is disabled for this instance.
        """
        if self.audit:
            if isinstance(self.audit, bool):
                return "/dev/stderr"
            if isinstance(self.audit, str):
                return self.audit
        return None

    @property
    def command(self):
        audit = self.audit_flag_value
        service_password = self.service_password_file

        # fmt: off
        return [
            self.binary_path, "run",
            *([f"--cluster-name={self.cluster_name}"] if self.cluster_name else []),
            *([f"--instance-name={self.name}"] if self.name else []),
            *([f"--replicaset-name={self.replicaset_name}"] if self.replicaset_name else []),
            *([f"--data-dir={self._data_dir}"] if self._data_dir else []),
            *([f"--plugin-dir={self.plugin_dir}"] if self.plugin_dir else []),
            *([f"--listen={self.listen}"] if self.listen else []),
            *([f"--peer={str.join(',', self.peers)}"] if self.peers else []),
            *(f"--failure-domain={k}={v}" for k, v in self.failure_domain.items()),
            *(["--init-replication-factor", f"{self.init_replication_factor}"]
              if self.init_replication_factor is not None else []),
            *(["--config", self.config_path] if self.config_path is not None else []),
            *(["--tier", self.tier] if self.tier is not None else []),
            *(["--audit", audit] if audit else []),
            *(["--service-password-file", service_password] if service_password else []),
        ]
        # fmt: on

    def __repr__(self):
        if self.process:
            return f"Instance({self.name}, listen={self.listen} cluster={self.cluster_name}, process.pid={self.process.pid})"  # noqa: E501
        else:
            return f"Instance({self.name}, listen={self.listen} cluster={self.cluster_name})"  # noqa: E501

    def __hash__(self):
        return hash((self.cluster_name, self.name))

    @contextmanager
    def connect(
        self, timeout: int | float, user: str | None = None, password: str | None = None
    ):
        if user is None:
            user = "pico_service"
            if password is None:
                password = self.service_password

        try:
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
        except Exception as e:
            self.check_process_alive()
            # if process is dead, the above call will raise an exception
            # otherwise we raise the original exception
            raise e from e

        try:
            yield c
        finally:
            c.close()

    def call(
        self,
        fn,
        *args,
        user: str | None = None,
        password: str | None = None,
        timeout: int | float = 10,
    ):
        try:
            with self.connect(timeout, user=user, password=password) as conn:
                return conn.call(fn, args)
        except Exception as e:
            self.check_process_alive()
            raise e from e

    def eval(
        self,
        expr,
        *args,
        user: str | None = None,
        password: str | None = None,
        timeout: int | float = 10,
    ):
        try:
            with self.connect(timeout, user=user, password=password) as conn:
                return conn.eval(expr, *args)
        except Exception as e:
            self.check_process_alive()
            raise e from e

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

    def replicaset_master_name(self, timeout: int | float = 10) -> str:
        """
        Returns `current_master_name` of this instance's replicaset.
        Waits until `current_master_name` == `target_master_name` if there's a
        master switchover in process.
        """

        def make_attempt():
            current_master_name = self.eval(
                """
                local replicaset_name = pico.instance_info(...).replicaset_name
                local info = box.space._pico_replicaset:get(replicaset_name)
                if info.target_master_name ~= info.current_master_name then
                    error(string.format('master is transitioning from %s to %s', info.current_master_name, info.target_master_name))
                end
                return info.current_master_name
                """,  # noqa: E501
                self.name,
            )
            assert current_master_name
            return current_master_name

        return Retriable(timeout=timeout, rps=4).call(make_attempt)  # type: ignore

    def sql(
        self,
        sql: str,
        *params,
        options: Optional[Dict[str, Any]] = None,
        strip_metadata=True,
        sudo=False,
        user: str | None = None,
        password: str | None = None,
        timeout: int | float = 10,
    ):
        """
        Run SQL query and return result.
        Parameters:
        * `sudo`: Whether the query is executed with 'admin' privileges or not.
        * `strip_metadata`: If `true`, the "metadata" field of the response is
            removed and only the list of rows is returned (the "rows" field).
            If the response instead contains just the "row_count" field, this
            parameter is ignored.
        """
        with self.connect(timeout=timeout, user=user, password=password) as conn:
            result = conn.sql(sql, sudo=sudo, *params, options=options)
        if strip_metadata and "rows" in result:
            return result["rows"]
        return result

    def retriable_sql(
        self,
        sql: str,
        *params,
        rps: int | float = 2,
        retry_timeout: int | float = 25,
        sudo: bool = False,
        user: str | None = None,
        password: str | None = None,
        timeout: int | float = 10,
        fatal: Type[Exception] | Tuple[Exception, ...] = ProcessDead,
        fatal_predicate: Callable[[Exception], bool] | str = lambda x: False,
    ) -> dict:
        """Retry SQL query with constant rate until success or fatal is raised"""

        predicate: Any = fatal_predicate
        if type(fatal_predicate) is str:

            def fatal_message(e: Exception) -> bool:
                return bool(re.search(fatal_predicate, str(e)))

            predicate = fatal_message

        attempt = 0

        def do_sql():
            nonlocal attempt
            attempt += 1
            if attempt > 1:
                print(f"retrying SQL query `{sql}` ({attempt=})", file=sys.stderr)

            return self.sql(
                sql, *params, sudo=sudo, user=user, password=password, timeout=timeout
            )

        return Retriable(
            timeout=retry_timeout,
            rps=rps,
            fatal=fatal,
            fatal_predicate=predicate,
        ).call(do_sql)

    def create_user(
        self,
        with_name: str,
        with_password: str,
        with_auth: str | None = None,
        user: str | None = None,
        password: str | None = None,
        timeout: int | float = 10,
    ):
        sql = f"CREATE USER \"{with_name}\" WITH PASSWORD '{with_password}' " + (
            ("USING " + with_auth) if with_auth else ""
        )
        self.sql(
            sql=sql,
            user=user,
            password=password,
            timeout=timeout,
        )

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

    def _process_output(self, src, out: io.TextIOWrapper):
        id = self.name or f":{self.port}"
        prefix = f"{id:<3} | "

        if sys.stdout.isatty():
            prefix = self.color(prefix)

        prefix_bytes = prefix.encode("utf-8")

        # `iter(callable, sentinel)` form: calls callable until it returns sentinel
        for line in iter(src.readline, b""):
            with OUT_LOCK:
                out.buffer.write(prefix_bytes)
                out.buffer.write(line)
                out.flush()
                for cb in self._on_output_callbacks:
                    cb(line)

        # Close the stream, because `Instance.fail_to_start` is waiting for it
        src.close()

    def on_output_line(self, cb: Callable[[bytes], None]):
        self._on_output_callbacks.append(cb)

    def start(
        self,
        peers: Optional[List["Instance"]] = None,
        cwd=None,
    ):
        if self.process:
            # Be idempotent
            return

        eprint(f"{self} starting...")

        if peers is not None:
            self.peers = list(map(lambda i: i.listen, peers))

        env = {**self.env}
        if not os.environ.get("PICODATA_LOG_LEVEL") and "PICODATA_LOG_LEVEL" not in env:
            env.update(PICODATA_LOG_LEVEL="verbose")

        if os.environ.get("RUST_BACKTRACE") is not None:
            env.update(RUST_BACKTRACE=str(os.environ.get("RUST_BACKTRACE")))

        if os.getenv("NOLOG"):
            out = subprocess.DEVNULL
        else:
            out = subprocess.PIPE

        self.process = subprocess.Popen(
            self.command,
            cwd=cwd or self.cwd,
            env=env or None,
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=out,
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
            # to form its own process group. The easiest way to
            # accomplish it is to start a new session. With this flag
            # pytest implicitly calls `setsid()`.
            start_new_session=True,
        )

        # Assert a new process group is created
        assert os.getpgid(self.process.pid) == self.process.pid

        if out == subprocess.DEVNULL:
            return

        for src, out in [  # type: ignore
            (self.process.stdout, sys.stdout),
            (self.process.stderr, sys.stderr),
        ]:
            threading.Thread(
                target=self._process_output,
                args=(src, out),
                daemon=True,
            ).start()

    def fail_to_start(self, timeout: int = 10):
        assert self.process is None, "process is already running"
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

    def wait_process_stopped(self, timeout: int = 10):
        if self.process is None:
            return

        # FIXME: copy-pasted from above
        self.process.wait(timeout)

        # When logs are disabled stdour and stderr are set to None
        if not (self.process.stdout or self.process.stderr):
            self.process = None
            return

        # Wait for all the output to be handled in the separate threads
        while not self.process.stdout.closed or not self.process.stderr.closed:  # type: ignore
            time.sleep(0.1)

        self.process = None

    def restart(self, kill: bool = False, remove_data: bool = False):
        if kill:
            self.kill()
        else:
            self.terminate()

        if remove_data:
            self.remove_data()

        self.start()

    def remove_data(self):
        shutil.rmtree(self.data_dir)

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
        op_kind: Literal["insert", "replace", "delete", "update"],
        table: str | int,
        tuple: Tuple | List | None = None,
        *,
        key: Tuple | List | None = None,
        ops: Tuple | List | None = None,
        index: int | None = None,
        term: int | None = None,
        ranges: List[CasRange] | None = None,
        user: int | None = None,
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
                assert range.key_min == range.key_max, "we don't use other ranges ever"
                range_key = range.key_min["key"]
                key_packed = msgpack.packb(range_key)
                bounds = dict(kind="eq", key=key_packed)
                range_packed = dict(table=table_id, bounds=bounds)
                predicate_ranges.append(range_packed)

        predicate = dict(
            index=index,
            term=term,
            ranges=predicate_ranges,
        )

        dml = dict(
            kind="dml",
            op_kind=op_kind,
            table=table_id,
        )
        if op_kind in ["insert", "replace"]:
            assert tuple
            dml["tuple"] = msgpack.packb(tuple)
        elif op_kind == "delete":
            assert key
            dml["key"] = msgpack.packb(key)
        elif op_kind == "update":
            assert key
            dml["key"] = msgpack.packb(key)
            assert ops
            dml["ops"] = [msgpack.packb(op) for op in ops]  # type: ignore
        else:
            raise Exception(f"unsupported {op_kind=}")

        # guest has super privs for now by default this should be equal
        # to ADMIN_USER_ID on the rust side
        as_user = user if user is not None else 1
        dml["initiator"] = as_user

        eprint(f"CaS:\n  {predicate=}")
        if len(dml.get("tuple") or []) > 512:  # type: ignore
            op_to_display = {k: v for k, v in dml.items()}
            op_to_display["tuple"] = "<too-big-to-display>"
            eprint(f"  dml={op_to_display}")
        else:
            eprint(f"  {dml=}")

        return self.call(".proc_cas", self.cluster_name, predicate, dml, as_user)[
            "index"
        ]

    def pico_property(self, key: str):
        tup = self.call("box.space._pico_property:get", key)
        if tup is None:
            return None

        return tup[1]

    def next_schema_version(self) -> int:
        return self.pico_property("next_schema_version") or 1

    def create_table(self, params: dict, timeout: float = 10) -> int:
        """
        Creates a table. Returns a raft index at which a newly created table
        has to exist on all peers.

        Submits SQL: CREATE TABLE.
        """
        name = params["name"]
        format = params["format"]
        engine = params.get("engine", "")
        distribution = params["distribution"]

        primary_key = ",".join(params["primary_key"])
        sharding_key = ",".join(params.get("sharding_key", ""))

        data = ""

        for record in format:
            is_nullable = "NOT NULL" if not record["is_nullable"] else ""
            data += f'"{record["name"]}" {record["type"].upper()} {is_nullable},'

        if params["distribution"] == "global":
            distribution = "GLOBALLY"
        elif params["distribution"] == "sharded":
            distribution = f"BY ({sharding_key})"
        else:
            raise Exception(
                f'Wrong distribution: {params["distribution"]}. '
                "Possible options: global, sharded"
            )

        if engine:
            engine = f"USING {engine}"

        self.sql(
            f'CREATE TABLE "{name}" ('
            f"{data} "
            f"PRIMARY KEY ({primary_key})) "
            f"{engine} "
            f"DISTRIBUTED {distribution} "
            f"OPTION (TIMEOUT = {timeout});",
            timeout=timeout + 0.5,
        )

        return self.raft_get_index()

    def drop_table(self, table: int | str, timeout: float = 10) -> int:
        """
        Drops the table. Returns a raft index at which the table has to be
        dropped on all peers.
        """
        self.sql(
            f'DROP TABLE "{table}" OPTION (TIMEOUT = {timeout});',
            timeout=timeout + 0.5,
        )
        return self.raft_get_index()

    def abort_ddl(self, timeout: float = 10) -> int:
        """
        Aborts a pending DDL operation and waits for abort to be committed localy.
        If `timeout` is reached earlier returns an error.

        Returns an index of the corresponding DdlAbort raft entry, or an error if
        there is no pending DDL operation.
        """
        index = self.call("pico.abort_ddl", timeout, timeout=timeout + 0.5)
        return index

    def propose_create_space(
        self, space_def: Dict[str, Any], wait_index: bool = True, timeout: int = 10
    ) -> int:
        """
        Proposes a space creation ddl prepare operation. Returns the index of
        the corresponding finalizing ddl commit or ddl abort entry.

        If `wait_index` is `True` will wait for the finalizing entry
        to be applied for `timeout` seconds.
        """
        op = dict(
            kind="ddl_prepare",
            schema_version=self.next_schema_version(),
            ddl=dict(kind="create_table", **space_def),
        )
        # TODO: rewrite the test using pico.cas
        index = self.call("pico.raft_propose", op, timeout=timeout)
        # Index of the corresponding ddl abort / ddl commit entry
        index_fin = index + 1
        if wait_index:
            self.raft_wait_index(index_fin, timeout)

        return index_fin

    def assert_raft_status(self, state, leader_id=None):
        status = self.call(".proc_raft_info")

        if leader_id is None:
            leader_id = status["leader_id"]

        assert {
            "raft_state": status["state"],
            "leader_id": status["leader_id"],
        } == {"raft_state": state, "leader_id": leader_id}

    def check_process_alive(self):
        if self.process is None:
            raise ProcessDead("process was not started")

        try:
            # Note: The process may have crashed due to the RPC, but there may
            # be a race between when the python connector receives the
            # connection reset error and when the OS will finish cleaning up
            # the process (especially considering we have a supervisor
            # process). So we introduce a tiny timeout here (which may still not
            # be enough in every case).
            exit_code = self.process.wait(timeout=0.1)  # type: ignore
        except subprocess.TimeoutExpired:
            # it's fine, the process is still running
            pass
        else:
            raise ProcessDead(f"process exited unexpectedly, {exit_code=}")

    def assert_process_dead(self):
        try:
            self.check_process_alive()
            assert False
        except ProcessDead:
            # Make it so we can call Instance.start later
            self.process = None

    def wait_online(
        self, timeout: int | float = 30, rps: int | float = 5, expected_incarnation=None
    ):
        """Wait until instance attains Online grade.

        This function will periodically check the current instance's grade and
        reset the timeout each time the grade changes.

        Args:
            timeout (int | float, default=6): time limit since last grade change
            rps (int | float, default=5): retries per second

        Raises:
            AssertionError: if doesn't succeed
        """

        if self.process is None:
            raise ProcessDead("process was not started")

        def fetch_current_state() -> Tuple[str, int]:
            self.check_process_alive()

            myself = self.call(".proc_instance_info")
            assert isinstance(myself, dict)

            assert isinstance(myself["raft_id"], int)
            self.raft_id = myself["raft_id"]

            assert isinstance(myself["name"], str)
            self.name = myself["name"]

            assert isinstance(myself["current_state"], dict)
            return (
                myself["current_state"]["variant"],
                myself["current_state"]["incarnation"],
            )

        start = time.monotonic()
        deadline = start + timeout
        next_retry = start
        last_state = None
        while True:
            now = time.monotonic()
            assert now < deadline, "timeout"

            # Throttling
            if now < next_retry:
                time.sleep(next_retry - now)
            next_retry = time.monotonic() + 1 / rps

            try:
                # Fetch state
                state = fetch_current_state()
                if state != last_state:
                    last_state = state
                    deadline = time.monotonic() + timeout

                # Check state
                variant, incarnation = state
                assert variant == "Online"
                if expected_incarnation is not None:
                    assert incarnation == expected_incarnation

                # Success!
                break

            except ProcessDead as e:
                raise e from e
            except Exception as e:
                match e.args:
                    case (ErrorCode.Loading, _):
                        # This error is returned when instance is in the middle
                        # of bootstrap (box.cfg{} is running). This sometimes
                        # takes quite a long time in our test runs, so we put
                        # this crutch in for that special case. The logic is:
                        # if tarantool is taking too long to bootstrap it's
                        # probably not our fault.
                        if time.monotonic() > start + timeout * 6:
                            raise e from e
                    case _:
                        if time.monotonic() > deadline:
                            raise e from e

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
        return self.call(".proc_get_index")

    def raft_read_index(self, timeout: int | float = 10) -> int:
        """
        Perform the quorum read operation.
        See `crate::traft::node::Node::read_index`.
        """

        return self.call(
            ".proc_read_index",
            timeout,  # this timeout is passed as an argument
            timeout=timeout + 1,  # this timeout is for network call
        )[0]

    def raft_wait_index(self, target: int, timeout: int | float = 10) -> int:
        """
        Wait until instance applies the `target` raft index.
        See `crate::traft::node::Node::wait_index`.
        """

        def make_attempt():
            return self.call(
                ".proc_wait_index",
                target,
                timeout,  # this timeout is passed as an argument
                timeout=timeout + 1,  # this timeout is for network call
            )

        index = Retriable(timeout=timeout + 1, rps=10).call(make_attempt)

        assert index is not None
        return index

    def get_vclock(self) -> int:
        """Get current vclock"""

        return self.call("pico.get_vclock")

    def wait_vclock(self, target: int, timeout: int | float = 10) -> int:
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

    def governor_step_counter(self) -> int:
        info = self.call(".proc_runtime_info")["internal"]
        return info["governor_step_counter"]

    def wait_governor_status(
        self,
        expected_status: str,
        old_step_counter: int | None = None,
        timeout: int | float = 10,
    ):
        assert expected_status != "not a leader", "use another function"

        def impl():
            info = self.call(".proc_runtime_info")["internal"]
            actual_status = info["governor_loop_status"]
            if actual_status == "not a leader":
                raise NotALeader("not a leader")

            if old_step_counter:
                assert old_step_counter != info["governor_step_counter"]

            assert actual_status == expected_status

        Retriable(timeout=timeout, rps=1, fatal=NotALeader).call(impl)

    def promote_or_fail(self):
        attempt = 0

        def make_attempt(timeout, rps):
            nonlocal attempt
            attempt = attempt + 1
            eprint(f"{self} is trying to become a leader, {attempt=}")

            # 1. Force the node to campaign.
            self.call(".proc_raft_promote")

            # 2. Wait until the miracle occurs.
            Retriable(timeout, rps).call(self.assert_raft_status, "Leader")

        Retriable(timeout=10, rps=1).call(make_attempt, timeout=1, rps=10)
        eprint(f"{self} is a leader now")

    def grant_privilege(
        self, user, privilege: str, object_type: str, object_name: Optional[str] = None
    ):
        if privilege == "execute" and object_type == "role":
            self.sql(f'GRANT "{object_name}" TO "{user}"', sudo=True)
        elif object_name:
            self.sql(
                f'GRANT {privilege} ON {object_type} "{object_name}" TO "{user}"',
                sudo=True,
            )
        else:
            self.sql(f'GRANT {privilege} {object_type} TO "{user}"', sudo=True)

    def revoke_privilege(
        self, user, privilege: str, object_type: str, object_name: Optional[str] = None
    ):
        if privilege == "execute" and object_type == "role":
            self.sql(f'REVOKE "{object_name}" FROM "{user}"', sudo=True)
        elif object_name:
            self.sql(
                f'REVOKE {privilege} ON {object_type} "{object_name}" FROM "{user}"',
                sudo=True,
            )
        else:
            self.sql(f'REVOKE {privilege} {object_type} FROM "{user}"', sudo=True)

    def start_and_wait(self) -> None:
        self.start()
        self.wait_online()

    @property
    def service_password(self) -> Optional[str]:
        if self.service_password_file is None:
            return None
        with open(self.service_password_file, "r") as f:
            password = f.readline()
            if password.endswith("\n"):
                password = password[:-1]
        return password


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
    plugin_dir: str
    base_host: str
    port_distributor: PortDistributor
    instances: list[Instance] = field(default_factory=list)
    config_path: str | None = None
    service_password_file: str | None = None

    def __repr__(self):
        return f'Cluster("{self.base_host}", n={len(self.instances)})'

    def __getitem__(self, item: int) -> Instance:
        return self.instances[item]

    def deploy(
        self,
        *,
        instance_count: int,
        init_replication_factor: int | None = None,
        tier: str | None = None,
        service_password: str | None = None,
        audit: bool | str = True,
    ) -> list[Instance]:
        assert not self.instances, "Already deployed"

        if not service_password:
            service_password = "password"

        self.set_service_password(service_password)

        for _ in range(instance_count):
            self.add_instance(
                wait_online=False,
                tier=tier,
                init_replication_factor=init_replication_factor,
                audit=audit,
            )

        return self.wait_online()

    def wait_online(self) -> list[Instance]:
        for instance in self.instances:
            instance.start()

        for instance in self.instances:
            instance.wait_online()

        eprint(f" {self} deployed ".center(80, "="))
        return self.instances

    def set_config_file(self, config: dict | None = None, yaml: str | None = None):
        assert config or yaml
        assert self.config_path is None

        self.config_path = self.data_dir + "/config.yaml"

        if config:
            yaml = yaml_lib.dump(config, default_flow_style=False)

        assert yaml
        with open(self.config_path, "w") as yaml_file:
            yaml_file.write(yaml)

    def set_service_password(self, service_password: str):
        self.service_password_file = self.data_dir + "/password.txt"
        with open(self.service_password_file, "w") as f:
            print(service_password, file=f)
        os.chmod(self.service_password_file, 0o600)

    def add_instance(
        self,
        wait_online=True,
        peers: list[str] | None = None,
        instance_name: str | bool = True,
        replicaset_name: str | None = None,
        failure_domain=dict(),
        init_replication_factor: int | None = None,
        tier: str | None = None,
        audit: bool | str = True,
        enable_http: bool = False,
    ) -> Instance:
        """Add an `Instance` into the list of instances of the cluster and wait
        for it to attain Online grade unless `wait_online` is `False`.

        `instance_name` specifies how the instance's name is generated in the
        following way:

        - if `instance_name` is a string, it will be used as a value for the
          `--instance-name` command-line option.

        - If `instance_name` is `True` (default), the `--instance-name` command-line
          option will be generated by the pytest according to the instances
          sequence number in cluster.

        - If `instance_name` is `False`, the instance will be started
          without the `--instance-name` command-line option and the particular value
          will be generated by the cluster.
        """
        i = 1 + len(self.instances)

        generated_instance_name: str | None
        match instance_name:
            case str() as iid:
                generated_instance_name = iid
            case True:
                generated_instance_name = f"i{i}"
            case False:
                generated_instance_name = None
            case _:
                raise Exception("unreachable")

        port = self.port_distributor.get()
        if not self.instances:
            bootstrap_port = port
        else:
            assert isinstance(self.instances[0].port, int)  # for mypy
            bootstrap_port = self.instances[0].port

        instance = Instance(
            binary_path=self.binary_path,
            cwd=self.data_dir,
            cluster_name=self.id,
            name=generated_instance_name,
            replicaset_name=replicaset_name,
            _data_dir=f"{self.data_dir}/i{i}",
            plugin_dir=self.plugin_dir,
            host=self.base_host,
            port=port,
            peers=peers or [f"{self.base_host}:{bootstrap_port}"],
            color=CLUSTER_COLORS[len(self.instances) % len(CLUSTER_COLORS)],
            failure_domain=failure_domain,
            init_replication_factor=init_replication_factor,
            tier=tier,
            config_path=self.config_path,
            audit=audit,
        )
        if self.service_password_file:
            instance.service_password_file = self.service_password_file

        if enable_http:
            listen = f"{self.base_host}:{self.port_distributor.get()}"
            instance.env["PICODATA_HTTP_LISTEN"] = listen

        self.instances.append(instance)

        if wait_online:
            instance.start()
            instance.wait_online()

        return instance

    def fail_to_add_instance(
        self,
        peers=None,
        instance_name: str | bool = True,
        failure_domain=dict(),
        init_replication_factor: int | None = None,
        tier: str = "storage",
    ):
        instance = self.add_instance(
            wait_online=False,
            peers=peers,
            instance_name=instance_name,
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
        shutil.rmtree(self.data_dir)

    def expel(
        self,
        target: Instance,
        peer: Instance | None = None,
    ):
        peer = peer if peer else target
        assert self.service_password_file, "cannot expel without pico_service password"
        assert target.name, "cannot expel without target instance name"

        # fmt: off
        command: list[str] = [
            self.binary_path, "expel",
            "--peer", f"pico_service@{peer.listen}",
            "--cluster-name", target.cluster_name or "",
            "--password-file", self.service_password_file,
            "--auth-type", "chap-sha1",
            target.name,
        ]
        # fmt: on

        print(f"executing: {command}")
        rc = subprocess.call(
            command,
            stdin=subprocess.DEVNULL,
        )
        assert rc == 0

    def raft_wait_index(self, index: int, timeout: float = 10):
        """
        Waits for all peers to commit an entry with index `index`.
        """
        assert type(index) is int
        deadline = time.time() + timeout
        for instance in self.instances:
            if instance.process is not None:
                timeout = deadline - time.time()
                if timeout < 0:
                    timeout = 0
                instance.raft_wait_index(index, timeout)

    def create_table(self, params: dict, timeout: float = 3.0):
        """
        Creates a table. Waits for all online peers to be aware of it.
        """
        index = self.instances[0].create_table(params, timeout)
        self.raft_wait_index(index, timeout)

    def drop_table(self, space: int | str, timeout: float = 3.0):
        """
        Drops the space. Waits for all online peers to be aware of it.
        """
        index = self.instances[0].drop_table(space, timeout)
        self.raft_wait_index(index, timeout)

    def abort_ddl(self, timeout: float = 3.0):
        """
        Aborts a pending ddl. Waits for all peers to be aware of it.
        """
        index = self.instances[0].abort_ddl(timeout)
        self.raft_wait_index(index, timeout)

    def batch_cas(
        self,
        ops: List,
        index: int | None = None,
        term: int | None = None,
        ranges: List[CasRange] | None = None,
        instance: Instance | None = None,
        user: str | None = None,
        password: str | None = None,
    ) -> int:
        if instance is None:
            instance = self.instances[0]

        predicate_ranges = []
        if ranges is not None:
            for range in ranges:
                predicate_ranges.append(
                    dict(
                        table=range.table,
                        key_min=range.key_min,
                        key_max=range.key_max,
                    )
                )

        predicate = dict(
            index=index,
            term=term,
            ranges=predicate_ranges,
        )

        eprint(f"batch CaS:\n  {predicate=}\n  {ops=}")
        return instance.call(
            "pico.batch_cas", dict(ops=ops), predicate, user=user, password=password
        )

    def cas(
        self,
        op_kind: Literal["insert", "replace", "delete", "update"],
        table: str,
        tuple: Tuple | List | None = None,
        *,
        key: Tuple | List | None = None,
        ops: Tuple | List | None = None,
        index: int | None = None,
        term: int | None = None,
        ranges: List[CasRange] | None = None,
        # If specified find leader via this instance
        instance: Instance | None = None,
        user: str | None = None,
    ) -> int:
        """
        Performs a clusterwide compare and swap operation.

        E.g. it checks the `predicate` on leader and if no conflicting entries were found
        appends the `op` to the raft log and returns its index.

        Calling this operation will route CaS request to a leader.
        """
        if instance is None:
            instance = self.instances[0]

        raft_info = instance.call(".proc_raft_info")
        leader_id = raft_info["leader_id"]
        [[leader_address]] = instance.sql(
            """ SELECT address FROM _pico_peer_address WHERE raft_id = ? """, leader_id
        )

        # ADMIN by default
        user_id = 1
        if user:
            [[user_id]] = instance.sql(
                """ SELECT id FROM _pico_user WHERE name = ? """, user
            )
            user_id = int(user_id)

        leader = self.get_instance_by_address(leader_address)
        return leader.cas(
            op_kind,
            table,
            tuple,
            key=key,
            ops=ops,
            index=index,
            term=term,
            ranges=ranges,
            user=user_id,
        )

    def get_instance_by_address(self, address: str) -> Instance:
        host, port = address.split(":", maxsplit=1)
        port = int(port)  # type: ignore

        for instance in self.instances:
            if instance.host == host and instance.port == port:
                return instance

        raise RuntimeError(f"no instance listenning on {host}:{port}")

    def wait_until_instance_has_this_many_active_buckets(
        self,
        i: Instance,
        expected: int,
        max_retries: int = 10,
    ):
        attempt = 1
        previous_active = None
        while True:
            for j in self.instances:
                j.eval(
                    """
                    if vshard.storage.internal.rebalancer_fiber ~= nil then
                        vshard.storage.rebalancer_wakeup()
                    end
                """
                )

            actual_active = Retriable(timeout=10, rps=4).call(
                lambda: i.call("vshard.storage.info")["bucket"]["active"]
            )
            if actual_active == expected:
                return

            print(
                f"\x1b[33mwaiting for bucket rebalancing, was: {previous_active}, became: {actual_active} (#{attempt})\x1b[0m"  # noqa: E501
            )

            if actual_active == previous_active:
                if attempt < max_retries:
                    attempt += 1
                else:
                    print("vshard.storage.info.bucket.active stopped changing")
                    assert actual_active == expected
            else:
                # Something changed -> reset retry counter
                attempt = 1

            previous_active = actual_active

            time.sleep(0.5)

    def masters(self) -> List[Instance]:
        ret = []
        for instance in self.instances:
            if not instance.eval("return box.info.ro"):
                ret.append(instance)

        return ret

    def grant_box_privilege(
        self, user, privilege: str, object_type: str, object_name: Optional[str] = None
    ):
        """
        Sometimes in our tests we go beyond picodata privilege model and need
        to grant priveleges on something that is not part of the picodata access control model.
        For example execute access on universe mainly needed to invoke functions.
        """
        for instance in self.masters():
            instance.eval(
                """
                box.session.su("admin")
                user, privilege, object_type, object_name = ...
                return box.schema.user.grant(user, privilege, object_type, object_name)
                """,
                [user, privilege, object_type, object_name],
            )


class PgStorage:
    def __init__(self, instance: Instance):
        self.instance: Instance = instance
        self.client_ids: list[int] = []

    def statements(self, id: int):
        return self.instance.call("pico.pg_statements", id)

    def portals(self, id: int):
        return self.instance.call("pico.pg_portals", id)

    def bind(self, id, *params):
        return self.instance.call("pico.pg_bind", id, *params, False)

    def close_stmt(self, id: int, name: str):
        return self.instance.call("pico.pg_close_stmt", id, name)

    def close_portal(self, id: int, name: str):
        return self.instance.call("pico.pg_close_portal", id, name)

    def describe_stmt(self, id: int, name: str) -> dict:
        return self.instance.call("pico.pg_describe_stmt", id, name)

    def describe_portal(self, id: int, name: str) -> dict:
        return self.instance.call("pico.pg_describe_portal", id, name)

    def execute(self, id: int, name: str, max_rows: int) -> dict:
        return self.instance.call("pico.pg_execute", id, name, max_rows, False)

    def flush(self):
        for id in self.client_ids:
            for name in self.statements(id)["available"]:
                self.close_stmt(id, name)
            for name in self.portals(id)["available"]:
                self.close_portal(id, name)

    def parse(
        self, id: int, name: str, sql: str, param_oids: list[int] | None = None
    ) -> int:
        param_oids = param_oids if param_oids is not None else []
        return self.instance.call("pico.pg_parse", id, name, sql, param_oids, False)

    def new_client(self, id: int):
        self.client_ids.append(id)
        return PgClient(self, id)


@dataclass
class PgClient:
    storage: PgStorage
    id: int

    @property
    def instance(self) -> Instance:
        return self.storage.instance

    @property
    def statements(self):
        return self.storage.statements(self.id)

    @property
    def portals(self):
        return self.storage.portals(self.id)

    def bind(self, *params):
        return self.storage.bind(self.id, *params)

    def close_stmt(self, name: str):
        return self.storage.close_stmt(self.id, name)

    def close_portal(self, name: str):
        return self.storage.close_portal(self.id, name)

    def describe_stmt(self, name: str) -> dict:
        return self.storage.describe_stmt(self.id, name)

    def describe_portal(self, name: str) -> dict:
        return self.storage.describe_portal(self.id, name)

    def execute(self, name: str, max_rows: int = -1) -> dict:
        return self.storage.execute(self.id, name, max_rows)

    def parse(self, name: str, sql: str, param_oids: list[int] | None = None) -> int:
        return self.storage.parse(self.id, name, sql, param_oids)


def build_profile() -> str:
    return os.environ.get("BUILD_PROFILE", "dev")


def get_test_dir():
    test_dir = Path(__file__).parent
    assert test_dir.name == "test"
    return test_dir


@pytest.fixture(scope="session")
def binary_path(cargo_build: None) -> str:
    """Path to the picodata binary, e.g. "./target/debug/picodata"."""
    metadata = subprocess.check_output(["cargo", "metadata", "--format-version=1"])
    target = json.loads(metadata)["target_directory"]
    # This file is huge, hide it from pytest output in case there's an exception
    # somewhere in this function.
    del metadata

    profile = build_profile()
    # Note: rust names the debug profile `dev`, but puts the binaries into the
    # `debug` directory.
    if profile == "dev":
        profile = "debug"

    binary_path = os.path.realpath(os.path.join(target, f"{profile}/picodata"))

    ext = None
    match sys.platform:
        case "linux":
            ext = "so"
        case "darwin":
            ext = "dylib"

    test_dir = get_test_dir()
    # Copy the test plugin library into the appropriate location
    source = f"{os.path.dirname(binary_path)}/libtestplug.{ext}"
    destinations = [
        f"{test_dir}/testplug/testplug/0.1.0/libtestplug.{ext}",
        f"{test_dir}/testplug/testplug/0.2.0/libtestplug.{ext}",
        f"{test_dir}/testplug/testplug_broken_manifest_1/0.1.0/libtestplug.{ext}",
        f"{test_dir}/testplug/testplug_broken_manifest_2/0.1.0/libtestplug.{ext}",
        f"{test_dir}/testplug/testplug_broken_manifest_3/0.1.0/libtestplug.{ext}",
        f"{test_dir}/testplug/testplug_small/0.1.0/libtestplug.{ext}",
        f"{test_dir}/testplug/testplug_small_svc2/0.1.0/libtestplug.{ext}",
        f"{test_dir}/testplug/testplug_w_migration/0.1.0/libtestplug.{ext}",
        f"{test_dir}/testplug/testplug_w_migration_2/0.1.0/libtestplug.{ext}",
        f"{test_dir}/testplug/testplug_w_migration/0.2.0/libtestplug.{ext}",
        f"{test_dir}/testplug/testplug_w_migration/0.2.0_broken/libtestplug.{ext}",
        f"{test_dir}/testplug/testplug_w_migration_in_tier/0.1.0/libtestplug.{ext}",
        f"{test_dir}/testplug/testplug_sdk/0.1.0/libtestplug.{ext}",
    ]
    for destination in destinations:
        if os.path.exists(destination) and filecmp.cmp(source, destination):
            continue
        eprint(f"Copying '{source}' to '{destination}'")
        shutil.copyfile(source, destination)

    plug_wrong_version_dir = f"{test_dir}/plug_wrong_version/plug_wrong_version"
    destination = f"{plug_wrong_version_dir}/0.1.0/libplug_wrong_version.{ext}"
    source = f"{os.path.dirname(binary_path)}/libplug_wrong_version.{ext}"
    if not (os.path.exists(destination) and filecmp.cmp(source, destination)):
        eprint(f"Copying '{source}' to '{destination}'")
        shutil.copyfile(source, destination)

    return binary_path


@pytest.fixture(scope="session")
def cargo_build(pytestconfig: pytest.Config) -> None:
    """Run cargo build before tests. Skipped in CI"""

    # Start test logs with a newline. This makes them prettier with
    # `pytest -s` (a shortcut for `pytest --capture=no`)
    eprint("")

    if os.environ.get("CI") is not None:
        eprint("Skipping cargo build")
        return

    features = ["error_injection"]
    if bool(pytestconfig.getoption("--with-webui")):
        features.append("webui")

    # fmt: off
    cmd = [
        "cargo", "build",
        "--profile", build_profile(),
        "--features", ",".join(features),
    ]
    # fmt: on
    eprint(f"Running {cmd}")
    assert subprocess.call(cmd) == 0, "cargo build failed"

    crates = ["gostech-audit-log", "testplug", "plug_wrong_version"]
    for crate in crates:
        cmd = ["cargo", "build", "-p", crate, "--profile", build_profile()]
        eprint(f"Running {cmd}")
        assert subprocess.call(cmd) == 0, f"cargo build {crate} failed"


@pytest.fixture(scope="session")
def cluster_names(xdist_worker_number) -> Iterator[str]:
    """Unique `cluster_name` generator."""
    return (f"cluster-{xdist_worker_number}-{i}" for i in count())


@pytest.fixture
def cluster(
    binary_path, tmpdir, cluster_names, port_distributor
) -> Generator[Cluster, None, None]:
    """Return a `Cluster` object capable of deploying test clusters."""
    # FIXME: instead of os.getcwd() construct a path relative to os.path.realpath(__file__)
    # see how it's done in def binary_path()
    plugin_dir = os.getcwd() + "/test/testplug"
    cluster = Cluster(
        binary_path=binary_path,
        id=next(cluster_names),
        data_dir=tmpdir,
        plugin_dir=plugin_dir,
        base_host=BASE_HOST,
        port_distributor=port_distributor,
    )
    yield cluster
    cluster.kill()


@pytest.fixture
def unstarted_instance(
    cluster: Cluster, port_distributor: PortDistributor, pytestconfig
) -> Generator[Instance, None, None]:
    """Returns a deployed instance forming a single-node cluster."""
    cluster.set_service_password("s3cr3t")
    instance = cluster.add_instance(wait_online=False)

    has_webui = bool(pytestconfig.getoption("--with-webui"))
    if has_webui:
        listen = f"{cluster.base_host}:{port_distributor.get()}"
        instance.env["PICODATA_HTTP_LISTEN"] = listen

    yield instance


@pytest.fixture
def instance(unstarted_instance: Instance):
    unstarted_instance.start()
    unstarted_instance.wait_online()
    yield unstarted_instance


@pytest.fixture
def pg_storage(instance: Instance) -> Generator[PgStorage, None, None]:
    """Returns a PG storage on a single instance."""
    storage = PgStorage(instance)
    yield storage
    storage.flush()


@pytest.fixture
def pg_client(instance: Instance) -> Generator[PgClient, None, None]:
    """Returns a PG client on a single instance."""
    storage = PgStorage(instance)
    yield storage.new_client(0)
    storage.flush()


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


class log_crawler:
    def __init__(self, instance: Instance, search_str: str) -> None:
        # If search_str contains multiple lines, we need to be checking multiple
        # lines at a time. Because self._cb will only be called on 1 line at a
        # time we need to do some hoop jumping to make this work.
        self.matched = False

        search_bytes = search_str.encode("utf-8")
        self.expected_lines = search_bytes.splitlines()
        self.n_lines = len(self.expected_lines)

        # This is the current window in which we're searching for the search_str.
        # Basically this is just last N lines of output we've seen.
        self.current_window: List[bytes] = []

        instance.on_output_line(self._cb)

    def _cb(self, line: bytes):
        # exit early if match was already found
        if self.matched:
            return

        if self.n_lines == 1 and self.expected_lines[0] in line:
            self.matched = True
            return

        if len(self.current_window) == self.n_lines:
            # shift window
            del self.current_window[0]
            self.current_window.append(line.strip(b"\n"))

        elif len(self.current_window) < self.n_lines:
            # extend window
            self.current_window.append(line.strip(b"\n"))
            if len(self.current_window) < self.n_lines:
                # still not engough data, no match possible
                return

        #
        # Look trough the search window
        #
        if not self.current_window[0].endswith(self.expected_lines[0]):
            return

        for i in range(1, self.n_lines - 1):
            if self.current_window[i] != self.expected_lines[i]:
                return

        if not self.current_window[-1].startswith(self.expected_lines[-1]):
            return

        self.matched = True

    def wait_matched(self, timeout=10):
        def must_match():
            assert self.matched

        Retriable(timeout=timeout, rps=4).call(func=must_match)


class AuditServer:
    def __init__(self, port: int) -> None:
        self.port = port
        self.process: Process | None = None
        self.queue: Queue[Dict[str, Any]] = Queue(maxsize=10_000)

    def start(self) -> None:
        if self.process is not None:
            return None

        def server(queue: Queue, host: str, port: int) -> None:
            class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
                QUEUE = queue

                def do_POST(self):
                    content_length = int(self.headers["Content-Length"])
                    post_data = self.rfile.read(content_length)

                    data = json.loads(post_data.decode("utf-8"))
                    message = {"message": "Log received successfully", "log": data}
                    self.send_response(200)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    response = bytes(json.dumps(message), "utf-8")
                    self.wfile.write(response)
                    self.QUEUE.put(data)

            httpd = HTTPServer((host, port), SimpleHTTPRequestHandler)
            httpd.serve_forever()

        self.process = Process(target=server, args=(self.queue, BASE_HOST, self.port))
        self.process.start()

    def cmd(self, binary_path: str) -> str:
        target_dir = os.path.dirname(binary_path)
        binary = os.path.realpath(os.path.join(target_dir, "gostech-audit-log"))
        assert os.path.exists(binary)
        args = f"--url http://{BASE_HOST}:{self.port}/log --debug"

        return f"| {binary} {args}"

    def logs(self) -> List[Dict[str, Any]]:
        result = []
        while not self.queue.empty():
            result.append(self.queue.get())
        return result

    def take_until_name(self, name: str):
        while True:
            event = self.queue.get(timeout=2)
            if event["name"] == name:
                return event

    def stop(self):
        if self.process is None:
            return None
        self.process.terminate()
        self.process.join()
        self.process = None


@dataclass
class Postgres:
    cluster: Cluster
    port: int
    host: str = "127.0.0.1"
    ssl: bool = False
    ssl_verify: bool = False

    def install(self):
        self.cluster.set_config_file(
            yaml=f"""
cluster:
    name: test
    tier:
        default:
instance:
    pg:
        listen: "{self.host}:{self.port}"
        ssl: {self.ssl}
"""
        )
        ssl_dir = Path(os.path.realpath(__file__)).parent / "ssl_certs"
        instance_dir = Path(self.cluster.data_dir) / "i1"
        instance_dir.mkdir()
        shutil.copyfile(ssl_dir / "server.crt", instance_dir / "server.crt")
        shutil.copyfile(ssl_dir / "server.key", instance_dir / "server.key")

        if self.ssl_verify:
            shutil.copyfile(ssl_dir / "root.crt", instance_dir / "ca.crt")

        self.cluster.deploy(instance_count=1)
        return self

    @property
    def instance(self):
        return self.cluster.instances[0]


# Exists for debugging purposes only. When you're debugging a single test it is
# simpler to configure wireshark (or other tools) for particular port.
# Shouldn't be used when multiple tests are run (will result in address already in use errors)
PG_LISTEN = os.getenv("PG_LISTEN")


@pytest.fixture
def pg_port(port_distributor: PortDistributor):
    if PG_LISTEN is not None:
        return int(PG_LISTEN)

    return port_distributor.get()


@pytest.fixture
def postgres(cluster: Cluster, pg_port: int):
    return Postgres(cluster, port=pg_port).install()


@pytest.fixture
def postgres_with_tls(cluster: Cluster, pg_port: int):
    return Postgres(cluster, port=pg_port, ssl=True).install()


@pytest.fixture
def postgres_with_mtls(cluster: Cluster, pg_port: int):
    return Postgres(cluster, port=pg_port, ssl=True, ssl_verify=True).install()
