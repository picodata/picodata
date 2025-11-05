import git
import io
import json
import os
import re
import filecmp
import psycopg
import shutil
import stat
import sys
import time
import threading
from packaging.version import InvalidVersion, Version
from http.server import BaseHTTPRequestHandler, HTTPServer
import requests
from prometheus_client.parser import text_string_to_metric_families
from prometheus_client import Metric

import yaml as yaml_lib  # type: ignore
import pytest
import signal
import subprocess
import msgpack  # type: ignore
from rand.params import generate_seed
from random import randint
from functools import reduce
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
)
from itertools import count
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
import tarantool
from tarantool.error import (  # type: ignore
    tnt_strerror,
    DatabaseError,
)
from multiprocessing import Process, Queue
from pathlib import Path

from framework import ldap
from framework.log import log
from framework.constants import BASE_HOST
from framework.port_distributor import PortDistributor
from framework.rolling.registry import Registry
from framework.rolling.runtime import Runtime
from framework.rolling.version import Version as RelativeVersion

pytest_plugins = "framework.sqltester"


# From raft.rs:
# A constant represents invalid id of raft.
# pub const INVALID_ID: u64 = 0;
INVALID_RAFT_ID = 0
METRICS_PORT = 7500

MAX_LOGIN_ATTEMPTS = 4
PICO_SERVICE_ID = 32

CLI_TIMEOUT = 10  # seconds

TOO_LONG_FOR_LOGS = 512


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
    NoSuchService = 10011
    ServiceNotStarted = 10012
    ServicePoisoned = 10013
    ServiceNotAvailable = 10014
    WrongPluginVersion = 10015
    NoSuchInstance = 10016
    NoSuchReplicaset = 10017
    LeaderUnknown = 10018
    PluginError = 10019
    InstanceExpelled = 10020
    ReplicasetExpelled = 10021
    InstanceUnavaliable = 10022
    CasTableNotOperable = 10023
    Uninitialized = 10024
    ExpelNotAllowed = 10025
    CasConfigNotAllowed = 10026
    RaftProposalDropped = 10027
    SbroadError = 10028

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
            RaftProposalDropped,
        ]
    )

    @classmethod
    def is_retriable_for_cas(cls, code):
        return code in cls.retriable_for_cas


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def assert_starts_with(actual_string: str | bytes, expected_prefix: str | bytes):
    """Using this function results in a better pytest output in case of assertion failure."""
    assert actual_string[: len(expected_prefix)] == expected_prefix


def shorten_expr_for_log(expr: str) -> str:
    expr = expr.strip()
    head, *tail = expr.split("\n", maxsplit=1)
    if tail and tail[0].strip():
        return head + "..."

    return head


def clamp_for_logs(*args):
    args_string = f"{args}"
    if len(args_string) > TOO_LONG_FOR_LOGS:
        return args_string[:TOO_LONG_FOR_LOGS] + "..."

    return args_string


def pytest_addoption(parser: pytest.Parser):
    parser.addoption("--seed", action="store", default=None, help="Seed for randomized tests")
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
        default=3303,  # 3301 and 3302 are needed for backwards compat tests
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


@pytest.fixture(scope="session")
def port_distributor(xdist_worker_number: int, pytestconfig) -> PortDistributor:
    assert isinstance(xdist_worker_number, int)
    assert xdist_worker_number >= 0
    global_base_port = pytestconfig.getoption("--base-port")

    ports_per_worker = (32768 - global_base_port) // int(os.environ.get("PYTEST_XDIST_WORKER_COUNT", "1"))
    worker_base_port = global_base_port + xdist_worker_number * ports_per_worker

    worker_max_port = worker_base_port + ports_per_worker

    return PortDistributor(start=worker_base_port, end=worker_max_port)


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
                            raise TarantoolError(error_type, arg, source_location) from exc
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
            case [*args]:  # type: ignore
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
        return """{{ fieldno = {}, type = "{}", is_nullable = {} }}""".format(self.fieldno, self.type, self.is_nullable)


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


class ColorCode:
    Grey = "\x1b[30m"
    Red = "\x1b[31m"
    Green = "\x1b[32m"
    Yellow = "\x1b[33m"
    Blue = "\x1b[34m"
    Magenta = "\x1b[35m"
    Cyan = "\x1b[36m"
    White = "\x1b[37m"
    IntenseGrey = "\x1b[30;1m"
    IntenseRed = "\x1b[31;1m"
    IntenseGreen = "\x1b[32;1m"
    IntenseYellow = "\x1b[33;1m"
    IntenseBlue = "\x1b[34;1m"
    IntenseMagenta = "\x1b[35;1m"
    IntenseCyan = "\x1b[36;1m"
    IntenseWhite = "\x1b[37;1m"
    Reset = "\x1b[0m"


class ProcessDead(Exception):
    pass


class NotALeader(Exception):
    pass


class CommandFailed(Exception):
    def __init__(self, stdout, stderr):
        self.stdout = maybe_decode_utf8(stdout)
        self.stderr = maybe_decode_utf8(stderr)


def maybe_decode_utf8(s: bytes) -> str | bytes:
    try:
        return s.decode()
    except UnicodeDecodeError:
        return s


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
                log.info(f"Retriable.call {func.__name__}")
                return func(*args, **kwargs)
            except self.fatal as e:
                raise e from e
            except Exception as e:
                log.info(f"got retriable exception: {e}")
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
            )[0]
        finally:
            if sudo:
                self.eval("box.session.su(...)", old_euid)

        return result


# Check whether error can be caused by instance crash.
# For example, `auth attempts limit exceeded` can't
# be cause of instance crash.
def can_cause_fail(error: Exception):
    if isinstance(error, TarantoolError):
        # FIXME after some tarantool errors there still may be a crash, we
        # should gather some statistics and update this with more conditions
        return False

    if not isinstance(error, DatabaseError):
        return True

    error_code = error.args[0].code

    # 47: ("ER_PASSWORD_MISMATCH", "Incorrect password supplied for user '%s'")
    if error_code == 47:
        return False

    # 32: ("ER_PROC_LUA", "%s")
    if error_code == 32:
        # Picodata tracks auth attempts in custom lua hook
        if error.args[0].message == "Maximum number of login attempts exceeded":
            return False

    return True


@dataclass
class Instance:
    runtime: Runtime
    cwd: str
    color_code: str

    share_dir: str | None = None
    cluster_name: str | None = None
    _instance_dir: Path | None = None
    _backup_dir: Path | None = None
    peers: list[str] = field(default_factory=list)
    host: str | None = None
    port: int | None = None
    _http_listen: str | bool | None = None

    pg_host: str | None = None
    pg_port: int | None = None
    pg_ssl: bool | None = None
    pg_ssl_cert_file: str | None = None
    pg_ssl_key_file: str | None = None
    pg_ssl_ca_file: str | None = None
    audit: str | bool = True
    tier: str | None = None
    init_replication_factor: int | None = None
    config_path: str | None = None
    name: str | None = None
    cluster_uuid: str | None = None
    replicaset_name: str | None = None
    failure_domain: dict[str, str] = field(default_factory=dict)
    _service_password: str | None = None
    service_password_file: str | None = None
    env: dict[str, str] = field(default_factory=dict)
    process: subprocess.Popen | None = None
    raft_id: int = INVALID_RAFT_ID
    _on_output_callbacks: list[Callable[[bytes], None]] = field(default_factory=list)

    iproto_tls: tuple[Path, Path, Path] | None = None
    """
    order: (CERT_FILE, KEY_FILE, CA_FILE)
    """
    iproto_tls_enabled: bool | None = None
    iproto_tls_cert: str | None = None
    iproto_tls_key: str | None = None
    iproto_tls_ca: str | None = None

    registry: Optional[Registry] = None

    @property
    def instance_dir(self):
        assert self._instance_dir
        return self._instance_dir

    @property
    def backup_dir(self):
        return self._backup_dir if self._backup_dir is not None else os.path.join(self.instance_dir, "backup")

    @property
    def iproto_listen(self):
        if self.host is None or self.port is None:
            return None
        return f"{self.host}:{self.port}"

    @property
    def pg_listen(self):
        if self.pg_host is None or self.pg_port is None:
            return None
        return f"{self.pg_host}:{self.pg_port}"

    def current_state(self, target: "Instance | None" = None) -> dict[str, str | int]:
        return self.instance_info(target)["current_state"]

    def states(self, target: "Instance | None" = None) -> tuple[str, str]:
        """Returns a pairs (current_state, target_state) but without the incarnation parts."""
        info = self.instance_info(target)
        return info["current_state"]["variant"], info["target_state"]["variant"]

    def get_tier(self):
        return "default" if self.tier is None else self.tier

    def uuid(self):
        return self.eval("return box.info.uuid")

    def replicaset_uuid(self):
        return self.eval("return box.info.cluster.uuid")

    def picodata_version(self):
        return self.call(".proc_version_info")["picodata_version"]

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

        # fmt: off
        return [
            self.runtime.command, "run",
            *([f"--cluster-name={self.cluster_name}"] if self.cluster_name else []),
            *([f"--instance-name={self.name}"] if self.name else []),
            *([f"--replicaset-name={self.replicaset_name}"] if self.replicaset_name else []),
            *([f"--instance-dir={self._instance_dir}"] if self._instance_dir else []),
            *([f"--backup-dir={self._backup_dir}"] if self._backup_dir else []),
            *([f"--share-dir={self.share_dir}"] if self.share_dir else []),
            *([f"--iproto-listen={self.iproto_listen}"] if self.iproto_listen else []),
            *([f"--pg-listen={self.pg_listen}"] if self.pg_listen else []),
            *([f"-c instance.pg.ssl={self.pg_ssl}"] if self.pg_ssl else []),
            *([f"-c instance.pg.cert_file={self.pg_ssl_cert_file}"] if self.pg_ssl and self.pg_ssl_cert_file else []),
            *([f"-c instance.pg.key_file={self.pg_ssl_key_file}"] if self.pg_ssl and self.pg_ssl_key_file else []),
            *([f"-c instance.pg.ca_file={self.pg_ssl_ca_file}"] if self.pg_ssl and self.pg_ssl_ca_file else []),
            *([f"--peer={str.join(',', self.peers)}"] if self.peers else []),
            *(f"--failure-domain={k}={v}" for k, v in self.failure_domain.items()),
            *(["--init-replication-factor", f"{self.init_replication_factor}"]
              if self.init_replication_factor is not None else []),
            *(["--config", self.config_path] if self.config_path is not None else []),
            *(["--tier", self.tier] if self.tier is not None else []),
            *(["--audit", audit] if audit else []),
            *(["-c instance.iproto_tls.enabled=true"] if self.iproto_tls_enabled else []),
            *([f"-c instance.iproto_tls.cert_file={self.iproto_tls_cert}"] if self.iproto_tls_cert else []),
            *([f"-c instance.iproto_tls.key_file={self.iproto_tls_key}"] if self.iproto_tls_key else []),
            *([f"-c instance.iproto_tls.ca_file={self.iproto_tls_ca}"] if self.iproto_tls_ca else []),
        ]
        # fmt: on

    def __repr__(self):
        if self.process:
            return f"Instance({self.name}, iproto_listen={self.iproto_listen}, cluster={self.cluster_name}, process.pid={self.process.pid})"  # noqa: E501
        else:
            return f"Instance({self.name}, iproto_listen={self.iproto_listen}, cluster={self.cluster_name})"  # noqa: E501

    def __hash__(self):
        return hash((self.cluster_name, self.name))

    def connect_via_pgproto(self, *, timeout: int | None = None, user: str, password: str):
        # TODO: if chap-sha1 is ever supported, we can default to _pico_service here.

        try:
            return psycopg.connect(
                user=user, password=password, host=self.pg_host, port=self.pg_port, connect_timeout=timeout
            )
        except Exception as e:
            if can_cause_fail(e):
                self.check_process_alive()
            # if process is dead, the above call will raise an exception
            # otherwise we raise the original exception
            raise e from e

    @contextmanager
    def connect(self, *, timeout: int | float, user: str | None = None, password: str | None = None):
        if user is None:
            user = "pico_service"
            if password is None:
                password = self.service_password

        need_iproto_tls = self.iproto_tls is not None and len(self.iproto_tls) == 3

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
                transport="ssl" if need_iproto_tls else None,
                ssl_cert_file=str(self.iproto_tls[0]) if need_iproto_tls else None,  # type: ignore
                ssl_key_file=str(self.iproto_tls[1]) if need_iproto_tls else None,  # type: ignore
                ssl_ca_file=str(self.iproto_tls[2]) if need_iproto_tls else None,  # type: ignore
            )
        except Exception as e:
            if can_cause_fail(e):
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
        log.info(f"{self.name or self.port} RPC CALL {fn}{clamp_for_logs(args)}", stacklevel=2)
        try:
            with self.connect(timeout=timeout, user=user, password=password) as conn:
                result = conn.call(fn, args)
                log.info(f"{self.name or self.port} RPC CALL {fn} result: {clamp_for_logs(result)}", stacklevel=2)
                return result
        except Exception as e:
            log.error(f"{self.name or self.port} RPC CALL {fn} failed: {e}", stacklevel=2)
            if can_cause_fail(e):
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
        # NOTE: Not using short_expr at first intentionally
        log.info(f"{self.name or self.port} RPC EVAL `{expr}` {clamp_for_logs(args)}", stacklevel=2)
        short_expr = shorten_expr_for_log(expr)
        try:
            with self.connect(timeout=timeout, user=user, password=password) as conn:
                result = conn.eval(expr, *args)
                log.info(
                    f"{self.name or self.port} RPC EVAL `{short_expr}` result: {clamp_for_logs(result)}",
                    stacklevel=2,
                )
                return result
        except Exception as e:
            log.error(f"{self.name or self.port} RPC EVAL `{short_expr}` failed: {e}", stacklevel=2)
            if can_cause_fail(e):
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
            log.info(f"{self} killed")
        self.process = None

    def hash(self, tup: tuple, key_def: KeyDef) -> int:
        tup_str = "{{ {} }}".format(", ".join(str(x) for x in tup))
        lua = """
            return require("key_def").new({kd}):hash(box.tuple.new({t}))
        """.format(t=tup_str, kd=str(key_def))
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
        # NOTE: Not using short_expr at first intentionally
        log.info(f"{self.name or self.port} RPC SQL `{sql}` {clamp_for_logs(params)} {options}", stacklevel=2)
        short_sql = shorten_expr_for_log(sql)
        try:
            with self.connect(timeout=timeout, user=user, password=password) as conn:
                result = conn.sql(sql, sudo=sudo, *params, options=options)
                log.info(
                    f"{self.name or self.port} RPC SQL `{short_sql}` result: {clamp_for_logs(result)}",
                    stacklevel=2,
                )
            if strip_metadata and "rows" in result:
                return result["rows"]
            return result
        except Exception as e:
            log.error(f"{self.name or self.port} RPC SQL to `{short_sql}` failed: {e}", stacklevel=2)
            if can_cause_fail(e):
                self.check_process_alive()
            raise e from e

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
                log.warning(f"retrying SQL query `{sql}` ({attempt=})")

            return self.sql(sql, *params, sudo=sudo, user=user, password=password, timeout=timeout)

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

        log.info(f"termintating instance {self}")

        with suppress(ProcessLookupError, PermissionError):
            os.killpg(self.process.pid, signal.SIGCONT)

        self.process.terminate()

        try:
            rc = self.process.wait(timeout=kill_after_seconds)
            log.info(f"{self} terminated: rc = {rc}")
            self.process = None
            return rc
        finally:
            self.kill()

    def _process_output(self, src, out: io.TextIOWrapper):
        is_a_tty = sys.stdout.isatty()

        def make_prefix():
            id = self.name or f":{self.port}"
            prefix = f"{id:<11} | "

            if is_a_tty:
                prefix = self.color_code + prefix + ColorCode.Reset

            return prefix.encode("utf-8")

        prefix = make_prefix()
        name_was = self.name
        # `iter(callable, sentinel)` form: calls callable until it returns sentinel
        for line in iter(src.readline, b""):
            # Update the log prefix if the name changed. This is needed because
            # when an instance starts up without an explicit --instance-name we
            # only find out it's name after it has initialized.
            # Do this here, because this is where the thread wakes up after
            # blocking on the input stream.
            if name_was != self.name:
                name_was = self.name
                prefix = make_prefix()

            with OUT_LOCK:
                out.buffer.write(prefix)
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

        log.info(f"{self} starting...")

        if peers is not None:
            self.peers = list(map(lambda i: i.iproto_listen, peers))

        env = {**self.env}

        # Set a default, but let os.environ override it
        if "PICODATA_LOG_LEVEL" not in env:
            env["PICODATA_LOG_LEVEL"] = "verbose"

        # Enable dumping panic backtraces to a file, so that we can report
        # panic in `Instance.check_process_alive`
        env["PICODATA_INTERNAL_BACKTRACE_DUMP"] = "1"

        passthrough_env = [
            "ASAN_OPTIONS",
            "ASAN_SYMBOLIZER_PATH",
            "LSAN_OPTIONS",
            "PATH",
            "PICODATA_LOG_LEVEL",
            "RUST_BACKTRACE",
            "TERM",
        ]
        for key in passthrough_env:
            val = os.environ.get(key)
            if val is not None:
                env[key] = val

        if os.getenv("NOLOG"):
            out = subprocess.DEVNULL
        else:
            out = subprocess.PIPE

        self.process = subprocess.Popen(
            self.command,
            cwd=cwd or self.cwd,
            env=env,
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

    def fail_to_start(self, timeout: int = 10, rolling: bool = False):
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

            if rolling:
                return
            else:
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
            self.remove_backup()

        self.start()

    def remove_data(self):
        log.info(f"removing instance_dir of {self}")
        shutil.rmtree(self.instance_dir)

    def http_listen(self):
        if isinstance(self._http_listen, str):
            return self._http_listen

        def error_message():
            return f"Http server is disabled on instance {self}\nconsider passing enable_http=True to Cluster.add_instance()"

        if self._http_listen is False:
            raise Exception(error_message())

        assert self._http_listen is None

        info = self.call(".proc_runtime_info")
        if "http" not in info:
            self._http_listen = False
            raise Exception(error_message())

        http = info["http"]
        self._http_listen = "{}:{}".format(http["host"], http["port"])

        return self._http_listen

    def get_metrics(self) -> dict[str, Metric]:
        response = requests.get(f"http://{self.http_listen()}/metrics")
        response.raise_for_status()

        metrics = dict()
        for metric in text_string_to_metric_families(response.text):
            metrics[metric.name] = metric

        return metrics

    def remove_backup(self):
        path = Path(self.backup_dir)
        log.info(f"removing backup_dir of {self}")
        if path.exists():
            shutil.rmtree(path)

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
    ) -> tuple[int, int]:
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
        res = self.call(".proc_cas_v2", self.cluster_name, predicate, dml, as_user)

        return (res["index"], res["res_row_count"])

    def pico_property(self, key: str):
        tup = self.call("box.space._pico_property:get", key)
        if tup is None:
            return None

        return tup[1]

    def next_schema_version(self) -> int:
        return self.pico_property("next_schema_version") or 1

    def create_table(self, params: dict, timeout: float = 10) -> int:
        """
        DEPRECATED: use Instance.sql(...) instead of this function.

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
            raise Exception(f"Wrong distribution: {params['distribution']}.Possible options: global, sharded")

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
        DEPRECATED: use Instance.sql(...) instead of this function

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

    def propose_create_space(self, space_def: Dict[str, Any], wait_index: bool = True, timeout: int = 10) -> int:
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
            # the process. So we introduce a tiny timeout here (which may still not
            # be enough in every case).
            exit_code = self.process.wait(timeout=1)  # type: ignore
        except subprocess.TimeoutExpired:
            # it's fine, the process is still running
            pass
        else:
            message = f"process exited unexpectedly, {exit_code=}"
            pid = self.process.pid
            bt = os.path.join(self.cwd, f"picodata-{pid}.backtrace")
            if os.path.exists(bt):
                with open(bt, "r") as f:
                    backtrace = f.read()
                message += "\n\n"
                message += backtrace

            if self._instance_dir:
                log_path = self._instance_dir / "picodata.log"
                if os.path.exists(log_path):
                    message += "\n"
                    message += f"instance logs have been written to '{log_path}'"

            raise ProcessDead(message)

    def assert_process_dead(self):
        try:
            self.check_process_alive()
            assert False
        except ProcessDead:
            # Make it so we can call Instance.start later
            self.process = None

    def instance_info(self, target: "Instance | None" = None, timeout: int | float = 10) -> dict[str, Any]:
        """Call .proc_instance_info on the instance
        and update the related properties on the object.
        """
        if not target:
            # Normalize the arguments
            target = self

        info = self.call(".proc_instance_info", target.name, timeout=timeout)
        assert isinstance(info, dict)

        assert isinstance(info["raft_id"], int)
        target.raft_id = info["raft_id"]

        assert isinstance(info["name"], str)
        target.name = info["name"]

        assert isinstance(info["replicaset_name"], str)
        target.replicaset_name = info["replicaset_name"]

        assert isinstance(info["cluster_name"], str)
        target.cluster_name = info["cluster_name"]

        assert isinstance(info["cluster_uuid"], str)
        target.cluster_uuid = info["cluster_uuid"]

        return info

    def wait_online(
        self,
        timeout: int | float = 30,
        rps: int | float = 5,
        expected_incarnation: int | None = None,
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
                state = self.current_state()
                if state != last_state:
                    last_state = state
                    deadline = time.monotonic() + timeout

                # Check state
                assert state["variant"] == "Online"
                if expected_incarnation is not None:
                    assert state["incarnation"] == expected_incarnation

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

        if self._instance_dir:
            instance_dir_by_name = Path(self._instance_dir).parent / self.name  # type: ignore
            if not instance_dir_by_name.exists():
                os.symlink(self._instance_dir, instance_dir_by_name, target_is_directory=True)
                log.info(f"created a symlink {self._instance_dir} -> {instance_dir_by_name}")

        log.info(f"{self} is online")

    def wait_has_states(
        self,
        current_state: str,
        target_state: str,
        target: "Instance | None" = None,
        timeout: int | float = 30,
        rps: int | float = 5,
    ):
        """Block until instance has the given states.
        Note that this seems similar to Instance.wait_online, but is a little bit
        different.
        """
        assert not (target == self and current_state == "Online" and target_state == "Online"), (
            "use Instance.wait_online() instead"
        )

        def check():
            states = self.states(target)
            assert states == (current_state, target_state)

        Retriable(timeout, rps, fatal=ProcessDead).call(check)

    def raft_term(self) -> int:
        """Get current raft `term`"""

        return self.call(".proc_raft_info")["term"]

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

    def raft_leader_id(self):
        return self.call(".proc_raft_leader_id")

    def raft_leader_uuid(self):
        return self.call(".proc_raft_leader_uuid")

    def get_vclock(self) -> Dict[int, int]:
        """Get current vclock"""

        return self.call(".proc_get_vclock")

    def wait_vclock(self, target: Dict[int, int], timeout: int | float = 10) -> int:
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
    ) -> int:
        assert expected_status != "not a leader", "use another function"

        def impl():
            info = self.call(".proc_runtime_info")["internal"]
            actual_status = info["governor_loop_status"]
            if actual_status == "not a leader":
                raise NotALeader("not a leader")

            step_counter = info["governor_step_counter"]
            if old_step_counter:
                assert old_step_counter != step_counter

            assert actual_status == expected_status

            return step_counter

        return Retriable(timeout=timeout, rps=1, fatal=NotALeader).call(impl)

    def promote_or_fail(self):
        attempt = 0

        def make_attempt(timeout, rps):
            nonlocal attempt
            attempt = attempt + 1
            log.info(f"{self} is trying to become a leader, {attempt=}")

            # 1. Force the node to campaign.
            self.call(".proc_raft_promote")

            # 2. Wait until the miracle occurs.
            Retriable(timeout, rps).call(self.assert_raft_status, "Leader")

        Retriable(timeout=10, rps=1).call(make_attempt, timeout=1, rps=10)
        log.info(f"{self} is a leader now")

    def grant_privilege(self, user, privilege: str, object_type: str, object_name: Optional[str] = None):
        if privilege == "execute" and object_type == "role":
            self.sql(f'GRANT "{object_name}" TO "{user}"', sudo=True)
        elif object_name:
            self.sql(
                f'GRANT {privilege} ON {object_type} "{object_name}" TO "{user}"',
                sudo=True,
            )
        else:
            self.sql(f'GRANT {privilege} {object_type} TO "{user}"', sudo=True)

    def revoke_privilege(self, user, privilege: str, object_type: str, object_name: Optional[str] = None):
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

    def set_service_password(self, password: str):
        if not self.instance_dir:
            raise ValueError("instance_dir must be set before setting the service password")

        cookie_file = os.path.join(self.instance_dir, ".picodata-cookie")
        self._service_password = password
        self.service_password_file = cookie_file
        os.makedirs(os.path.dirname(cookie_file), exist_ok=True)
        with open(self.service_password_file, "w") as f:
            f.write(self._service_password)
        os.chmod(self.service_password_file, 0o600)

    def set_config_file(self, config_path: str):
        self.config_path = config_path

    def setup_logging(self, log_to_console: bool | None, log_to_file: bool | None, env_instance_log: str | None):
        # Choose default values for options
        if env_instance_log:
            assert env_instance_log in ["file", "console", "both", "none"], (
                "invalid value for INSTANCE_LOG environment variable"
            )
            if log_to_file is None:
                log_to_file = env_instance_log in ["file", "both"]
            if log_to_console is None:
                log_to_console = env_instance_log in ["console", "both"]
        else:
            # By default DON't log to file
            if log_to_file is None:
                log_to_file = False
            # By default log to console
            if log_to_console is None:
                log_to_console = True

        assert isinstance(log_to_console, bool)
        assert isinstance(log_to_file, bool)

        if log_to_file and log_to_console:
            self.env["PICODATA_LOG"] = f"|tee {self.instance_dir}/picodata.log"
        elif log_to_file:
            self.env["PICODATA_LOG"] = f"{self.instance_dir}/picodata.log"
        elif not log_to_console:
            self.env["PICODATA_LOG"] = "/dev/null"

    @property
    def service_password(self) -> Optional[str]:
        if self.service_password_file is None:
            return None
        if not os.path.exists(self.service_password_file):
            return None
        with open(self.service_password_file, "r") as f:
            password = f.readline()
            if password.endswith("\n"):
                password = password[:-1]
        return password

    def is_healthy(self) -> bool:
        if self.current_state()["variant"] != "Online":
            return False

        res_all = self.sql("SELECT COUNT(*) FROM _pico_governor_queue")
        res_done = self.sql("SELECT COUNT(*) FROM _pico_governor_queue WHERE status = 'done'")
        if res_all != res_done:
            return False

        tt_procs = [
            "proc_backup_abort_clear",
            "proc_apply_backup",
            "proc_internal_script",
        ]
        for proc_name in tt_procs:
            res = self.call("box.space._func.index.name:select", [f".{proc_name}"])
            if res[0][2] != f".{proc_name}":
                return False

        res = self.sql("SELECT name, is_default FROM _pico_tier")
        if res != [["default", True]]:
            return False

        res = self.sql("SELECT format FROM _pico_table WHERE id = 523")
        if res[0][0][7] != {"field_type": "boolean", "is_nullable": True, "name": "is_default"}:
            return False

        res = self.call("box.space._space:select", [523])
        if res[0][6][7] != {"type": "boolean", "is_nullable": True, "name": "is_default"}:
            return False

        res = self.sql("SELECT value FROM _pico_property WHERE key = 'system_catalog_version'")
        if res[0][0] not in ["25.3.7", "25.4.1", "25.5.1"]:
            return False

        self.sql("AUDIT POLICY dml_default BY pico_service")
        res = self.sql("SELECT * FROM _pico_user_audit_policy")
        if res != [[32, 0]]:
            return False

        self.sql("AUDIT POLICY dml_default EXCEPT pico_service")
        res = self.sql("SELECT * FROM _pico_user_audit_policy")
        if res != []:
            return False

        return True

    def is_ceased(self) -> bool:
        try:
            print(f"{self.name}: called is_ceased")
            return not self.is_healthy()
        except ProcessDead:
            return True

    def change_version(
        self,
        to: RelativeVersion,
        fail: bool = False,
    ) -> None:
        print(
            f"instance_name={self.name}: changing version [from={self.runtime.absolute_version}, to={to}], should fail? {fail}"
        )

        assert self.registry

        self.terminate()
        self.runtime = self.registry.get(to)
        self.fail_to_start(rolling=True) if fail else self.start_and_wait()

    def fill_with_data(self):
        ddl = self.sql(
            """
            CREATE TABLE deliveries (
                nmbr INTEGER NOT NULL,
                product TEXT NOT NULL,
                quantity INTEGER,
                PRIMARY KEY (nmbr))
            USING vinyl DISTRIBUTED BY (product)
            IN TIER "default";
        """
        )
        assert ddl["row_count"] == 1

        dml = self.sql(
            """
            INSERT INTO deliveries VALUES
                (1, 'metalware', 2000),
                (2, 'adhesives', 300),
                (3, 'moldings', 100),
                (4, 'bars', 5),
                (5, 'blocks', 15000);
        """
        )
        assert dml["row_count"] == 5

        dcl = self.sql("CREATE ROLE toy OPTION (TIMEOUT = 3.0)")
        assert dcl["row_count"] == 1

        dcl = self.sql("GRANT ALTER ON TABLE deliveries TO toy")
        assert dcl["row_count"] == 1

        dcl = self.sql(
            """
            CREATE USER "andy"
            WITH PASSWORD 'P@ssw0rd'
            USING chap-sha1;
        """
        )
        assert dcl["row_count"] == 1

        ddl = self.sql(
            """
            CREATE INDEX product_quantity
            ON deliveries
            USING TREE (product, quantity)
            WITH (
                HINT = true,
                BLOOM_FPR = 0.05,
                RUN_SIZE_RATIO = 3.5,
                PAGE_SIZE = 8192,
                RANGE_SIZE = 1073741824,
                RUN_COUNT_PER_LEVEL = 2
            );
        """
        )
        assert ddl["row_count"] == 1

        ddl = self.sql(
            """
            CREATE PROCEDURE proc (int, text, int)
            AS $$INSERT INTO deliveries VALUES($1, $2, $3)$$;
        """
        )
        assert ddl["row_count"] == 1

        lua = self.eval("return box.snapshot()")
        assert lua == "ok"


CLUSTER_COLORS = (
    ColorCode.Cyan,
    ColorCode.Yellow,
    ColorCode.Green,
    ColorCode.Magenta,
    ColorCode.Blue,
    ColorCode.IntenseCyan,
    ColorCode.IntenseYellow,
    ColorCode.IntenseGreen,
    ColorCode.IntenseMagenta,
    ColorCode.IntenseBlue,
)


@dataclass
class Cluster:
    runtime: Runtime
    id: str
    data_dir: str
    base_host: str
    port_distributor: PortDistributor
    instances: list[Instance] = field(default_factory=list)
    config_path: str | None = None
    service_password: str | None = None
    service_password_file: str | None = None
    share_dir: str | None = None

    # Instance which can be used for interaction with the cluster. It is cached
    # so that we don't have to do expensive RPC to figure out an instance to
    # connect to
    peer: Instance | None = None

    registry: Optional[Registry] = None

    def __repr__(self):
        ports = ",".join(str(i.port) for i in self.instances)
        return f'Cluster("{self.base_host}:{{{ports}}}", n={len(self.instances)})'

    def __getitem__(self, item: int) -> Instance:
        return self.instances[item]

    def set_unique_configs_for_instances(
        self,
        *,
        init_replication_factor: int | None = None,
        share_dir_path: Path | None = None,
    ):
        """Set instance config based on unique instance fields.

        Initially after cluster deploy each instance is pointing to the single
        cluster config file. This function is called after all instances deployment
        in order to set unique config for each instance.
        """

        # Generate default config.
        data = subprocess.check_output([self.runtime.command, "config", "default"])
        default_config = data.decode()
        config_yaml_obj = yaml_lib.safe_load(default_config)

        # Fix config values which we generate in our test framework.
        for i in self.instances:
            config_yaml_obj_i = config_yaml_obj

            if share_dir_path:
                config_yaml_obj_i["instance"]["share_dir"] = str(share_dir_path)

            config_yaml_obj_i["instance"]["admin_socket"] = str(os.path.join(i.instance_dir, "admin.sock"))
            config_yaml_obj_i["instance"]["name"] = i.name
            config_yaml_obj_i["instance"]["replicaset_name"] = i.replicaset_name
            config_yaml_obj_i["instance"]["tier"] = i.tier
            config_yaml_obj_i["instance"]["instance_dir"] = str(i.instance_dir)
            config_yaml_obj_i["instance"]["backup_dir"] = str(i.backup_dir)
            config_yaml_obj_i["instance"]["iproto_listen"] = i.iproto_listen
            config_yaml_obj_i["instance"]["iproto_advertise"] = i.iproto_listen
            if init_replication_factor:
                config_yaml_obj_i["cluster"]["default_replication_factor"] = init_replication_factor

                if i.tier:
                    tier_to_set = i.tier
                else:
                    tier_to_set = "default"
                config_yaml_obj_i["cluster"]["tier"][tier_to_set]["replication_factor"] = init_replication_factor
            config_yaml_obj_i["instance"]["peer"] = [i.iproto_listen]
            config_yaml_obj_i["instance"]["pg"]["listen"] = i.pg_listen
            config_yaml_obj_i["instance"]["pg"]["advertise"] = i.pg_listen

            config_path = os.path.join(i.instance_dir, "picodata.yaml")
            with open(config_path, "w") as f:
                yaml_lib.safe_dump(config_yaml_obj_i, f, default_flow_style=False)

            i.set_config_file(config_path)

    def restore(
        self,
        backup_path: str,
        masters_number: int | None = None,  # Passed for rebalancer awaiting.
        *,
        is_absolute: bool | None = None,
        config_name: str | None = None,
    ):
        """Clusterwide restore.

        Should be used as a landmark for creation of ansible role
        responsible for clusterwide restore.
        """

        def get_instance_full_backup_path(i: Instance):
            if is_absolute:
                i_backup_full_path = backup_path
            else:
                backup_dir = i.backup_dir
                i_backup_full_path = os.path.join(backup_dir, backup_path)
            return i_backup_full_path

        def restore_instance(i: Instance):
            i_backup_full_path = get_instance_full_backup_path(i)
            log.info(f"executing: `picodata restore` on instance {i} and backup path {i_backup_full_path}")
            try:
                command = [
                    self.runtime.command,
                    "restore",
                    "--path",
                    i_backup_full_path,
                    *(["--config", config_name] if config_name is not None else []),
                ]

                # TODO: See https://git.picodata.io/core/picodata/-/issues/2187.
                subprocess.check_output(command, stderr=subprocess.PIPE)
            except subprocess.CalledProcessError as e:
                raise CommandFailed(e.stdout, e.stderr) from e

        # For each instance in cluster check that it contains needed backup dir.
        # Probably clusterwide restore is executed on a cluster with different number of
        # instances.
        #
        # TODO: At that moment we know the topology of
        #       * current cluster
        #       * cluster from which backup is taken
        #       We should also compare them and throw error
        #       in case they differ.
        #       See https://git.picodata.io/core/picodata/-/issues/2188.
        for i in self.instances:
            i_backup_full_path = get_instance_full_backup_path(i)
            assert os.path.exists(i_backup_full_path), (
                f"Backup does not exist on the path: {i_backup_full_path} for {i}"
            )

        # Stop all instances
        self.terminate()

        # Call `picodata restore` on each instance
        for i in self.instances:
            restore_instance(i)

        # Wait until all buckets are known and vshard functions normally.
        # TODO: The reason of adding crawlers waiting is that
        #       vshard is not initialized sometimes before
        #       `wait_until_instance_has_this_many_active_buckets` call.
        #       Why`wait_online` doesn't help with that?
        #       See https://git.picodata.io/core/picodata/-/issues/2188.
        log_crawlers = []

        # Back instacnes to live
        for i in self.instances:
            i.start()
        for i in self.instances:
            if i.replicaset_master_name() == i.name:
                lc = log_crawler(i, "Discovery enters idle mode, all buckets are known")
                log_crawlers.append(lc)
            i.wait_online()

        for lc in log_crawlers:
            lc.wait_matched(timeout=30)

        # Wait for rebalance to complete
        if masters_number:
            for i in self.instances:
                if i.replicaset_master_name() == i.name:
                    self.wait_until_instance_has_this_many_active_buckets(i, int(3000 / masters_number))

    def deploy(
        self,
        *,
        instance_count: int,
        init_replication_factor: int | None = None,
        tier: str | None = None,
        service_password: str | None = None,
        audit: bool | str = True,
        wait_online: bool = True,
    ) -> list[Instance]:
        """Deploy a cluster of instances.

        The instances are deployed without any guaranteed order.
        If an ascending order by name is required,
        sort the resulting list using the instance's name attribute.
        """
        assert not self.instances, "Already deployed"

        if not service_password:
            service_password = "password"

        self.set_service_password(service_password)

        for _ in range(instance_count):
            print(
                f"cluster_id={self.id}: deploying instance [version={self.runtime.absolute_version} ({self.runtime.relative_version})]"
            )
            self.add_instance(
                wait_online=False,
                tier=tier,
                init_replication_factor=init_replication_factor,
                audit=audit,
            )

        if wait_online:
            return self.wait_online()
        else:
            return self.instances

    def reset(self):
        self.kill()
        self.remove_data()
        self.instances = []
        self.peer = None

    def wait_online(self, timeout: int = 30) -> list[Instance]:
        for instance in self.instances:
            instance.start()

        deadline = time.time() + timeout

        not_online = self.instances[:]
        while not_online:
            if len(not_online) == 1:
                instance = not_online.pop()
                instance.wait_online(timeout=deadline - time.time())
                break

            i = 0
            while i < len(not_online):
                instance = not_online[i]
                try:
                    instance.wait_online(timeout=0.2)
                    not_online.pop(i)
                except ProcessDead as e:
                    raise e from e
                except Exception as e:
                    if time.time() > deadline:
                        raise e from e
                    else:
                        # This instance is not yet online, check if maybe one
                        # of the others is dead
                        i += 1

        log.info(f" {self} deployed ".center(80, "="))
        return self.instances

    def set_config_file(self, config: dict | None = None, yaml: str | None = None):
        assert config or yaml
        assert self.config_path is None

        self.config_path = self.data_dir + "/picodata.yaml"

        if config:
            yaml = yaml_lib.dump(config, default_flow_style=False)

        assert yaml
        with open(self.config_path, "w") as yaml_file:
            yaml_file.write(yaml)

    def set_share_dir(self, share_dir: str):
        self.share_dir = share_dir

        for i in self.instances:
            i.share_dir = share_dir

    def set_service_password(self, service_password: str):
        self.service_password = service_password
        self.service_password_file = os.path.join(self.data_dir, ".picodata-cookie")
        with open(self.service_password_file, "w") as f:
            f.write(service_password)
        os.chmod(self.service_password_file, 0o600)

    def add_instance(
        self,
        wait_online=True,
        peers: list[str] | None = None,
        name: str | None = None,
        replicaset_name: str | None = None,
        failure_domain=dict(),
        init_replication_factor: int | None = None,
        tier: str | None = None,
        audit: bool | str = True,
        enable_http: bool = False,
        pg_port: int | None = None,
        service_password: str | None = None,
        backup_dir: Path | None = None,
        log_to_console: bool | None = None,
        log_to_file: bool | None = None,
    ) -> Instance:
        """Add an `Instance` into the list of instances of the cluster and wait
        for it to attain Online grade unless `wait_online` is `False`.

        `name` specifies how the instance's name is generated in the
        following way:

        - if `name` is a string, it will be used as a value for the
          `--instance-name` command-line option.

        - if `name` is None, cluster will automatically generate a name for instance
        """

        port = self.port_distributor.get()
        if not self.instances:
            bootstrap_port = port
        else:
            assert isinstance(self.instances[0].port, int)  # for mypy
            bootstrap_port = self.instances[0].port

        # NOTE: because we literally skip the redefinition
        # of a pgproto port at the config stage, we have to
        # pass them manually and check it's correctness
        if pg_port is None:
            pg_port = self.port_distributor.get()
            if bootstrap_port == pg_port or port == pg_port:
                pg_port = self.port_distributor.get()

        instance_dir = self.choose_instance_dir(name or str(port))

        if log_to_file:
            os.makedirs(instance_dir)

        instance = Instance(
            runtime=self.runtime,
            cwd=self.data_dir,
            cluster_name=self.id,
            name=name,
            replicaset_name=replicaset_name,
            _instance_dir=instance_dir,
            _backup_dir=backup_dir,
            share_dir=self.share_dir,
            host=self.base_host,
            port=port,
            pg_host=self.base_host,
            pg_port=pg_port,
            peers=peers or [f"{self.base_host}:{bootstrap_port}"],
            color_code=CLUSTER_COLORS[len(self.instances) % len(CLUSTER_COLORS)],
            failure_domain=failure_domain,
            init_replication_factor=init_replication_factor,
            tier=tier,
            config_path=self.config_path,
            audit=audit,
            registry=self.registry,
        )

        instance.setup_logging(log_to_console, log_to_file, os.getenv("INSTANCE_LOG"))

        if service_password:
            instance.set_service_password(service_password)
        elif self.service_password:
            instance.set_service_password(self.service_password)

        if enable_http:
            listen = f"{self.base_host}:{self.port_distributor.get()}"
            instance.env["PICODATA_HTTP_LISTEN"] = listen

        # Tweak the on_shutdown timeout so that tests don't run for too long
        instance.env["PICODATA_INTERNAL_ON_SHUTDOWN_TIMEOUT"] = "5"

        self.instances.append(instance)

        if wait_online:
            instance.start()
            instance.wait_online()

        return instance

    def fail_to_add_instance(
        self,
        peers=None,
        name: str | None = None,
        failure_domain=dict(),
        init_replication_factor: int | None = None,
        tier: str = "storage",
    ):
        instance = self.add_instance(
            wait_online=False,
            peers=peers,
            name=name,
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
        force: bool = False,
        timeout: int = 30,
    ):
        peer = self.leader(peer)
        assert self.service_password_file, "cannot expel without pico_service password"

        picodata_expel(peer=peer, target=target, password_file=self.service_password_file, force=force, timeout=timeout)

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
        DEPRECATED: use Instance.sql(...) instead of this function.

        Creates a table. Waits for all online peers to be aware of it.
        """
        index = self.leader().create_table(params, timeout)
        self.raft_wait_index(index, timeout)

    def drop_table(self, space: int | str, timeout: float = 3.0):
        """
        DEPRECATED: use Instance.sql(...) instead of this function.

        Drops the space. Waits for all online peers to be aware of it.
        """
        index = self.leader().drop_table(space, timeout)
        self.raft_wait_index(index, timeout)

    def abort_ddl(self, timeout: float = 3.0):
        """
        Aborts a pending ddl. Waits for all peers to be aware of it.
        """
        index = self.leader().abort_ddl(timeout)
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
    ) -> tuple[int, int, int]:
        instance = self.leader(instance)

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

        return instance.call("pico.batch_cas", dict(ops=ops), predicate, user=user, password=password)

    def leader(self, peer: Instance | None = None) -> Instance:
        raft_info = None
        if peer:
            try:
                raft_info = peer.call(".proc_raft_info")
                self.peer = peer
            except (ProcessDead, ConnectionRefusedError):
                pass

        if not raft_info and self.peer:
            try:
                raft_info = self.peer.call(".proc_raft_info")
            except (ProcessDead, ConnectionRefusedError):
                self.peer = None

        if not raft_info:
            for instance in self.instances:
                try:
                    raft_info = instance.call(".proc_raft_info")
                    self.peer = instance
                    break
                except (ProcessDead, ConnectionRefusedError):
                    pass

        assert raft_info, "no instances alive, are you sure the cluster is deployed?"

        assert self.peer
        if raft_info["state"] == "Leader":
            return self.peer

        connection_type = "iproto"
        leader_id = raft_info["leader_id"]
        if leader_id == 0:
            # Do not use `assert` because Cluster.wait_has_states considers
            # AssertionError to be fatal, but in this case we just want to retry
            raise Exception("leader unknown")

        [[leader_address]] = self.peer.sql(
            "SELECT address FROM _pico_peer_address WHERE raft_id = ? and connection_type = ?",
            leader_id,
            connection_type,
        )
        self.peer = self.get_instance_by_address(leader_address)
        return self.peer

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
        peer: Instance | None = None,
        user: str | None = None,
    ) -> tuple[int, int]:
        """
        Performs a clusterwide compare and swap operation.

        E.g. it checks the `predicate` on leader and if no conflicting entries were found
        appends the `op` to the raft log and returns its index.

        Calling this operation will route CaS request to a leader.
        """

        leader = self.leader(peer)

        # ADMIN by default
        user_id = 1
        if user:
            [[user_id]] = leader.sql("SELECT id FROM _pico_user WHERE name = ?", user)
            user_id = int(user_id)

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

    def wait_until_buckets_balanced(self, max_retries: int = 10):
        """
        Waits until all instances get the same amount of buckets.
        Raises if no instances are running in the current cluster.
        Total amount of buckets is taken from tiers configuration.
        """

        total_instances = len(self.instances)
        total_buckets = self.instances[0].sql("SELECT bucket_count FROM _pico_tier")
        uniform_buckets = total_buckets[0][0] / total_instances

        for instance in self.instances:
            self.wait_until_instance_has_this_many_active_buckets(instance, uniform_buckets)

    def wait_until_instance_has_this_many_active_buckets(
        self,
        i: Instance,
        expected: int,
        max_retries: int = 10,
    ):
        attempt = 1
        previous_active = None
        while True:
            instances_with_rebalancer = []
            for j in self.instances:
                if not j.process:
                    continue
                has_rebalancer = j.eval(
                    """
                    for _, router in pairs(pico.router) do
                        -- The discovery fiber is responsible for updating the
                        -- router's bucket to replicaset mapping cache.
                        router:discovery_wakeup()
                    end

                    -- The recovery and gc fibers handle any buckets left in a transitionary state
                    -- in case rebalancing process got stopped abruptly. This can happen for example
                    -- if an instance is joined while rebalancing is in progress.
                    vshard.storage.recovery_wakeup()
                    vshard.storage.garbage_collector_wakeup()

                    if vshard.storage.internal.rebalancer_fiber ~= nil then
                        -- The rebalancer fiber is responsible for sending sharded
                        -- data between storages.
                        vshard.storage.rebalancer_wakeup()
                        return true
                    end
                    return false
                """
                )
                if has_rebalancer:
                    instances_with_rebalancer.append(j.name or j.port)
            if len(instances_with_rebalancer) > 1:
                log.error(f"multiple instances are running vshard rebalancer: {instances_with_rebalancer}")

            actual_active = Retriable(timeout=10, rps=4).call(lambda: i.call("vshard.storage.info")["bucket"]["active"])
            if actual_active == expected:
                return

            log.warning(
                f"\x1b[33mwaiting for bucket rebalancing on {i.name}, was: {previous_active}, became: {actual_active} (#{attempt})\x1b[0m"  # noqa: E501
            )

            if actual_active == previous_active:
                if attempt < max_retries:
                    attempt += 1
                else:
                    if len(instances_with_rebalancer) == 0:
                        message = "vshard.rebalancer is not running anywhere!"
                    elif len(instances_with_rebalancer) == 1:
                        message = f"vshard.rebalancer is lagging (running on {instances_with_rebalancer[0]})"
                    else:
                        message = f"more than 1 vshard.rebalancer: {instances_with_rebalancer}"
                    log.error(f"vshard.storage.info.bucket.active stopped changing: {message}")
                    assert actual_active == expected, message
            else:
                # Something changed -> reset retry counter
                attempt = 1

            previous_active = actual_active

            time.sleep(0.5)

    def wait_has_states(
        self,
        target: "Instance | None",
        current_state: str,
        target_state: str,
        timeout: int | float = 30,
        rps: int | float = 5,
    ):
        def do_attempt():
            leader = self.leader()
            leader.wait_has_states(current_state, target_state, target=target, timeout=timeout, rps=rps)

        # This retriable call is needed so that we can handled leader changes
        Retriable(fatal=AssertionError, timeout=timeout, rps=rps).call(do_attempt)

    def masters(self) -> List[Instance]:
        leader = self.leader()
        master_names = leader.sql("SELECT current_master_name FROM _pico_replicaset")
        master_names = set(name for [name] in master_names)
        ret = []
        for instance in self.instances:
            if instance.name in master_names:
                ret.append(instance)

        return ret

    def grant_box_privilege(self, user, privilege: str, object_type: str, object_name: Optional[str] = None):
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

    def choose_instance_dir(self, wanted_name: str) -> Path:
        wanted_path = f"{self.data_dir}/{wanted_name}"

        candidate = wanted_path
        n = 2
        while os.path.exists(candidate):
            candidate = f"{wanted_path}-{n}"
            n += 1

        return Path(candidate)

    def is_healthy(
        self,
        exclude: List[Instance] = list(),
    ) -> bool:
        for instance in self.instances:
            if instance not in exclude:
                if instance.is_ceased() and not instance.is_healthy():
                    return False
        return True

    def is_ceased(
        self,
        exclude: List[Instance] = list(),
    ) -> bool:
        for instance in self.instances:
            if instance not in exclude:
                if not instance.is_ceased() and instance.is_healthy():
                    return False
        return True

    def change_version(
        self,
        to: RelativeVersion,
        exclude: List[Instance] = list(),
        fail: bool = False,
    ) -> None:
        print(
            f"cluster_id={self.id}: changing version [from={self.runtime.absolute_version}, to={to}], should fail? {fail}"
        )

        assert self.registry
        self.runtime = self.registry.get(to)

        for instance in self.instances:
            if instance not in exclude:
                instance.change_version(to, fail)

    def pick_random_instance(
        self,
        exclude: List[Instance] = list(),
    ) -> Instance:
        index = randint(0, len(self.instances) - 1)
        pick = self.instances[index]
        if pick not in exclude:
            return pick
        else:
            return self.pick_random_instance(exclude)

    def wait_balanced(self, buckets_total: int = 3000, *, max_retries: int = 10):
        if not self.instances:
            raise RuntimeError("wait_balanced() called on an empty cluster")

        assert buckets_total % len(self.instances) == 0, "buckets_total must be divisible by the number of instances"

        expected_per_instance = buckets_total // len(self.instances)
        for instance in self.instances:
            self.wait_until_instance_has_this_many_active_buckets(
                instance,
                expected_per_instance,
                max_retries=max_retries,
            )


def picodata_expel(
    *, peer: Instance, target: Instance, password_file: str | None, force: bool = False, timeout: int = 30
):
    """Run `picodata expel` with specified parameters.
    This function is only needed for a couple of tests which explicitly check
    what happens based on different peer/target combinations.

    In general using `Cluster.expel` is nicer.
    """
    target_info = peer.instance_info(target)
    target_uuid = target_info["uuid"]

    assert password_file, "service password file is needed for expel to work"

    # fmt: off
    command: list[str] = [
        peer.runtime.command, "expel",
        "--peer", f"pico_service@{peer.iproto_listen}",
        "--cluster-name", target.cluster_name or "",
        "--password-file", password_file,
        "--auth-type", "chap-sha1",
        *(["--force"] if force else []),
        "--timeout", str(timeout),
        target_uuid,
    ]
    # fmt: on

    log.info(f"executing: {command}")
    try:
        subprocess.check_output(
            command,
            # NOTE: even though we're redirecting stdin, picodata will still
            # attempt to read the password from the tty (this is a feature, not
            # a bug). For this reason we must provide the serivce password file.
            stdin=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            # Increase the timeout so that we have a chance of receiving the
            # timeout error from picodata
            cwd=peer.cwd,
            timeout=timeout + 10,
        )
    except subprocess.CalledProcessError as e:
        raise CommandFailed(e.stdout, e.stderr) from e


class PgStorage:
    def __init__(self, instance: Instance):
        self.instance: Instance = instance
        self.client_ids: list[int] = []

    def statements(self, id: int):
        return self.instance.call("pico.pg_statements", id)

    def portals(self, id: int):
        return self.instance.call("pico.pg_portals", id)

    def bind(self, id, *params):
        return self.instance.call("pico.pg_bind", id, *params)

    def close_stmt(self, id: int, name: str):
        return self.instance.call("pico.pg_close_stmt", id, name)

    def close_portal(self, id: int, name: str):
        return self.instance.call("pico.pg_close_portal", id, name)

    def describe_stmt(self, id: int, name: str) -> dict:
        return self.instance.call("pico.pg_describe_stmt", id, name)

    def describe_portal(self, id: int, name: str) -> dict:
        return self.instance.call("pico.pg_describe_portal", id, name)

    def execute(self, id: int, name: str, max_rows: int) -> dict:
        return self.instance.call("pico.pg_execute", id, name, max_rows)

    def flush(self):
        for id in self.client_ids:
            for name in self.statements(id)["available"]:
                self.close_stmt(id, name)
            for name in self.portals(id)["available"]:
                self.close_portal(id, name)

    def parse(self, id: int, name: str, sql: str, param_oids: list[int] | None = None) -> int:
        param_oids = param_oids if param_oids is not None else []
        return self.instance.call("pico.pg_parse", id, name, sql, param_oids)

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


def target() -> str:
    # get target triple like x86_64-unknown-linux-gnu
    rustc_version = subprocess.check_output(["rustc", "-vV"]).decode()
    for line in rustc_version.splitlines():
        if line.startswith("host:"):
            return line.split()[1]

    raise Exception(f"cannot deduce target using rustc, version output: {rustc_version}")


@pytest.fixture(scope="session")
def binary_path_fixt(cargo_build_fixt: None) -> Runtime:
    return binary_path()


def binary_path() -> Runtime:
    """Path to the picodata binary, e.g. "./target/debug/picodata"."""
    metadata = subprocess.check_output(
        [
            "cargo",
            "metadata",
            "--format-version=1",
            "--frozen",
            "--no-deps",
        ]
    )
    cargo_target_dir = json.loads(metadata)["target_directory"]
    # This file is huge, hide it from pytest output in case there's an exception
    # somewhere in this function.
    del metadata

    profile = build_profile()

    # XXX: this is a hack for sanitizer-enabled builds (ASan etc).
    # In order to disable that kind of instrumentation for build.rs,
    # we have to pass --target=... or set CARGO_BUILD_TARGET,
    # which will affect the binary path.
    cargo_build_target = os.environ.get("CARGO_BUILD_TARGET")
    if cargo_build_target is None and profile.startswith("asan"):
        cargo_build_target = target()

    if cargo_build_target:
        cargo_target_dir = os.path.join(cargo_target_dir, cargo_build_target)

    # Note: rust names the debug profile `dev`, but puts the binaries into the
    # `debug` directory.
    if profile == "dev":
        profile = "debug"

    binary_path = os.path.realpath(os.path.join(cargo_target_dir, f"{profile}/picodata"))

    test_dir = get_test_dir()
    # Copy the test plugin library into the appropriate location
    # TODO: it's too messy to have all of these files being copied in one place.
    # We should just remove this and call `copy_plugin_library` in each
    # corresponding test directly. That way the tests would be more self
    # contained and this function would be much simpler.
    destinations = [
        f"{test_dir}/testplug/testplug/0.1.0",
        f"{test_dir}/testplug/testplug/0.2.0",
        f"{test_dir}/testplug/testplug/0.3.0",
        f"{test_dir}/testplug/testplug/0.4.0",
        f"{test_dir}/testplug/testplug_small/0.1.0",
        f"{test_dir}/testplug/testplug_small_svc2/0.1.0",
        f"{test_dir}/testplug/testplug_w_migration/0.1.0",
        f"{test_dir}/testplug/testplug_w_migration_2/0.1.0",
        f"{test_dir}/testplug/testplug_w_migration/0.2.0",
        f"{test_dir}/testplug/testplug_w_migration/0.2.0_changed",
        f"{test_dir}/testplug/testplug_sdk/0.1.0",
    ]

    cargo_target_dir = Path(binary_path).parent
    for dst_dir in destinations:
        copy_plugin_library(cargo_target_dir, dst_dir, "libtestplug")

    repository = Repository()

    absolute_version = repository.current_version()
    relative_version = RelativeVersion.CURRENT
    runner_entity = Path(binary_path)
    return Runtime(absolute_version, relative_version, runner_entity)


# TODO: implement a proper plugin installation routine for tests
def copy_plugin_library(from_dir: Path | str, share_dir: Path | str, lib_name: str):
    ext = dynamic_library_extension()
    file_name = f"{lib_name}.{ext}"

    src = Path(from_dir) / file_name
    dst = Path(share_dir) / file_name

    if os.path.exists(dst) and filecmp.cmp(src, dst):
        return

    log.info(f"Copying '{src}' to '{dst}'")
    shutil.copyfile(src, dst)


def dynamic_library_extension() -> str:
    match sys.platform:
        case "linux":
            return "so"
        case "darwin":
            return "dylib"
    raise Exception("unsupported platform")


@pytest.fixture(scope="session")
def cargo_build_fixt(pytestconfig: pytest.Config) -> None:
    cargo_build(bool(pytestconfig.getoption("--with-webui")))


def cargo_build(with_webui: bool = False) -> None:
    """Run cargo build before tests. Skipped in CI"""

    # Start test logs with a newline. This makes them prettier with
    # `pytest -s` (a shortcut for `pytest --capture=no`)
    eprint("")

    if os.environ.get("CI") is not None:
        log.info("Skipping cargo build")
        return

    features = ["error_injection"]
    if with_webui:
        features.append("webui")

    # fmt: off
    cmd = [
        "cargo", "build",
        "--profile", build_profile(),
        "--features", ",".join(features),
    ]
    # fmt: on
    log.info(f"Running {cmd}")
    subprocess.check_call(cmd)

    crates = ["gostech-audit-log", "testplug"]
    for crate in crates:
        cmd = ["cargo", "build", "-p", crate, "--profile", build_profile()]
        log.info(f"Running {cmd}")
        subprocess.check_call(cmd)

    # XXX: plug_wrong_version is even more special than the other ones
    plug_wrong_version_dir = get_test_dir() / "plug_wrong_version"
    cmd = ["cargo", "build", "-p", "plug_wrong_version", "--profile=dev"]
    log.info(f"Running {cmd}")
    subprocess.check_call(cmd, cwd=plug_wrong_version_dir)


@pytest.fixture(scope="session")
def cluster_names(xdist_worker_number) -> Iterator[str]:
    """Unique `cluster_name` generator."""
    return (f"cluster-{xdist_worker_number}-{i}" for i in count())


@pytest.fixture(scope="class")
def class_tmp_dir(tmpdir_factory):
    return tmpdir_factory.mktemp("tmp")


@pytest.fixture(scope="class")
def cluster_factory(binary_path_fixt, class_tmp_dir, cluster_names, port_distributor):
    def cluster_factory_():
        # FIXME: instead of os.getcwd() construct a path relative to os.path.realpath(__file__)
        # see how it's done in def binary_path()
        share_dir = os.getcwd() + "/test/testplug"
        cluster = Cluster(
            runtime=binary_path_fixt,
            id=next(cluster_names),
            data_dir=class_tmp_dir,
            share_dir=share_dir,
            base_host=BASE_HOST,
            port_distributor=port_distributor,
        )
        cluster.set_service_password("password")
        return cluster

    yield cluster_factory_


@pytest.fixture(scope="class")
def cluster(cluster_factory) -> Generator[Cluster, None, None]:
    """Return a `Cluster` object capable of deploying test clusters."""
    cluster = cluster_factory()
    yield cluster
    cluster.kill()
    log.info(f"Cluster data directory was: {cluster.data_dir}")


@pytest.fixture
def second_cluster(binary_path_fixt, tmpdir, cluster_names, port_distributor):
    cluster2_dir = os.path.join(tmpdir, "cluster2")
    os.makedirs(cluster2_dir, exist_ok=True)

    cluster = Cluster(
        runtime=binary_path_fixt,
        id=next(cluster_names),
        data_dir=cluster2_dir,
        base_host=BASE_HOST,
        port_distributor=port_distributor,
    )

    cluster.set_service_password("password")

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
    def __init__(self, instance: Instance, search_str: str, use_regex: bool = False) -> None:
        self.instance = instance
        self.search_str = search_str
        self.use_regex = use_regex

        # If search_str contains multiple lines, we need to be checking multiple
        # lines at a time. Because self._cb will only be called on 1 line at a
        # time we need to do some hoop jumping to make this work.
        self.matched = False

        if self.use_regex:
            self.regex = re.compile(search_str)
            self.expected_lines = []
            self.n_lines = 1
        else:
            search_bytes = search_str.encode("utf-8")
            self.expected_lines = search_bytes.splitlines()
            self.n_lines = len(self.expected_lines)

        # This is the current window in which we're searching for the search_str.
        # Basically this is just last N lines of output we've seen.
        self.current_window: List[bytes] = []

        self.instance = instance
        self.instance.on_output_line(self._cb)

    def _cb(self, line: bytes):
        # exit early if match was already found
        if self.matched:
            return

        if self.use_regex:
            if self.regex.search(line.decode("utf-8")):
                self.matched = True
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

        try:
            Retriable(timeout=timeout, rps=4).call(func=must_match)
        except Exception as e:
            # Check if a panic happened while we were waiting for a message in logs
            self.instance.check_process_alive()
            raise e from e

    def __repr__(self):
        return f"log_crawler({self.instance}, '{self.search_str}')"


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


class AuditServer:
    def __init__(self, port: int) -> None:
        self.port = port
        self.process: Process | None = None
        self.queue: Queue[Dict[str, Any]] = Queue(maxsize=10_000)

    def start(self) -> None:
        if self.process is not None:
            return None

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
    cert_file: str | None = None
    key_file: str | None = None
    ca_file: str | None = None

    def install(self):
        # deploy ~~3~~ 1 instances and configure pgproto on the first one
        i1 = self.cluster.add_instance(wait_online=False)
        # See https://git.picodata.io/core/picodata/-/issues/1218
        # TL;DR: this fixes flakiness
        # self.cluster.add_instance(wait_online=False)
        # self.cluster.add_instance(wait_online=False)
        i1.pg_host = self.host
        i1.pg_port = self.port
        i1.pg_ssl = self.ssl
        i1.pg_ssl_cert_file = self.cert_file
        i1.pg_ssl_key_file = self.key_file
        i1.pg_ssl_ca_file = self.ca_file

        if i1.pg_ssl_cert_file is None or i1.pg_ssl_key_file is None or i1.pg_ssl_ca_file is None:
            ssl_dir = Path(os.path.realpath(__file__)).parent / "ssl_certs"
            instance_dir = Path(i1.instance_dir)
            instance_dir.mkdir(exist_ok=True)
            if i1.pg_ssl_cert_file is None:
                shutil.copyfile(ssl_dir / "server.crt", instance_dir / "server.crt")
            if i1.pg_ssl_key_file is None:
                shutil.copyfile(ssl_dir / "server.key", instance_dir / "server.key")

            if i1.pg_ssl_ca_file is None and self.ssl_verify:
                shutil.copyfile(ssl_dir / "combined-ca.crt", instance_dir / "ca.crt")

        self.cluster.wait_online()
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


@pytest.fixture
def postgres_with_custom_cert_paths(cluster: Cluster, pg_port: int):
    ssl_dir = Path(os.path.realpath(__file__)).parent / "ssl_certs"
    cert_file = str(ssl_dir / "server-with-ext.crt")
    key_file = str(ssl_dir / "my-server.key")
    ca_file = str(ssl_dir / "my-combined-ca.crt")
    return Postgres(
        cluster, port=pg_port, ssl=True, ssl_verify=True, cert_file=cert_file, key_file=key_file, ca_file=ca_file
    ).install()


def copy_dir(src: Path, dst: Path, copy_socks: bool = False):
    if os.path.exists(dst):
        if any(os.scandir(dst)):
            raise ValueError(f"{dst} is not empty, exiting...")
    else:
        os.makedirs(dst)

    for item in os.listdir(src):
        source_item = os.path.join(src, item)
        dest_item = os.path.join(dst, item)

        if stat.S_ISSOCK(os.stat(source_item).st_mode) and not copy_socks:
            continue

        if os.path.isdir(source_item):
            shutil.copytree(source_item, dest_item)
        else:
            shutil.copy2(source_item, dest_item)


@pytest.fixture
def ldap_server(cluster: Cluster, port_distributor: PortDistributor) -> Generator[ldap.LdapServer, None, None]:
    server = ldap.configure_ldap_server(
        username="ldapuser",
        password="ldappass",
        data_dir=cluster.data_dir,
        port=port_distributor.get(),
        tls=False,
    )

    yield server

    server.process.kill()


@pytest.fixture
def ldap_server_with_tls(cluster: Cluster, port_distributor: PortDistributor) -> Generator[ldap.LdapServer, None, None]:
    server = ldap.configure_ldap_server(
        username="ldapuser",
        password="ldappass",
        data_dir=cluster.data_dir,
        port=port_distributor.get(),
        tls=True,
    )

    yield server

    server.process.kill()


GITLAB_URL = "https://git.picodata.io/"
PICODATA_GITLAB_PROJECT_ID = 58


@pytest.hookimpl
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    if not hasattr(item, "execution_count"):  # mypy
        # reruns are not stamped for all tests blindly, only if global setting is passed or item has `flaky` mark
        # https://github.com/pytest-dev/pytest-rerunfailures/blob/d0ff1d4337993cb0f74a46e59f1e6bed0857a0a6/src/pytest_rerunfailures.py#L546
        return

    if item.execution_count == 1:
        # No retries
        return

    assert item.execution_count > 1

    if item.get_closest_marker("flaky") is None:
        # Not marked as flaky
        return

    gitlab_token = os.getenv("PYTEST_REPORT_GITLAB_TOKEN")
    if not (os.getenv("CI") and gitlab_token):
        return

    import gitlab

    gl = gitlab.Gitlab(url=GITLAB_URL, private_token=gitlab_token)
    project = gl.projects.get(id=PICODATA_GITLAB_PROJECT_ID, lazy=True)

    issue_title = f"flaky: {item.nodeid}"
    search_issues = project.issues.list(search=issue_title)

    issue = None
    for search_issue in search_issues:
        if search_issue.title == issue_title:
            issue = search_issue
            break

    if issue is None:
        raise Exception(
            f"Cant find ticket to report flaky test result. Test name: {item.nodeid}, search result: {search_issues}"
        )

    job_url = os.getenv("CI_JOB_URL")
    assert job_url
    if issue.state == "closed":
        issue.state_event = "reopen"
        issue.save()

    issue.discussions.create({"body": f"[AUTOMATIC COMMENT] Failed once again: {job_url}"})


class Repository:
    root: Path
    info: git.Git
    repo: git.Repo
    tags: List[Version]  # descending order

    def __init__(self) -> None:
        self.root = Path(os.getcwd())

        self.info = git.Git(self.root)
        self.repo = git.Repo(self.root)

        self.tags = list()
        for tag in map(str, self.repo.tags):
            try:
                version = Version(tag)
                if version.major >= 25:
                    self.tags.append(version)
            except InvalidVersion:
                pass
        self.tags.sort(reverse=True)

    def current_version(self) -> Version:
        dirty_version = self.info.describe()
        if not dirty_version:
            error = "no git describe info"
            raise ValueError(error)

        version_parts = dirty_version.split("-")
        current_version = Version(version_parts[0])
        return current_version

    def all_versions(self) -> List[Version]:
        return self.tags

    def rolling_versions(self) -> List[Version]:
        """
        TODO: ?.
        """

        seen = set()
        result = list()

        for version in self.all_versions():
            # NOTE: versions list is descending, and for rolling tests we ignore
            # versions that does not follow rolling upgrade guarantees.
            if version.major < 25:
                break

            # NOTE: we only need latest versions by patch of major with minor, so
            # as long as our versions list is descending, we can key by major and
            # minor, appending to the seen versions set to filter properly.
            key = (version.major, version.minor)
            if key not in seen:
                seen.add(key)
                result.append(version)

        return result
