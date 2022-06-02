import io
import os
import re
import sys
import threading
from types import SimpleNamespace
import funcy  # type: ignore
import pytest
import signal
import subprocess

from shutil import rmtree
from typing import Callable, Generator, Iterator
from itertools import count
from pathlib import Path
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


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


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
            case [x]:
                return x
            case [None, str(err)]:
                raise ReturnError(err)
            case [*args]:
                raise MalformedAPI(*args)
            case _:
                raise RuntimeError("unreachable")

    return inner


@dataclass(frozen=True)
class RaftStatus:
    id: int
    raft_state: str
    is_ready: bool
    leader_id: int | None = None


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

OUT_LOCK = threading.Lock()


@dataclass
class Instance:
    binary_path: str
    cluster_id: str
    instance_id: str
    data_dir: str
    peers: list[str]
    host: str
    port: int

    color: Callable[[str], str]

    env: dict[str, str] = field(default_factory=dict)
    process: subprocess.Popen | None = None
    raft_id: int = INVALID_RAFT_ID

    @property
    def listen(self):
        return f"{self.host}:{self.port}"

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
            "--instance-id", self.instance_id,
            "--data-dir", self.data_dir,
            "--listen", self.listen,
            "--peer", ','.join(self.peers)
        ]
        # fmt: on

    def __repr__(self):
        return f"Instance({self.instance_id}, listen={self.listen})"

    @contextmanager
    def connect(self, timeout: int | float):
        c = Connection(
            self.host,
            self.port,
            socket_timeout=timeout,
            connection_timeout=timeout,
        )
        try:
            yield c
        finally:
            c.close()

    @normalize_net_box_result
    def call(self, fn, *args, timeout: int | float = 1):
        with self.connect(timeout) as conn:
            return conn.call(fn, args)

    @normalize_net_box_result
    def eval(self, expr, *args, timeout: int | float = 1):
        with self.connect(timeout) as conn:
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

    def terminate(self, kill_after_seconds=10):
        """Terminate the instance gracefully with SIGTERM"""
        if self.process is None:
            # Be idempotent
            return

        with suppress(ProcessLookupError, PermissionError):
            os.killpg(self.process.pid, signal.SIGCONT)

        self.process.terminate()

        try:
            self.process.wait(timeout=kill_after_seconds)
            eprint(f"{self} terminated")
        finally:
            self.kill()

    def _process_output(self, src, out):
        prefix = f"{self.instance_id:<3} | "

        if sys.stdout.isatty():
            prefix = self.color(prefix)

        for line in io.TextIOWrapper(src, line_buffering=True):
            with OUT_LOCK:
                out.write(prefix)
                out.write(line)
                out.flush()

    def start(self):
        if self.process:
            # Be idempotent
            return

        eprint(f"{self} starting...")

        self.process = subprocess.Popen(
            self.command,
            env=self.env or None,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
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
        status = self.call("picolib.raft_status")
        assert isinstance(status, dict)
        return RaftStatus(**status)

    def raft_propose_eval(self, lua_code: str, timeout_seconds=2):
        return self.call(
            "picolib.raft_propose_eval",
            timeout_seconds,
            lua_code,
        )

    def assert_raft_status(self, state, leader_id=None):
        status = self._raft_status()

        if leader_id is None:
            leader_id = status.leader_id

        want = {"raft_state": state, "leader_id": leader_id}
        have = {
            "raft_state": status.raft_state,
            "leader_id": status.leader_id,
        }

        assert want == have

    @funcy.retry(tries=30, timeout=0.2)
    def wait_ready(self):
        status = self._raft_status()
        assert status.is_ready
        self.raft_id = status.id
        eprint(f"{self} is ready")

    @funcy.retry(tries=4, timeout=0.1, errors=AssertionError)
    def promote_or_fail(self):
        eprint(f"{self} is trying to become a leader")

        # 1. Force the node to campaign.
        self.call("picolib.raft_timeout_now")

        # 2. Wait until the miracle occurs.
        @funcy.retry(tries=4, timeout=0.1, errors=AssertionError)
        def wait_promoted():
            self.assert_raft_status("Leader")

        wait_promoted()
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

    def __repr__(self):
        return f'Cluster("{self.base_host}:{self.base_port}", n={len(self.instances)})'

    def __getitem__(self, item: int) -> Instance:
        return self.instances[item]

    def deploy(self, *, instance_count: int) -> list[Instance]:
        assert not self.instances, "Already deployed"

        for i in range(instance_count):
            self.add_instance(wait_ready=False)

        for instance in self.instances:
            instance.start()

        for instance in self.instances:
            instance.wait_ready()

        eprint(f" {self} deployed ".center(80, "="))
        return self.instances

    def add_instance(self, wait_ready=True, peers=None) -> Instance:
        i = 1 + len(self.instances)

        instance = Instance(
            binary_path=self.binary_path,
            cluster_id=self.id,
            instance_id=f"i{i}",
            data_dir=f"{self.data_dir}/i{i}",
            host=self.base_host,
            port=self.base_port + i,
            peers=peers or [f"{self.base_host}:{self.base_port + 1}"],
            color=CLUSTER_COLORS[len(self.instances) % len(CLUSTER_COLORS)],
        )

        assert self.base_port <= instance.port <= self.max_port
        self.instances.append(instance)

        if wait_ready:
            instance.start()
            instance.wait_ready()

        return instance

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


@pytest.fixture(scope="session")
def compile() -> None:
    assert subprocess.call(["cargo", "build"]) == 0, "cargo build failed"


@pytest.fixture(scope="session")
def binary_path(compile) -> str:
    return os.path.realpath(Path(__file__) / "../../../target/debug/picodata")


@pytest.fixture(scope="session")
def cluster_ids(xdist_worker_number) -> Iterator[str]:
    return (f"cluster-{xdist_worker_number}-{i}" for i in count())


@pytest.fixture
def cluster(
    binary_path,
    tmpdir,
    xdist_worker_number,
    cluster_ids,
) -> Generator[Cluster, None, None]:
    n = xdist_worker_number
    assert isinstance(n, int)
    assert n >= 0

    # Provide each worker a dedicated pool of 100 listening ports
    base_port = 3300 + n * 100
    max_port = base_port + 99
    assert max_port <= 65535

    cluster = Cluster(
        binary_path=binary_path,
        id=next(cluster_ids),
        data_dir=tmpdir,
        base_host="127.0.0.1",
        base_port=base_port,
        max_port=max_port,
    )
    yield cluster
    cluster.kill()


@pytest.fixture
def instance(cluster: Cluster) -> Generator[Instance, None, None]:
    cluster.deploy(instance_count=1)
    yield cluster[0]
