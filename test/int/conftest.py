import os
import re
import sys
import funcy  # type: ignore
import pytest
import signal
import subprocess

from shutil import rmtree
from typing import Generator
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
INVALID_ID = 0
RE_XDIST_WORKER_ID = re.compile(r"^gw(\d+)$")


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def xdist_worker_number(worker_id: str) -> int:
    """
    Identify xdist worker by an integer instead of a string.
    This is used for parallel testing.
    See also: https://pypi.org/project/pytest-xdist/
    """

    if worker_id == "master":
        return 0

    match = RE_XDIST_WORKER_ID.match(worker_id)
    if not match:
        raise ValueError(worker_id)

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
    leader_id: int
    is_ready: bool


@dataclass
class Instance:
    binary_path: str

    instance_id: str
    data_dir: str
    peers: list[str]
    host: str
    port: int

    env: dict[str, str] = field(default_factory=dict)
    process: subprocess.Popen | None = None
    raft_id: int = INVALID_ID

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
            "--instance-id", self.instance_id,
            "--data-dir", self.data_dir,
            "--listen", self.listen,
            "--peer", ','.join(self.peers)
        ]
        # fmt: on

    def __repr__(self):
        return f"Instance({self.instance_id}, listen={self.listen})"

    @contextmanager
    def connect(self, timeout: int):
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
    def call(self, fn, *args, timeout: int = 1):
        with self.connect(timeout) as conn:
            return conn.call(fn, args)

    @normalize_net_box_result
    def eval(self, expr, *args, timeout: int = 1):
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

    def start(self):
        if self.process:
            # Be idempotent
            return

        self.process = subprocess.Popen(
            self.command,
            env=self.env or None,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )

        eprint(f"{self} starting...")

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

    def __hash__(self):
        return hash(self.id) ^ hash(self.cluster_id) ^ hash(self.listen)

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

    @funcy.retry(tries=20, timeout=0.1)
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


@dataclass
class Cluster:
    binary_path: str

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
            instance_id=f"i{i}",
            data_dir=f"{self.data_dir}/i{i}",
            host=self.base_host,
            port=self.base_port + i,
            peers=peers or [f"{self.base_host}:{self.base_port + 1}"],
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


@pytest.fixture
def cluster(binary_path, tmpdir, worker_id) -> Generator[Cluster, None, None]:
    n = xdist_worker_number(worker_id)
    assert isinstance(n, int)
    assert n >= 0

    # Provide each worker a dedicated pool of 100 listening ports
    base_port = 3300 + n * 100
    max_port = base_port + 99
    assert max_port <= 65535

    cluster = Cluster(
        binary_path=binary_path,
        data_dir=tmpdir,
        base_host="127.0.0.1",
        base_port=base_port,
        max_port=max_port,
    )
    yield cluster
    cluster.terminate()
    cluster.remove_data()


@pytest.fixture
def instance(cluster: Cluster) -> Generator[Instance, None, None]:
    cluster.deploy(instance_count=1)
    yield cluster[0]
