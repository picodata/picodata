import io
import json
import os
import re
import sys
import threading
from types import SimpleNamespace
import funcy  # type: ignore
import pytest
import signal
import subprocess
from rand.params import generate_seed
from functools import reduce
from datetime import datetime
from shutil import rmtree
from typing import Callable, Generator, Iterator
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
    term: int
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

POSITION_IN_SPACE_INSTANCE_ID = 3


@dataclass
class Instance:
    binary_path: str
    cluster_id: str
    data_dir: str
    peers: list[str]
    host: str
    port: int
    init_replication_factor: int

    color: Callable[[str], str]

    instance_id: str | None = None
    failure_domain: dict[str, str] = field(default_factory=dict)
    env: dict[str, str] = field(default_factory=dict)
    process: subprocess.Popen | None = None
    raft_id: int = INVALID_RAFT_ID
    _on_output_callbacks: list[Callable[[str], None]] = field(default_factory=list)

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
            *([f"--instance-id={self.instance_id}"] if self.instance_id else []),
            "--data-dir", self.data_dir,
            "--listen", self.listen,
            "--peer", ','.join(self.peers),
            *(f"--failure-domain={k}={v}" for k, v in self.failure_domain.items()),
            "--init-replication-factor", f"{self.init_replication_factor}"
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
        status = self.call("picolib.raft_status")
        assert isinstance(status, dict)
        return RaftStatus(**status)

    def raft_propose_eval(self, lua_code: str, timeout_seconds=2):
        return self.call(
            "picolib.raft_propose_eval",
            lua_code,
            dict(timeout=timeout_seconds),
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
    def wait_online(self):
        """Wait until instance attains Online grade

        Raises:
            AssertionError: if doesn't succeed
        """

        whoami = self.call("picolib.whoami")
        assert isinstance(whoami, dict)
        assert isinstance(whoami["raft_id"], int)
        assert isinstance(whoami["instance_id"], str)
        self.raft_id = whoami["raft_id"]
        self.instance_id = whoami["instance_id"]

        myself = self.call("picolib.peer_info", self.instance_id)
        assert isinstance(myself, dict)
        assert myself["current_grade"] == "Online"

        eprint(f"{self} is online")

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

    def deploy(
        self, *, instance_count: int, init_replication_factor: int = 1
    ) -> list[Instance]:
        assert not self.instances, "Already deployed"

        for _ in range(instance_count):
            self.add_instance(
                wait_online=False, init_replication_factor=init_replication_factor
            )

        for instance in self.instances:
            instance.start()

        for instance in self.instances:
            instance.wait_online()

        eprint(f" {self} deployed ".center(80, "="))
        return self.instances

    def add_instance(
        self,
        wait_online=True,
        peers=None,
        instance_id: str | bool = True,
        failure_domain=dict(),
        init_replication_factor=1,
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

        instance = Instance(
            binary_path=self.binary_path,
            cluster_id=self.id,
            instance_id=generated_instance_id,
            data_dir=f"{self.data_dir}/i{i}",
            host=self.base_host,
            port=self.base_port + i,
            peers=peers or [f"{self.base_host}:{self.base_port + 1}"],
            init_replication_factor=init_replication_factor,
            color=CLUSTER_COLORS[len(self.instances) % len(CLUSTER_COLORS)],
            failure_domain=failure_domain,
        )

        assert self.base_port <= instance.port <= self.max_port
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
        init_replication_factor=1,
    ):
        instance = self.add_instance(
            wait_online=False,
            peers=peers,
            instance_id=instance_id,
            failure_domain=failure_domain,
            init_replication_factor=init_replication_factor,
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

    def expel(self, target: Instance, peer: Instance = None):
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


@pytest.fixture(scope="session")
def compile() -> None:
    """Run `cargo build` before tests."""

    assert subprocess.call(["cargo", "build"]) == 0, "cargo build failed"


@pytest.fixture(scope="session")
def binary_path(compile) -> str:
    """Path to the picodata binary, e.g. "./target/debug/picodata"."""
    metadata = subprocess.check_output(["cargo", "metadata", "--format-version=1"])
    target = json.loads(metadata)["target_directory"]
    return os.path.realpath(os.path.join(target, "debug/picodata"))


@pytest.fixture(scope="session")
def cluster_ids(xdist_worker_number) -> Iterator[str]:
    """Unique `clister_id` generator."""
    return (f"cluster-{xdist_worker_number}-{i}" for i in count())


@pytest.fixture
def cluster(
    binary_path,
    tmpdir,
    xdist_worker_number,
    cluster_ids,
) -> Generator[Cluster, None, None]:
    """Return a `Cluster` object capable of deploying test clusters."""
    n = xdist_worker_number
    assert isinstance(n, int)
    assert n >= 0

    # Provide each worker a dedicated pool of 200 listening ports
    base_port = 3300 + n * 200
    max_port = base_port + 199
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
    """Returns a deployed instance forming a single-node cluster."""
    cluster.deploy(instance_count=1)
    yield cluster[0]


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
