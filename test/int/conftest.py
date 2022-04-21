from contextlib import contextmanager, suppress
from dataclasses import dataclass
from functools import cached_property, wraps
import itertools
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import pytest
import tarantool
from filelock import FileLock


@pytest.fixture(scope="session")
def session_data_mutex(tmp_path_factory, worker_id):
    """
    Returns a context manager with a mutex-guarded session-scoped dict to use
    in parallel tests with multiprocessing.
    """

    parallel_test_run = worker_id != "master"

    if parallel_test_run:
        common_dir_for_each_pytest_run = tmp_path_factory.getbasetemp().parent
        data_file: Path = common_dir_for_each_pytest_run / "data.json"
        lock_file: Path = data_file.with_suffix(".lock")

        @contextmanager
        def lock_session_data() -> dict:
            with FileLock(lock_file):
                if data_file.is_file():
                    data = json.loads(data_file.read_text())
                else:
                    data = {}
                yield data
                data_file.write_text(json.dumps(data))

        return lock_session_data
    else:
        data = {}

        @contextmanager
        def d():
            yield data

        return d


@pytest.fixture
def run_id(session_data_mutex):
    with session_data_mutex() as data:
        prev_run_id = data.get("run_id", 0)
        run_id = prev_run_id + 1
        data["run_id"] = run_id
    return run_id


@dataclass
class Instance:
    id: str
    cluster_id: str
    peers: list[str]
    listen: str
    address: tuple[str, int]
    workdir: str
    binary_path: str
    env: dict[str, str] | None = None
    replicaset_id: str | None = None
    process: subprocess.Popen | None = None

    @property
    def command(self):
        # fmt: off
        return [
            self.binary_path, "run",
            "--cluster-id", self.cluster_id,
            "--instance-id", self.id,
            "--listen", self.listen,
            "--peer", ','.join(self.peers),
            # "--data-dir", self.workdir
        ]
        # fmt: on

    @contextmanager
    def connection(self):
        c = tarantool.connect(*self.address)
        try:
            yield c
        finally:
            c.close()

    def _normalize_net_box_result(self, result):
        match result.data:
            case []:
                return
            case [x]:
                return x
            case [x, *rest]:
                return (x, *rest)

    def call(self, fn, *args):
        with self.connection() as conn:
            return self._normalize_net_box_result(conn.call(fn, args))

    def eval(self, expr, *args):
        with self.connection() as conn:
            return self._normalize_net_box_result(conn.eval(expr, *args))

    def kill(self):
        pid = self.process.pid
        with suppress(ProcessLookupError, PermissionError):
            os.killpg(pid, signal.SIGKILL)
            print("killed:,", self)
            with suppress(ChildProcessError):
                os.waitpid(pid, 0)

    def stop(self, kill_after_seconds=10) -> int:
        self.process.terminate()
        try:
            try:
                return self.process.wait(kill_after_seconds)
            except subprocess.TimeoutExpired:
                self.kill()
                return -1
        finally:
            self.kill()

    def start(self):
        self.process = subprocess.Popen(
            self.command,
            cwd=self.workdir,
            env=self.env or {},
            start_new_session=True,
        )
        # Assert a new process group is created
        assert os.getpgid(self.process.pid) == self.process.pid
        wait_tcp_port(*self.address)

    def restart(self, kill=False):
        if kill:
            self.kill()
        else:
            self.stop()
        self.start()

    def remove_data(self):
        for root, dirs, files in os.walk(self.workdir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))

    def __repr__(self):
        return f"picodata({' '.join(self.command[1:])})"


@dataclass(frozen=True)
class Cluster:
    id: str
    instances: list[Instance]

    @cached_property
    def instances_by_id(self) -> dict[str, Instance]:
        return {i.id: i for i in self.instances}

    @cached_property
    def replicasets(self) -> dict[str, list[Instance]]:
        rss = {}
        for i in self.instances:
            rss.setdefault(i.replicaset_id, []).append(i)
        return rss

    def for_each_instance(self, fn):
        return list(map(fn, self.instances))

    def stop(self, *args, **kwargs):
        self.for_each_instance(lambda i: i.stop(*args, **kwargs))

    def start(self, *args, **kwargs):
        self.for_each_instance(lambda i: i.start(*args, **kwargs))

    def kill(self, *args, **kwargs):
        self.for_each_instance(lambda i: i.kill(*args, **kwargs))

    def restart(self, *args, **kwargs):
        self.for_each_instance(lambda i: i.restart(*args, **kwargs))

    def remove_data(self, *args, **kwargs):
        self.for_each_instance(lambda i: i.remove_data(*args, **kwargs))

    def __repr__(self):
        return f"Cluster(id={self.id}, [{', '.join(i.id for i in self.instances)}])"


@pytest.fixture(scope="session")
def compile(session_data_mutex):
    with session_data_mutex() as data:
        if data.get("compiled"):
            return
        assert subprocess.call(["cargo", "build"]) == 0, "cargo build failed"

        data["compiled"] = True


@pytest.fixture(scope="session")
def binary_path(compile) -> str:
    return os.path.realpath(Path(__file__) / "../../../target/debug/picodata")


@pytest.fixture
def run_instance(binary_path, tmpdir, run_id):
    tmpdir = Path(tmpdir)
    ports = (30000 + run_id * 100 + i for i in itertools.count())

    instances: list[Instance] = []

    def really_run_instance(
        *,
        instance_id: str,
        cluster_id: str,
        port: int | None = None,
        peers: list[str] | None = None,
        env: dict[str, str] | None = None,
    ):
        cluster_dir = tmpdir / cluster_id
        env = env or {}
        workdir = cluster_dir / instance_id
        port = port or next(ports)
        address = ("127.0.0.1", port)
        listen = f"127.0.0.1:{port}"
        peers = peers or [listen]

        with suppress(FileExistsError):
            cluster_dir.mkdir()
        workdir.mkdir()

        instance = Instance(
            cluster_id=cluster_id,
            id=instance_id,
            peers=peers,
            address=address,
            listen=listen,
            workdir=str(workdir),
            binary_path=str(binary_path),
            env=env,
        )

        instance.start()
        instances.append(instance)

        return instance

    yield really_run_instance

    for instance in instances:
        instance.stop(kill_after_seconds=2)


@pytest.fixture
def run_cluster(run_instance, run_id):
    cluster_ids = (f"cluster-{run_id:03d}-{i:03d}" for i in itertools.count(1))

    def really_run_cluster(cluster_id=None, instance_count=1):
        cluster_id = cluster_id or next(cluster_ids)
        instance_numbers = tuple(range(1, instance_count + 1))
        ports = [40000 + run_id * 1000 + i for i in instance_numbers]
        peers = [f"127.0.0.1:{p}" for p in ports]

        return Cluster(
            id=cluster_id,
            instances=[
                run_instance(
                    instance_id=f"i{instance_number}",
                    peers=peers,
                    cluster_id=cluster_id,
                    port=port,
                )
                for instance_number, port in zip(instance_numbers, ports)
            ],
        )

    yield really_run_cluster


def wait_tcp_port(host, port, timeout=10):
    t = time.monotonic()
    while time.monotonic() - t < timeout:
        try:
            s = socket.create_connection((host, port), timeout=timeout)
            s.close()
            return
        except socket.error:
            time.sleep(0.02)
    raise TimeoutError("Port is closed %s:%i" % (host, port))


@pytest.fixture
def instance(run_instance, run_id):
    return run_instance(instance_id="i1", cluster_id=f"cluster-{run_id:03d}-1")


@pytest.fixture
def cluster(run_cluster):
    return run_cluster(instance_count=3)
