import os
import subprocess
from dataclasses import field
from multiprocessing.pool import ThreadPool
from typing import Callable
import funcy  # type: ignore
import pytest
from conftest import Cluster, Instance, pgrep_tree, pid_alive
from prettytable import PrettyTable  # type: ignore
from pytest_harvest import saved_fixture


@pytest.mark.parametrize("fibers", [1, 2, 3, 5, 10, 50, 100])
@saved_fixture
def test_benchmark_replace(cluster: Cluster, tmpdir, with_flamegraph, fibers):
    DURATION = 2
    FLAMEGRAPH_TMP_DIR = tmpdir
    FLAMEGRAPH_OUT_DIR = "tmp/flamegraph/replace"
    cluster.deploy(instance_count=1)
    [i1] = cluster.instances

    luacode_init = """
    box.schema.space.create('bench', {
    if_not_exists = true,
    is_local = true,
    format = {
        {name = 'id', type = 'unsigned', is_nullable = false},
        {name = 'value', type = 'unsigned', is_nullable = false}
    }
    })
    box.space.bench:create_index('pk', {
        if_not_exists = true,
        parts = {{'id'}}
    })
    fiber=require('fiber')
    function f(deadline)
        local result = 0
        while deadline > fiber.clock() do
            for i=1, 1000 do
                box.space.bench:replace({1, i})
            end
            result = result + 1000
        end
        return result
    end
    function benchmark_replace(t, c)
        local fibers = {}
        local t1 = fiber.clock()
        local deadline = t1 + t
        for i=1, c do
            fibers[i] = fiber.new(f, deadline)
            fibers[i]:set_joinable(true)
        end
        local total = 0
        for i=1, c do
            local _, ret = fibers[i]:join()
            total = total + ret
        end
        local t2 = fiber.clock()
        return {c = c, total = total, time = t2-t1, rps = total/(t2-t1)}
    end
    """

    i1.assert_raft_status("Leader")

    i1.eval(luacode_init)

    def benchmark():
        i = i1
        pid = child_pid(i)
        acc = []
        c = fibers

        def f(i, t, c):
            return i.eval("return benchmark_replace(...)", t, c, timeout=60)

        if with_flamegraph:
            f = flamegraph_decorator(
                f,
                f"f{str(c).zfill(3)}",
                pid,
                FLAMEGRAPH_TMP_DIR,
                FLAMEGRAPH_OUT_DIR,
                DURATION,
                acc,
            )
        stat = f(i, DURATION, c)

        return ((c, stat), acc[0] if len(acc) > 0 else None)

    return benchmark()


# fmt: off
@pytest.mark.parametrize("cluster_size,fibers", [
    (1, 1), (1, 10), (1, 20),
    (2, 1), (2, 10), (2, 20),
    (3, 1), (3, 10), (3, 20),
    (10, 1), (10, 10), (10, 20),
    (20, 1), (20, 10), (20, 20)
])
# fmt: on
@saved_fixture
def test_benchmark_nop(cluster, tmpdir, cluster_size, fibers, with_flamegraph):
    """
    For adequate flamegraphs don't forget to set kernel options:
    $ sudo echo 0 > /proc/sys/kernel/perf_event_paranoid
    $ sudo echo 0 > /proc/sys/kernel/kptr_restrict
    """
    DURATION = 2
    FLAMEGRAPH_OUT_DIR = "tmp/flamegraph/nop"
    FLAMEGRAPH_TMP_DIR = tmpdir

    def expand_cluster(cluster: Cluster, size: int):
        c = 0
        while len(cluster.instances) < size:
            cluster.add_instance(wait_ready=False).start()
            c += 1
            if c % 5 == 0:
                wait_longer(cluster)
        wait_longer(cluster)

    def init(i: Instance):
        init_code = """
        fiber=require('fiber')
        function f(deadline)
            local result = 0
            while deadline > fiber.clock() do
                for i=1, 5 do
                    picolib.raft_propose_nop()
                end
                result = result + 5
            end
            return result
        end
        function benchmark_nop(t, c)
            local fibers = {}
            local t1 = fiber.clock()
            local deadline = t1 + t
            for i=1, c do
                fibers[i] = fiber.new(f, deadline)
                fibers[i]:set_joinable(true)
            end
            local total = 0
            for i=1, c do
                local _, ret = fibers[i]:join()
                total = total + ret
            end
            local t2 = fiber.clock();
            return {c = c, total = total, time = t2-t1, rps = total/(t2-t1)}
        end
        """
        i.eval(init_code)

    def find_leader(cluster: Cluster):
        for i in cluster.instances:
            if is_leader(i):
                return i
        raise Exception("Leader not found")

    def is_leader(i: Instance):
        return state(i)["raft_state"] == "Leader"

    def state(i: Instance):
        return i.eval("return picolib.raft_status()")

    def benchmark():
        print(f"===== Cluster size = {cluster_size}, fibers = {fibers} =====")
        acc = []
        expand_cluster(cluster, cluster_size)
        i = find_leader(cluster)
        init(i)
        pid = child_pid(i)

        def f(instance, duration, fiber_count):
            return instance.eval("return benchmark_nop(...)", duration, fiber_count, timeout=300)

        if with_flamegraph:
            f = flamegraph_decorator(
                f,
                f"s{str(cluster_size).zfill(3)}f{str(fibers).zfill(3)}",
                pid,
                FLAMEGRAPH_TMP_DIR,
                FLAMEGRAPH_OUT_DIR,
                DURATION,
                acc,
            )
        stat = f(i, DURATION, fibers)

        return ((cluster_size, fibers, stat), acc[0] if len(acc) > 0 else None)

    return benchmark()


@funcy.retry(tries=30, timeout=10)  # type: ignore
def wait_longer(cluster):
    for instance in cluster.instances:
        instance.wait_ready()


def test_summarize_replace_and_nop(fixture_store, with_flamegraph, capsys):
    def report_replace(stats):
        t = PrettyTable()
        t.title = "tarantool replace/sec"
        t.field_names = ["fibers", "rps"]
        for (c, stat) in stats:
            t.add_row([c, int(stat["rps"])])
        t.align = "r"
        return t

    def report_nop(stats):
        sizes = list(set(d[0] for d in stats))
        fibers = list(set(d[1] for d in stats))

        data = [[""] * len(fibers) for _ in sizes]
        for (s, c, stat) in stats:
            size_index = sizes.index(s)
            fiber_index = fibers.index(c)
            val = int(stat["rps"])
            data[size_index][fiber_index] = val  # type: ignore

        t = PrettyTable()
        t.title = "Nop/s"
        t.field_names = ["Cluster size \\ fibers", *fibers]
        t.align = "r"

        index = 0
        for d in data:
            row = [sizes[index], *d]
            t.add_row(row)
            index += 1

        return t

    with capsys.disabled():
        print()
        if "test_benchmark_replace" in fixture_store:
            stats1 = [d[1][0] for d in fixture_store["test_benchmark_replace"].items()]
            print(report_replace(stats1))
        if "test_benchmark_nop" in fixture_store:
            stats2 = [d[1][0] for d in fixture_store["test_benchmark_nop"].items()]
            print(report_nop(stats2))

    if with_flamegraph:
        acc = []
        for d in fixture_store["test_benchmark_replace"].items():
            acc.append(d[1][1])
        for d in fixture_store["test_benchmark_nop"].items():
            acc.append(d[1][1])
        gather_flamegraphs(acc)


class Flamegraph:
    flamegraph_dir: str
    tmp_dir: str
    out_dir: str
    name: str
    watchpid: int
    duration: int
    process: subprocess.Popen | None = None
    env: dict[str, str] = field(default_factory=dict)

    def __init__(
        self,
        tmp_dir: str,
        out_dir: str,
        name: str,
        watchpid: int,
        duration: int,
    ):
        self.flamegraph_dir = os.environ.get("FLAMEGRAPH_DIR", "../FlameGraph")
        self.tmp_dir = tmp_dir
        self.out_dir = out_dir
        self.name = name
        self.watchpid = watchpid
        self.duration = duration

    @property
    def perf_data(self):
        return os.path.join(self.tmp_dir, f"{self.name}-perf.data")

    @property
    def perf_out(self):
        return os.path.join(self.tmp_dir, f"{self.name}-perf.out")

    @property
    def perf_folded(self):
        return os.path.join(self.tmp_dir, f"{self.name}-perf.folded")

    @property
    def perf_svg(self):
        return os.path.join(self.out_dir, f"{self.name}-perf.svg")

    @property
    def stackcollapse_perf_pl(self):
        return os.path.join(self.flamegraph_dir, "stackcollapse-perf.pl")

    @property
    def flamegraph_pl(self):
        return os.path.join(self.flamegraph_dir, "flamegraph.pl")

    @property
    def record_command(self):
        # fmt: off
        return [
            "perf", "record",
            "-p", f"{self.watchpid}",
            "-F", "99",
            "-a",
            "--call-graph", "dwarf",
            "-o", self.perf_data,
            "--",
            "sleep", f"{self.duration}",
        ]
        # fmt: on

    def start(self):
        os.makedirs(self.tmp_dir, exist_ok=True)
        os.makedirs(self.out_dir, exist_ok=True)
        self.process = subprocess.Popen(self.record_command)

    def wait(self):
        self.process.wait()

    def gather_data(self):
        subprocess.run(
            ["perf", "script", "-i", self.perf_data], stdout=open(self.perf_out, "wb")
        )
        subprocess.run(
            [self.stackcollapse_perf_pl, self.perf_out],
            stdout=open(self.perf_folded, "wb"),
        )
        subprocess.run(
            [self.flamegraph_pl, self.perf_folded], stdout=open(self.perf_svg, "wb")
        )


def flamegraph_decorator(
    f: Callable,
    name: str,
    pid: int,
    tmp_dir: str,
    out_dir: str,
    duration: int,
    acc: list[Flamegraph],
):
    def inner(*args, **kwargs):
        flamegraph = Flamegraph(
            tmp_dir,
            out_dir,
            name,
            pid,
            duration,
        )
        flamegraph.start()
        result = f(*args, **kwargs)
        flamegraph.wait()
        acc.insert(0, flamegraph)
        return result

    return inner


def gather_flamegraphs(acc: list[Flamegraph]):
    with ThreadPool() as pool:
        pool.map(lambda flamegraph: flamegraph.gather_data(), acc)


def child_pid(i: Instance):
    assert i.process
    assert i.process.pid
    assert pid_alive(i.process.pid)
    pids = pgrep_tree(i.process.pid)
    assert pid_alive(pids[1])
    return pids[1]
