from dataclasses import field
from itertools import count
from multiprocessing.pool import ThreadPool
from typing import Callable
from conftest import Cluster, Instance, pgrep_tree, pid_alive, retrying
from prettytable import PrettyTable  # type: ignore
import os
import funcy  # type: ignore
import subprocess


def test_benchmark_replace(cluster: Cluster, tmpdir, with_flamegraph, capsys):
    FIBERS = [1, 2, 3, 5, 10, 60, 70, 100, 200, 300, 400, 500, 600, 700, 800]
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
        stats = []
        pid = child_pid(i)
        acc = []
        for c in FIBERS:

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
            stats.append((c, stat))

        if with_flamegraph:
            gather_flamegraphs(acc)

        return stats

    def report(stats):
        t = PrettyTable()
        t.title = "tarantool replace/sec"
        t.field_names = ["fibers", "rps"]
        for (c, stat) in stats:
            t.add_row([c, int(stat["rps"])])
        t.align = "r"
        return t

    with capsys.disabled():
        print("")
        print(report(benchmark()))


def test_benchmark_nop(
    binary_path, xdist_worker_number, tmpdir, with_flamegraph, capsys
):
    """
    For adequate flamegraphs don't forget to set kernel options:
    $ sudo echo 0 > /proc/sys/kernel/perf_event_paranoid
    $ sudo echo 0 > /proc/sys/kernel/kptr_restrict
    """
    SIZES = [1, 2, 3, 10, 25, 50]
    FIBERS = [1, 10, 30, 40, 50, 75]
    DURATION = 2
    FLAMEGRAPH_OUT_DIR = "tmp/flamegraph/nop"
    FLAMEGRAPH_TMP_DIR = os.path.join(tmpdir, "flamegraph")
    CLUSTER_DATA_DIR = os.path.join(tmpdir, "cluster")

    def new_cluster():
        n = xdist_worker_number
        assert isinstance(n, int)
        assert n >= 0

        # Provide each worker a dedicated pool of 200 listening ports
        base_port = 3300 + n * 200
        max_port = base_port + 199
        assert max_port <= 65535

        cluster_ids = (f"cluster-{xdist_worker_number}-{i}" for i in count())

        cluster = Cluster(
            binary_path=binary_path,
            id=next(cluster_ids),
            data_dir=CLUSTER_DATA_DIR,
            base_host="127.0.0.1",
            base_port=base_port,
            max_port=max_port,
        )
        return cluster

    def expand_cluster(cluster: Cluster, size: int):
        c = 0
        while len(cluster.instances) < size:
            cluster.add_instance(wait_ready=False).start()
            c += 1
            if c % 3 == 0:
                wait_longer(cluster)
        wait_longer(cluster)

    def make_cluster(size: int):
        cluster = new_cluster()
        expand_cluster(cluster, size)
        return cluster

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

    def instance_to_pid(i: Instance):
        assert i.process
        return i.process.pid

    def benchmark():
        stats = []
        sizes = list(set(SIZES))
        sizes.sort()
        acc = []
        for s in sizes:
            print(f"===== Cluster size = {s} =====")
            cluster = make_cluster(s)
            pids = list(map(instance_to_pid, cluster.instances))
            i = find_leader(cluster)
            init(i)
            pid = child_pid(i)

            for c in FIBERS:
                print(f"===== Cluster size = {s}, fibers = {c} =====")

                def f(i, t, c):
                    return [i.eval("return benchmark_nop(...)", t, c, timeout=300)]

                if with_flamegraph:
                    f = flamegraph_decorator(
                        f,
                        f"s{str(s).zfill(3)}f{str(c).zfill(3)}",
                        pid,
                        FLAMEGRAPH_TMP_DIR,
                        FLAMEGRAPH_OUT_DIR,
                        DURATION,
                        acc,
                    )
                stat = f(i, DURATION, c)
                stats.append((s, c, stat))

            cluster.kill()
            cluster.remove_data()
            retrying(lambda: assert_all_pids_down(pids))

        if with_flamegraph:
            gather_flamegraphs(acc)

        return stats

    def report_vertical(stats):
        t = PrettyTable()
        t.field_names = ["cluster size", "fibers", "rps"]
        t.align = "r"
        for (s, c, stat) in stats:
            t.add_row([s, c, int(avg(rps(stat)))])
        return t

    def report_table(stats):
        def index_exist(lis: list, i: int):
            try:
                lis[i]
            except IndexError:
                return False
            return True

        data = []
        for (s, c, stat) in stats:
            size_index = SIZES.index(s)
            fiber_index = FIBERS.index(c)
            val = int(avg(rps(stat)))
            if not index_exist(data, size_index):
                data.insert(size_index, [])
            if not index_exist(data[size_index], fiber_index):
                data[size_index].insert(fiber_index, val)

        t = PrettyTable()
        t.title = "Nop/s"
        t.field_names = ["Cluster size \\ fibers", *FIBERS]
        t.align = "r"

        index = 0
        for d in data:
            row = [SIZES[index], *d]
            t.add_row(row)
            index += 1

        return t

    def rps(stat):
        return list(map(lambda s: s["rps"], stat))

    def avg(x):
        return sum(x) / len(x)

    def assert_all_pids_down(pids):
        assert all(map(lambda pid: not pid_alive(pid), pids))

    with capsys.disabled():
        stats = benchmark()
        print(report_table(stats))


@funcy.retry(tries=30, timeout=10)
def wait_longer(cluster):
    for instance in cluster.instances:
        instance.wait_ready()


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
        acc.append(flamegraph)
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
