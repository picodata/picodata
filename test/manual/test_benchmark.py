from itertools import count
from conftest import Cluster, Instance, pid_alive, retrying
from prettytable import PrettyTable  # type: ignore
import funcy  # type: ignore


def test_benchmark_replace(cluster: Cluster, capsys):
    ITERATIONS = 99999
    FIBERS = [1, 2, 3, 5, 10, 50, 60, 70, 100, 200, 300, 400, 500, 600, 700, 800]
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
    clock=require('clock')
    fiber=require('fiber')
    function f(n)
        for i=1, n do
            box.space.bench:replace({1, i})
        end
    end
    function benchmark_replace(n, c)
        local fibers = {}
        local t1 = clock.monotonic()
        for i=1, c do
            fibers[i] = fiber.new(f, n)
            fibers[i]:set_joinable(true)
        end
        for i=1, c do
            fibers[i]:join()
        end
        local t2 = clock.monotonic();
        return {c=c, nc = n*c, time = t2-t1, rps = n*c/(t2-t1)}
    end
    """

    i1.assert_raft_status("Leader")

    i1.eval(luacode_init)

    def benchmark_one(n: int, c: int):
        return i1.eval("return benchmark_replace(...)", n, c, timeout=60)

    def benchmark():
        stats = []
        for c in FIBERS:
            result = benchmark_one(int(ITERATIONS / c), c)
            stats.append((c, result))
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


def test_benchmark_nop(binary_path, xdist_worker_number, tmpdir, capsys):
    SIZES = [1, 2, 3, 5, 10, 20, 30, 40, 50]
    FIBERS = [1, 5, 10, 15, 20, 25]

    data_dir = tmpdir

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
            data_dir=data_dir,
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
        return cluster

    def make_cluster(size: int):
        cluster = new_cluster()
        expand_cluster(cluster, size)
        return cluster

    def init(i: Instance):
        init_code = """
        clock=require('clock')
        fiber=require('fiber')
        function f(n)
            for i=1, n do
                picolib.raft_propose_nop()
            end
        end
        function benchmark_nop(n, c)
            local fibers = {};
            local t1 = clock.monotonic();
            for i=1, c do
                fibers[i] = fiber.new(f, n)
                fibers[i]:set_joinable(true)
            end
            for i=1, c do
                fibers[i]:join()
            end
            local t2 = clock.monotonic();
            return {c=c, nc = n*c, time = t2-t1, rps = n*c/(t2-t1)}
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
        for s in sizes:
            cluster = make_cluster(s)
            pids = list(map(instance_to_pid, cluster.instances))
            i = find_leader(cluster)
            init(i)

            for c in FIBERS:
                stat = [i.eval("return benchmark_nop(...)", 10, c, timeout=60)]
                stats.append((s, c, stat))

            cluster.kill()
            cluster.remove_data()
            retrying(lambda: assert_all_pids_down(pids))
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
        t.field_names = ["cluster size \\ fibers", *FIBERS]
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
