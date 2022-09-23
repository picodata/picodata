from conftest import Cluster
from prettytable import PrettyTable  # type: ignore


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
