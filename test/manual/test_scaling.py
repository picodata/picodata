# mypy: disable-error-code="import"
import funcy  # type: ignore
import time
from matplotlib import pyplot
import matplotlib

from conftest import (
    Cluster,
    eprint,
    BASE_HOST,
)


@funcy.retry(tries=30, timeout=10)
def wait_longer(cluster):
    for instance in cluster.instances:
        instance.wait_online()


def test_instant(cluster: Cluster):
    t1 = time.time()
    cluster.deploy(instance_count=5)
    for i in range(60):
        cluster.add_instance(wait_online=False).start()
    wait_longer(cluster)
    t2 = time.time()

    eprint(f"It took {t2-t1} seconds")
    eprint("=" * 80)

    while True:
        time.sleep(1)


def test_chunked(cluster: Cluster):
    """
    Deploy the cluster of 60 instance and suspend until the test is
    interrupted by Ctrl+C.
    """
    t1 = time.time()

    cluster.deploy(instance_count=5)

    chunk_size = 5
    total_size = 60
    total_size -= len(cluster.instances)
    for j in range(int(total_size / chunk_size)):
        for i in range(chunk_size):
            cluster.add_instance(wait_online=False).start()

        wait_longer(cluster)

        t2 = time.time()
        eprint("=" * 80)
        eprint(f"Deployed {len(cluster.instances)} instances so far")
        eprint(f"It took {t2-t1} seconds")
        eprint("=" * 80)
        time.sleep(1)

    while True:
        time.sleep(1)


# `PORT_RANGE` needs to be increased to run this test
def test_cas_conflicts(binary_path, tmpdir_factory, cluster_ids, port_range):
    """
    Deploys clusters with increasing number of instances,
    and counts the number of CaS conflicts on join.

    The diagram of cas conflicts vs number of instances
    is then saved as a PNG file.
    """
    base_port, max_port = port_range

    matplotlib.use("Agg")

    cas_conflicts_per_size = []
    ready_in = []
    sizes = range(5, 30, 2)
    for size in sizes:
        cas_conflicts = 0
        cluster = Cluster(
            binary_path=binary_path,
            id=next(cluster_ids),
            data_dir=tmpdir_factory.mktemp(f"cluster{size}"),
            base_host=BASE_HOST,
            base_port=base_port,
            max_port=max_port,
        )
        start = time.time()
        cluster.deploy(instance_count=1)

        def count_conflicts(line: bytes):
            nonlocal cas_conflicts
            if line.find(b"compare-and-swap: ConflictFound") != -1:
                cas_conflicts += 1

        cluster[0].on_output_line(count_conflicts)

        for _ in range(size - 1):
            cluster.add_instance(wait_online=False).start()

        for instance in cluster.instances:
            instance.wait_online(60)
        ready_in.append(time.time() - start)
        cas_conflicts_per_size.append(cas_conflicts)
        cluster.terminate()
        base_port += size + 1

    fig, axs = pyplot.subplots(2, 1)

    axs[0].plot(sizes, cas_conflicts_per_size, ".-")
    axs[0].set_xlabel("instances")
    axs[0].set_ylabel("cas conflicts")

    axs[1].plot(sizes, ready_in, ".-")
    axs[1].set_xlabel("instances")
    axs[1].set_ylabel("ready in (seconds)")
    pyplot.savefig("cas_conflicts_on_join.png")
