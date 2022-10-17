import funcy  # type: ignore
import time

from conftest import (
    Cluster,
    eprint,
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
