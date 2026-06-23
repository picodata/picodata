import threading

import psycopg
from conftest import Cluster, log_crawler

BLOCK = "BLOCK_SQL_DISPATCH_AFTER_PUT_META"
BLOCKING_LOG = f"ERROR INJECTION '{BLOCK}': BLOCKING"

QUERY = "SELECT id FROM t WHERE val = 1"
CLIENTS = 10
ROUNDS = 6


def test_frozen_plan_still_referenced(cluster: Cluster):
    # two replicasets for dispatch
    i1, _ = cluster.deploy(instance_count=2)
    cluster.wait_until_buckets_balanced()

    i1.sql("CREATE TABLE t (id INT PRIMARY KEY, val INT)", sudo=True)
    i1.sql("CREATE USER \"u\" WITH PASSWORD 'P@ssw0rd'")
    i1.sql('GRANT READ TABLE TO "u"', sudo=True)
    dsn = f"user=u password=P@ssw0rd host={i1.pg_host} port={i1.pg_port} sslmode=disable"

    conns = [psycopg.connect(dsn, autocommit=True) for _ in range(CLIENTS)]
    errors: list[str] = []
    lock = threading.Lock()

    def worker(conn):
        try:
            conn.execute(QUERY).fetchall()
        except Exception as e:
            with lock:
                errors.append(str(e))

    try:
        for _ in range(ROUNDS):
            blocked = log_crawler(i1, BLOCKING_LOG)
            i1.call("pico._inject_error", BLOCK, True)
            threads = [threading.Thread(target=worker, args=(c,)) for c in conns]
            for t in threads:
                t.start()
            # wait until clients are pinned so they share same plan_id
            blocked.wait_matched()
            i1.call("pico._inject_error", BLOCK, False)
            for t in threads:
                t.join(timeout=30)
            for t in threads:
                assert not t.is_alive(), "worker thread did not finish in time"
            if errors:
                break
    finally:
        i1.call("pico._inject_error", BLOCK, False)
        for c in conns:
            c.close()

    assert not errors, f"errors occurred: {errors}"
