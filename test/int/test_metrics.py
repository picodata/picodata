from typing import Dict

from conftest import (
    Cluster,
    Instance,
    TarantoolError,
    ErrorCode,
)
import pytest
import requests  # type: ignore
from prometheus_client import Metric
from prometheus_client.samples import Sample
import psycopg


@pytest.mark.webui
def test_metrics_ok(instance: Instance) -> None:
    http_listen = instance.env["PICODATA_HTTP_LISTEN"]
    response = requests.get(f"http://{http_listen}/metrics")
    assert response.ok


# Verifies that all picodata metrics are present
# and accessible on /metrics endpoint
@pytest.mark.webui
def test_picodata_metrics(cluster: Cluster) -> None:
    instance = cluster.add_instance(name="i1", init_replication_factor=2, enable_http=True)
    i2 = cluster.add_instance(name="i2", enable_http=True)

    instance.wait_online()
    instance.sql("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT) DISTRIBUTED GLOBALLY")
    instance.sql("INSERT INTO test VALUES (1, 'one')")

    with pytest.raises(TarantoolError) as e:
        i2.sql("INSERT INTO test VALUES (1, 'one')")
        assert e.value.args[:2] == (ErrorCode.SbroadError, "sbroad: Lua error (IR dispatch)")

    i2.sql("INSERT INTO test VALUES (2, 'two')")
    with pytest.raises(TarantoolError) as e:
        instance.sql("INSERT INTO test VALUES (2, 'two')")
        assert e.value.args[:2] == (ErrorCode.SbroadError, "sbroad: Lua error (IR dispatch)")

    http_listen = instance.env["PICODATA_HTTP_LISTEN"]
    url = f"http://{http_listen}/metrics"
    response = requests.get(url)
    assert response.ok, f"Metrics endpoint {url} did not return OK: {response.status_code}"
    metrics_output = response.text
    expected_metrics = [
        "pico_governor_changes_total",
        "pico_sql_query_total",
        "pico_sql_query_errors_total",
        "pico_sql_query_duration",
        "pico_rpc_request_total",
        "pico_rpc_request_errors_total",
        "pico_rpc_request_duration",
        "pico_cas_records_total",
        "pico_cas_errors_total",
        "pico_cas_ops_duration",
        "pico_instance_state",
        "pico_raft_applied_index",
        "pico_raft_commit_index",
        "pico_raft_term",
        "pico_raft_state",
        "pico_raft_leader_id",
    ]

    for metric in expected_metrics:
        assert metric in metrics_output, f"Metric '{metric}' not found in /metrics output"


def check_metric(families: Dict[str, Metric], name: str, value: float | None, **labels):
    found_family = families.get(name)
    if value is None:
        if not labels:
            assert found_family is None, "Metric {} found".format(name)
        elif found_family is not None:
            for sample in found_family.samples:
                match_all_labels = all([sample.labels.get(lbl) == val for lbl, val in labels.items()])
                assert not match_all_labels, "Labeled {} metric found".format(name)
    else:
        assert found_family is not None, "Metric {} not found".format(name)
        if not labels:
            assert len(found_family.samples) == 1, "Metric has {} samples instead of 1".format(
                len(found_family.samples)
            )
            sample = found_family.samples[0]
            assert sample.value == value, "Metric has unexpected value"
        else:
            filtered_samples = []
            for sample in found_family.samples:
                if all([sample.labels.get(label) == value for label, value in labels.items()]):
                    filtered_samples.append(sample)
            assert len(filtered_samples) == 1, "Labeled metric has {} samples instead of 1".format(
                len(filtered_samples)
            )
            sample = filtered_samples[0]
            assert sample.value == value, "Labeled metric has unexpected value"


@pytest.mark.webui
def test_pgproto_metrics_collected(instance: Instance) -> None:
    check_metric(instance.get_metrics(), "pico_sql_query", None)

    # make a non-privileged user with iproto
    instance.sql("CREATE USER postgres WITH PASSWORD 'Passw0rd'")

    # creating a user for postgress will increase the metric value, so check it
    check_metric(instance.get_metrics(), "pico_sql_query", 1.0)

    # execute a query through postgresql
    host, port = instance.pg_host, instance.pg_port
    conn = psycopg.connect(f"postgres://postgres:Passw0rd@{host}:{port}")
    conn.autocommit = True  # do not make a transaction

    conn.execute("SELECT 1").fetchall()

    # the select above should've increase the metric value too
    check_metric(instance.get_metrics(), "pico_sql_query", 2.0)


@pytest.mark.webui
def test_router_and_storage_cache_metrics(instance: Instance):
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_router_cache_misses", 0)

    ################
    # Run ACL query
    ################

    # Create a postgres user to test caches via pgproto
    instance.sql("CREATE USER postgres WITH PASSWORD 'Passw0rd'")
    host, port = instance.pg_host, instance.pg_port
    pgproto = psycopg.connect(f"postgres://postgres:Passw0rd@{host}:{port}")
    pgproto.autocommit = True  # do not make a transaction

    metrics = instance.get_metrics()

    # ACL is not cached
    check_metric(metrics, "pico_router_cache_statements_added", 0)
    check_metric(metrics, "pico_storage_cache_statements_added", 0)

    # Every query (even ACL) is looked in the router cache so miss is reported
    check_metric(metrics, "pico_router_cache_misses", 1)
    # ACL is not executed on storage so no requests are reported
    check_metric(metrics, "pico_storage_1st_requests", None)
    check_metric(metrics, "pico_storage_2nd_requests", None)

    ################
    # Run DDL query
    ################

    instance.sql("CREATE TABLE t (a INT PRIMARY KEY, b INT)")

    metrics = instance.get_metrics()

    # Every query (even DDL) is looked in the router cache so miss is reported
    check_metric(metrics, "pico_router_cache_misses", 2)
    # ACL is not executed on storage so no requests are reported
    check_metric(metrics, "pico_storage_1st_requests", None)
    check_metric(metrics, "pico_storage_2nd_requests", None)

    ################
    # Run DML query
    ################

    instance.sql("INSERT INTO t VALUES (1,2)")

    metrics = instance.get_metrics()

    # Every query is looked in the router cache so miss is reported
    check_metric(metrics, "pico_router_cache_misses", 3)
    # DML is cached on the router
    check_metric(metrics, "pico_router_cache_statements_added", 1)
    # DML without DQL doesn't have cache misses
    check_metric(metrics, "pico_storage_1st_requests", 1, query_type="dml")
    check_metric(metrics, "pico_storage_2nd_requests", None)

    ##########################
    # Execute first DQL query
    ##########################

    instance.sql("SELECT * FROM t")

    # DQL statement is cached
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_router_cache_statements_added", 2)
    check_metric(metrics, "pico_storage_cache_statements_added", 1)

    # Router cache miss is reported
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_router_cache_misses", 4)

    # Both requests to the storage cache are reported (i.e. cache miss)
    check_metric(metrics, "pico_storage_1st_requests", 1, query_type="dql", result="ok")
    check_metric(metrics, "pico_storage_2nd_requests", 1, query_type="dql", result="ok")

    #####################################
    # Run the same DQL query via pgproto
    #####################################

    instance.sql("SELECT * FROM t")

    # Statement wasn't cached again
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_router_cache_statements_added", 2)
    check_metric(metrics, "pico_storage_cache_statements_added", 1)

    # Cache hit is reported on the router
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_router_cache_hits", 1)

    # Only 1st requests to the storage cache is reported (i.e. cache hit)
    check_metric(metrics, "pico_storage_1st_requests", 2, query_type="dql", result="ok")
    check_metric(metrics, "pico_storage_2nd_requests", 1, query_type="dql", result="ok")

    ##############################################
    # Ensure storage cache evictions are reported
    ##############################################

    # Make storage cache full
    instance.sql("ALTER SYSTEM SET sql_storage_cache_count_max = 1")
    # Nothing evicted yet
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_statements_evicted", 0)
    check_metric(metrics, "pico_storage_cache_statements_added", 1)
    # Overflow the cache
    instance.sql("SELECT a + b FROM t")
    # Ensure eviction is reported
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_statements_evicted", 1)
    check_metric(metrics, "pico_storage_cache_statements_added", 2)

    #############################################
    # Ensure router cache evictions are reported
    #############################################

    # There is already 3 entries in the cache
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_router_cache_statements_added", 3)
    # Make router cache full
    for i in range(0, 47):
        instance.sql(f"SELECT a + {i} FROM t")
    # Nothing evicted yet
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_router_cache_statements_added", 50)
    check_metric(metrics, "pico_router_cache_statements_evicted", 0)
    # Overflow the cache
    instance.sql("SELECT a + b + 1 FROM t")
    # Ensure eviction is reported
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_router_cache_statements_added", 51)
    check_metric(metrics, "pico_router_cache_statements_evicted", 1)

    ##########################################################
    # Ensure cache size is correctly reported after shrinking
    ##########################################################

    # Pollute storage cache
    instance.sql("ALTER SYSTEM SET sql_storage_cache_count_max = 4")
    instance.sql("SELECT (1) FROM t")
    instance.sql("SELECT (1), (2) FROM t")
    instance.sql("SELECT (1), (2), (3) FROM t")
    instance.sql("SELECT (1), (2), (3), (4) FROM t")

    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_statements_added", 8)
    check_metric(metrics, "pico_storage_cache_statements_evicted", 4)

    # Shrink the cache using count_max parameter
    instance.sql("ALTER SYSTEM SET sql_storage_cache_count_max = 2")
    # size = added - evicted
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_statements_added", 8)
    check_metric(metrics, "pico_storage_cache_statements_evicted", 6)

    # Shrink the cache using size_max parameter
    instance.sql("ALTER SYSTEM SET sql_storage_cache_size_max = 1")
    # size = added - evicted
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_statements_added", 8)
    check_metric(metrics, "pico_storage_cache_statements_evicted", 8)

    # Still can do some queries
    instance.sql("SELECT * FROM t JOIN t t2 on t.a = t2.a")


def get_instance_state_metric(instance: Instance) -> Dict[str, Sample]:
    metric = instance.get_metrics()["pico_instance_state"]
    samples_by_instance_name = {}
    for sample in metric.samples:
        samples_by_instance_name[sample.labels["instance"]] = sample

    return samples_by_instance_name


def all_but_first_report_online(cluster: Cluster, offline_name):
    for instance_to_fetch_metric in cluster.instances[1:]:
        samples_by_instance_name = get_instance_state_metric(instance_to_fetch_metric)

        for instance_to_check_state in cluster.instances:
            assert instance_to_check_state.name is not None  # mypy
            sample = samples_by_instance_name[instance_to_check_state.name]
            if instance_to_check_state.name == offline_name:
                assert sample.labels["state"] == "Offline"
            else:
                assert sample.labels["state"] == "Online"


def test_instance_state_metric(cluster: Cluster):
    i1, i2, i3 = cluster.deploy(instance_count=3, enable_http=True)

    # check after startup all instances report all other instances as Online
    for instance in cluster.instances:
        samples_by_instance_name = get_instance_state_metric(instance)

        for instance in cluster.instances:
            assert instance.name is not None  # mypy
            sample = samples_by_instance_name.get(instance.name)
            assert sample is not None
            assert sample.labels["tier"] == "default"
            assert sample.labels["state"] == "Online"

    # shut down one instance, ensure others see it as offline
    i1.terminate()
    i3.promote_or_fail()

    all_but_first_report_online(cluster, i1.name)

    i1.start_and_wait()

    # check that after instance became online again we see it everywhere as online
    for instance_with_metrics in cluster.instances:
        samples_by_instance_name = get_instance_state_metric(instance_with_metrics)

        for instance in cluster.instances:
            assert instance.name is not None  # mypy
            sample = samples_by_instance_name.get(instance.name)
            assert sample is not None, f"Missing {instance.name} in {instance_with_metrics} metrics"
            assert sample.labels["tier"] == "default"
            assert sample.labels["state"] == "Online"

    # make it offline again to check in snapshot later
    i1.terminate()

    all_but_first_report_online(cluster, i1.name)

    # resest raft_wal_count_max to trigger snapshot transfer
    i3.sql("ALTER SYSTEM SET raft_wal_count_max = 1")
    assert i3.call("box.space._raft_log:len") == 0

    # add new instance to trigger snapshot transfer, to check that metrics are updated with snapshot
    i4 = cluster.add_instance(enable_http=True, peers=[i2.iproto_listen])

    for instance in cluster.instances[1:]:
        samples_by_instance_name = get_instance_state_metric(instance)
        assert i4.name in samples_by_instance_name

        all_but_first_report_online(cluster, i1.name)


@pytest.mark.webui
def test_storage_cache_mestrics(instance: Instance):
    # All metrics uninitialized
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_hits", None)
    check_metric(metrics, "pico_storage_cache_misses", None)

    # Create a postgres user to test metrics via pgproto
    instance.sql("CREATE USER postgres WITH PASSWORD 'Passw0rd'")
    host, port = instance.pg_host, instance.pg_port
    pgproto = psycopg.connect(f"postgres://postgres:Passw0rd@{host}:{port}")
    pgproto.autocommit = True  # do not make a transaction
    instance.sql("GRANT CREATE TABLE TO postgres", sudo=True)

    # ACL does not access the storage cache.
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_hits", None)
    check_metric(metrics, "pico_storage_cache_misses", None)

    # Create and fill table
    pgproto.execute("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
    pgproto.execute("INSERT INTO t VALUES (1,2)")
    pgproto.execute("INSERT INTO t VALUES (2,3)")

    # DDL and DML do not access the storage cache.
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_hits", None)
    check_metric(metrics, "pico_storage_cache_misses", None)

    # DQL true cache misses are reported
    pgproto.execute("SELECT a FROM t")
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_hits", None)
    check_metric(metrics, "pico_storage_cache_misses", 1, query_type="dql", rpc_type="1st", miss_type="true")
    check_metric(metrics, "pico_storage_cache_misses", 1, query_type="dql", rpc_type="2nd", miss_type="true")

    # DQL cache hits are reported
    pgproto.execute("SELECT a FROM t")
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_hits", 1, query_type="dql", rpc_type="1st")

    # DQL stale cache misses are reported after schema changes.
    pgproto.execute("CREATE TABLE t2 (a INT PRIMARY KEY, b INT)")
    pgproto.execute("SELECT a FROM t")
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_hits", 1, query_type="dql", rpc_type="1st")
    check_metric(metrics, "pico_storage_cache_misses", 1, query_type="dql", rpc_type="1st", miss_type="stale")

    # Recompilation due to schema changes is reported.
    pgproto.execute("CREATE INDEX ib ON t(b)")
    pgproto.execute("SELECT a FROM t")
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_hits", 1, query_type="dql", rpc_type="1st")
    check_metric(metrics, "pico_storage_cache_misses", 2, query_type="dql", rpc_type="1st", miss_type="stale")

    # ALTER TABLE changes picodata's schema version leading to
    # complete recompilation of the plan and the statement from scratch.
    pgproto.execute("ALTER TABLE t ADD COLUMN c INT")
    pgproto.execute("SELECT a FROM t")
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_hits", 1, query_type="dql", rpc_type="1st")
    check_metric(metrics, "pico_storage_cache_misses", 2, query_type="dql", rpc_type="1st", miss_type="true")
    check_metric(metrics, "pico_storage_cache_misses", 2, query_type="dql", rpc_type="2nd", miss_type="true")

    # `pico_storage_cache_busy_misses_total` cannot be reported at the moment
    # due to the lock on the storage cache.
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_misses", None, query_type="dql", rpc_type="1st", miss_type="busy")
    check_metric(metrics, "pico_storage_cache_misses", None, query_type="dql", rpc_type="2nd", miss_type="busy")


@pytest.mark.webui
def test_local_sql_collisions_gl_2367(instance: Instance):
    # Create a postgres user to test metrics via pgproto
    instance.sql("CREATE USER postgres WITH PASSWORD 'Passw0rd'")
    host, port = instance.pg_host, instance.pg_port
    pgproto = psycopg.connect(f"postgres://postgres:Passw0rd@{host}:{port}")
    pgproto.autocommit = True  # do not make a transaction
    instance.sql("GRANT CREATE TABLE TO postgres", sudo=True)

    # Create and fill table
    pgproto.execute("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
    pgproto.execute("INSERT INTO t VALUES (1,2)")

    # All metrics uninitialized
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_hits", None, query_type="dql", rpc_type="1st")
    check_metric(metrics, "pico_storage_cache_misses", None, query_type="dql", rpc_type="1st", miss_type="true")
    check_metric(metrics, "pico_storage_cache_misses", None, query_type="dql", rpc_type="1st", miss_type="stale")

    # Ensure statements with colliding local SQL are cached twice without any problems.
    # (https://git.picodata.io/core/picodata/-/issues/2367)

    # Assert local SQL collides.
    raw1 = pgproto.execute("EXPLAIN (raw) SELECT DISTINCT * FROM t ORDER BY 1").fetchall()
    raw2 = pgproto.execute("EXPLAIN (raw) SELECT * FROM t GROUP BY a, b ORDER BY 1").fetchall()
    assert raw1[1] == raw2[1]

    # EXPLAIN shouldn't be reported.
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_hits", None, query_type="dql", rpc_type="1st")
    check_metric(metrics, "pico_storage_cache_misses", None, query_type="dql", rpc_type="1st", miss_type="true")
    check_metric(metrics, "pico_storage_cache_misses", None, query_type="dql", rpc_type="1st", miss_type="stale")

    # Cache statements.
    pgproto.execute("SELECT DISTINCT * FROM t ORDER BY 1")

    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_hits", None, query_type="dql", rpc_type="1st")
    # Motion materialization gives one true miss.
    check_metric(metrics, "pico_storage_cache_misses", 1, query_type="dql", rpc_type="1st", miss_type="true")
    # Then the router executes the top query locally.
    check_metric(metrics, "pico_storage_cache_misses", 1, query_type="dql", rpc_type="local", miss_type="true")

    # Cache another statements with the same local SQL.
    pgproto.execute("SELECT * FROM t GROUP BY a, b ORDER BY 1")
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_hits", None, query_type="dql", rpc_type="1st")
    # Motion materialization gives one true miss.
    check_metric(metrics, "pico_storage_cache_misses", 2, query_type="dql", rpc_type="1st", miss_type="true")
    # Then the router executes the top query locally.
    check_metric(metrics, "pico_storage_cache_misses", 2, query_type="dql", rpc_type="local", miss_type="true")

    # Run cached statements.
    # Due to temporary spaces creation stale misses will be reported on local and 1st requests.
    pgproto.execute("SELECT DISTINCT * FROM t ORDER BY 1")
    pgproto.execute("SELECT * FROM t GROUP BY a, b ORDER BY 1")
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_hits", None, query_type="dql", rpc_type="1st")
    check_metric(metrics, "pico_storage_cache_hits", 1, query_type="dql", rpc_type="local")
    check_metric(metrics, "pico_storage_cache_misses", 2, query_type="dql", rpc_type="1st", miss_type="true")
    check_metric(metrics, "pico_storage_cache_misses", 2, query_type="dql", rpc_type="local", miss_type="true")
    check_metric(metrics, "pico_storage_cache_misses", 1, query_type="dql", rpc_type="local", miss_type="stale")
    check_metric(metrics, "pico_storage_cache_misses", 2, query_type="dql", rpc_type="1st", miss_type="stale")

    # Run cached statements again after recompilation with the current schema verions.
    pgproto.execute("SELECT DISTINCT * FROM t ORDER BY 1")
    pgproto.execute("SELECT * FROM t GROUP BY a, b ORDER BY 1")
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_hits", 2, query_type="dql", rpc_type="1st")
    check_metric(metrics, "pico_storage_cache_hits", 3, query_type="dql", rpc_type="local")
    check_metric(metrics, "pico_storage_cache_misses", 2, query_type="dql", rpc_type="1st", miss_type="true")
    check_metric(metrics, "pico_storage_cache_misses", 2, query_type="dql", rpc_type="local", miss_type="true")
    check_metric(metrics, "pico_storage_cache_misses", 1, query_type="dql", rpc_type="local", miss_type="stale")
    check_metric(metrics, "pico_storage_cache_misses", 2, query_type="dql", rpc_type="1st", miss_type="stale")

    # Run cached statements one more time.
    pgproto.execute("SELECT DISTINCT * FROM t ORDER BY 1")
    pgproto.execute("SELECT * FROM t GROUP BY a, b ORDER BY 1")
    metrics = instance.get_metrics()
    check_metric(metrics, "pico_storage_cache_hits", 4, query_type="dql", rpc_type="1st")
    check_metric(metrics, "pico_storage_cache_hits", 5, query_type="dql", rpc_type="local")
    check_metric(metrics, "pico_storage_cache_misses", 2, query_type="dql", rpc_type="1st", miss_type="true")
    check_metric(metrics, "pico_storage_cache_misses", 2, query_type="dql", rpc_type="local", miss_type="true")
    check_metric(metrics, "pico_storage_cache_misses", 1, query_type="dql", rpc_type="local", miss_type="stale")
    check_metric(metrics, "pico_storage_cache_misses", 2, query_type="dql", rpc_type="1st", miss_type="stale")
