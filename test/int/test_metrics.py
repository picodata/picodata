from typing import Iterable

from conftest import (
    Cluster,
    Instance,
    TarantoolError,
    ErrorCode,
)
import pytest
import requests  # type: ignore
from prometheus_client.parser import text_string_to_metric_families
from prometheus_client import Metric
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


def get_metrics(http_listen):
    response = requests.get(f"http://{http_listen}/metrics")
    response.raise_for_status()
    return list(text_string_to_metric_families(response.text))


def check_metric(families: Iterable[Metric], name: str, value: float | None):
    found_family = None
    for family in families:
        print(family.name, name)
        if family.name == name:
            found_family = family
            break
    if value is None:
        assert found_family is None, "Metric {} found".format(name)
    else:
        assert found_family is not None, "Metric {} not found".format(name)
        assert len(found_family.samples) == 1, "Metric has {} samples instead of 1".format(len(found_family.samples))
        sample = found_family.samples[0]
        assert sample.value == value, "Metric has unexpected value"


@pytest.mark.webui
def test_pgproto_metrics_collected(instance: Instance) -> None:
    http_listen = instance.env["PICODATA_HTTP_LISTEN"]

    metrics = get_metrics(http_listen)
    check_metric(metrics, "pico_sql_query", None)

    # make a non-privileged user with iproto
    instance.sql("CREATE USER postgres WITH PASSWORD 'Passw0rd'")

    # creating a user for postgress will increase the metric value, so check it
    metrics = get_metrics(http_listen)
    check_metric(metrics, "pico_sql_query", 1.0)

    # execute a query through postgresql
    host, port = instance.pg_host, instance.pg_port
    conn = psycopg.connect(f"postgres://postgres:Passw0rd@{host}:{port}")
    conn.autocommit = True  # do not make a transaction

    conn.execute("SELECT 1").fetchall()

    # the select above should've increase the metric value too
    metrics = get_metrics(http_listen)
    check_metric(metrics, "pico_sql_query", 2.0)


@pytest.mark.webui
def test_router_and_storage_cache_metrics(instance: Instance):
    http_listen = instance.env["PICODATA_HTTP_LISTEN"]

    metrics = get_metrics(http_listen)
    check_metric(metrics, "pico_router_cache_misses", 0)

    ################
    # Run ACL query
    ################

    # Create a postgres user to test caches via pgproto
    instance.sql("CREATE USER postgres WITH PASSWORD 'Passw0rd'")
    host, port = instance.pg_host, instance.pg_port
    pgproto = psycopg.connect(f"postgres://postgres:Passw0rd@{host}:{port}")
    pgproto.autocommit = True  # do not make a transaction

    metrics = get_metrics(http_listen)

    # ACL is not cached
    check_metric(metrics, "pico_router_cache_statements_added", 0)
    check_metric(metrics, "pico_storage_cache_statements_added", 0)

    # Every query (even ACL) is looked in the router cache so miss is reported
    check_metric(metrics, "pico_router_cache_misses", 1)
    # ACL is not executed on storage so no requests are reported
    check_metric(metrics, "pico_storage_cache_1st_requests", 0)
    check_metric(metrics, "pico_storage_cache_2nd_requests", 0)

    ################
    # Run DDL query
    ################

    instance.sql("CREATE TABLE t (a INT PRIMARY KEY, b INT)")

    metrics = get_metrics(http_listen)

    # Every query (even DDL) is looked in the router cache so miss is reported
    check_metric(metrics, "pico_router_cache_misses", 2)
    # ACL is not executed on storage so no requests are reported
    check_metric(metrics, "pico_storage_cache_1st_requests", 0)
    check_metric(metrics, "pico_storage_cache_2nd_requests", 0)

    ################
    # Run DML query
    ################

    instance.sql("INSERT INTO t VALUES (1,2)")

    metrics = get_metrics(http_listen)

    # Every query is looked in the router cache so miss is reported
    check_metric(metrics, "pico_router_cache_misses", 3)
    # DML is cached on the router
    check_metric(metrics, "pico_router_cache_statements_added", 1)
    # We don't cache in the storage cache DML so no requests are reported
    check_metric(metrics, "pico_storage_cache_1st_requests", 0)
    check_metric(metrics, "pico_storage_cache_2nd_requests", 0)

    ##########################
    # Execute first DQL query
    ##########################

    instance.sql("SELECT * FROM t")

    # DQL statement is cached
    metrics = get_metrics(http_listen)
    check_metric(metrics, "pico_router_cache_statements_added", 2)
    check_metric(metrics, "pico_storage_cache_statements_added", 1)

    # Router cache miss is reported
    metrics = get_metrics(http_listen)
    check_metric(metrics, "pico_router_cache_misses", 4)

    # Both requests to the storage cache are reported (i.e. cache miss)
    check_metric(metrics, "pico_storage_cache_1st_requests", 1)
    check_metric(metrics, "pico_storage_cache_2nd_requests", 1)

    #####################################
    # Run the same DQL query via pgproto
    #####################################

    instance.sql("SELECT * FROM t")

    # Statement wasn't cached again
    metrics = get_metrics(http_listen)
    check_metric(metrics, "pico_router_cache_statements_added", 2)
    check_metric(metrics, "pico_storage_cache_statements_added", 1)

    # Cache hit is reported on the router
    metrics = get_metrics(http_listen)
    check_metric(metrics, "pico_router_cache_hits", 1)

    # Only 1st requests to the storage cache is reported (i.e. cache hit)
    check_metric(metrics, "pico_storage_cache_1st_requests", 2)
    check_metric(metrics, "pico_storage_cache_2nd_requests", 1)

    ##############################################
    # Ensure storage cache evictions are reported
    ##############################################

    # Make storage cache full
    instance.sql("ALTER SYSTEM SET sql_storage_cache_count_max = 1")
    # Nothing evicted yet
    metrics = get_metrics(http_listen)
    check_metric(metrics, "pico_storage_cache_statements_evicted", 0)
    check_metric(metrics, "pico_storage_cache_statements_added", 1)
    # Overflow the cache
    instance.sql("SELECT a + b FROM t")
    # Ensure eviction is reported
    metrics = get_metrics(http_listen)
    check_metric(metrics, "pico_storage_cache_statements_evicted", 1)
    check_metric(metrics, "pico_storage_cache_statements_added", 2)

    #############################################
    # Ensure router cache evictions are reported
    #############################################

    # There is already 3 entries in the cache
    metrics = get_metrics(http_listen)
    check_metric(metrics, "pico_router_cache_statements_added", 3)
    # Make router cache full
    for i in range(0, 47):
        instance.sql(f"SELECT a + {i} FROM t")
    # Nothing evicted yet
    metrics = get_metrics(http_listen)
    check_metric(metrics, "pico_router_cache_statements_added", 50)
    check_metric(metrics, "pico_router_cache_statements_evicted", 0)
    # Overflow the cache
    instance.sql("SELECT a + b + 1 FROM t")
    # Ensure eviction is reported
    metrics = get_metrics(http_listen)
    check_metric(metrics, "pico_router_cache_statements_added", 51)
    check_metric(metrics, "pico_router_cache_statements_evicted", 1)
