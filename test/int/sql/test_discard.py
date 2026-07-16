import re
import threading

import pytest
from conftest import Instance, TarantoolError, log_crawler


def _metric_total(instance: Instance, name: str) -> float:
    """Sum all samples of a counter family."""
    family = instance.get_metrics().get(name)
    if family is None:
        return 0.0
    return sum(s.value for s in family.samples)


BLOCK = "DO $$ BEGIN RETURN QUERY SELECT b FROM t WHERE a = 1; END $$;"


def test_discard_cache_sql_stmt(instance: Instance):
    instance.sql("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
    instance.sql("INSERT INTO t VALUES (1, 2)")
    instance.sql("SELECT * FROM t")

    before = _metric_total(instance, "pico_storage_cache_statements_evicted")
    flushed = instance.sql("DISCARD CACHE SQL_STMT")["row_count"]
    after = _metric_total(instance, "pico_storage_cache_statements_evicted")

    # The returned row count equals the number of entries actually evicted.
    assert after == before + flushed

    assert instance.sql("DISCARD CACHE SQL_STMT")["row_count"] == 0

    instance.sql("SELECT * FROM t")
    assert instance.sql("DISCARD CACHE SQL_STMT")["row_count"] == 1


def test_discard_cache_sql_router(instance: Instance):
    instance.sql("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
    instance.sql("INSERT INTO t VALUES (1, 2)")
    instance.sql("SELECT * FROM t")

    before = _metric_total(instance, "pico_router_cache_statements_evicted")
    flushed = instance.sql("DISCARD CACHE SQL_ROUTER")["row_count"]
    after = _metric_total(instance, "pico_router_cache_statements_evicted")

    # The returned row count equals the number of entries actually evicted.
    assert after == before + flushed

    # DISCARD itself is never cached.
    assert instance.sql("DISCARD CACHE SQL_ROUTER")["row_count"] == 0

    instance.sql("SELECT * FROM t")
    assert instance.sql("DISCARD CACHE SQL_ROUTER")["row_count"] == 1


def test_discard_cache_sql_block(instance: Instance):
    instance.sql("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
    instance.sql("INSERT INTO t VALUES (1, 2)")

    assert instance.sql("DISCARD CACHE SQL_BLOCK")["row_count"] == 0

    instance.sql(BLOCK)
    instance.sql(BLOCK)

    before = _metric_total(instance, "pico_router_block_pattern_cache_statements_evicted")
    flushed = instance.sql("DISCARD CACHE SQL_BLOCK")["row_count"]
    after = _metric_total(instance, "pico_router_block_pattern_cache_statements_evicted")

    # The returned row count equals the number of entries actually evicted.
    assert after == before + flushed

    assert instance.sql("DISCARD CACHE SQL_BLOCK")["row_count"] == 0

    instance.sql(BLOCK)
    instance.sql("DO $$ BEGIN RETURN QUERY SELECT b FROM t WHERE a = 1 LIMIT 1; END $$;")
    assert instance.sql("DISCARD CACHE SQL_BLOCK")["row_count"] == 2


def test_discard_cache(instance: Instance):
    instance.sql("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
    instance.sql("INSERT INTO t VALUES (1, 2)")
    instance.sql("SELECT * FROM t")
    instance.sql(BLOCK)

    METRICS = (
        "pico_storage_cache_statements_evicted",
        "pico_router_cache_statements_evicted",
        "pico_router_block_pattern_cache_statements_evicted",
    )
    before = sum(map(lambda name: _metric_total(instance, name), METRICS))
    flushed = instance.sql("DISCARD CACHE")["row_count"]
    after = sum(map(lambda name: _metric_total(instance, name), METRICS))

    # The returned row count equals the number of entries actually evicted.
    assert after == before + flushed

    instance.sql("SELECT * FROM t")
    assert instance.sql("discard cache")["row_count"] == 2
    assert instance.sql("Discard Cache Sql_Block")["row_count"] == 0


@pytest.mark.parametrize(
    "query",
    [
        "DISCARD",
        "DISCARD foo",
        "DISCARD ALL",
        "DISCARD SQL_STMT",
        "DISCARD CACHE ALL",
        "DISCARD CACHE SQL_STMT SQL_BLOCK",
        "DISCARDCACHE",
        "DISCARD 'cache'",
        "DISCARD CACHE 'sql_stmt'",
        "EXPLAIN DISCARD CACHE",
    ],
)
def test_discard_syntax_errors(instance: Instance, query: str):
    with pytest.raises(TarantoolError, match="rule parsing error"):
        instance.sql(query)


@pytest.mark.parametrize(
    "query, name",
    [
        ("DISCARD CACHE SQLSTMT", '"sqlstmt"'),
        ("DISCARD CACHE foo", '"foo"'),
        ('DISCARD CACHE "SQL_STMT"', '"SQL_STMT"'),
    ],
)
def test_discard_unknown_cache(instance: Instance, query: str, name: str):
    error = f"unsupported cache: {name} (supported: sql_stmt, sql_router, sql_block)"
    with pytest.raises(TarantoolError, match=re.escape(error)):
        instance.sql(query)


def test_discard_quoted_cache_name(instance: Instance):
    instance.sql("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
    instance.sql("SELECT * FROM t")
    assert instance.sql('DISCARD CACHE "sql_router"')["row_count"] == 1


def test_discard_access_denied(instance: Instance):
    user = "alice"
    password = "T0psecret"
    instance.sql(f"CREATE USER {user} WITH PASSWORD '{password}' USING chap-sha1")

    for kind in ["CACHE SQL_STMT", "CACHE SQL_ROUTER", "CACHE SQL_BLOCK", "CACHE"]:
        with pytest.raises(TarantoolError, match=f"DISCARD access is denied for user '{user}'"):
            instance.sql(f"DISCARD {kind}", user=user, password=password)


def test_discard_keywords_are_not_reserved(instance: Instance):
    instance.sql("CREATE TABLE discard (cache INT PRIMARY KEY, sql_stmt INT, sql_router INT, sql_block INT)")
    instance.sql("INSERT INTO discard VALUES (1, 2, 3, 4)")
    assert instance.sql("SELECT cache, sql_stmt, sql_router, sql_block FROM discard") == [[1, 2, 3, 4]]


# A query is pinned right after inserting the first row of its temporary table
POPULATE_INJECTION = "BLOCK_POPULATE_TABLE_BEFORE_YIELD"
POPULATE_BLOCKING_LOG = f"ERROR INJECTION '{POPULATE_INJECTION}': BLOCKING"

SQL_QUERY = "SELECT a FROM t WHERE b IN (SELECT b FROM t)"
SQL_EXPECTED = [[1], [2], [3], [4], [5]]


@pytest.mark.parametrize("kind", ["CACHE SQL_STMT", "CACHE SQL_ROUTER", "CACHE"])
def test_discard_while_statement_is_executing(instance: Instance, kind: str):
    """Flushing the cache while a statement is executing works fine."""

    instance.sql("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
    instance.sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")

    # Fill the caches
    assert instance.sql(SQL_QUERY) == SQL_EXPECTED

    result = {}
    error = {}

    def worker():
        try:
            result["rows"] = instance.sql(SQL_QUERY)
        except Exception as e:
            error["err"] = e

    blocked = log_crawler(instance, POPULATE_BLOCKING_LOG)
    instance.call("pico._inject_error", POPULATE_INJECTION, True)

    worker_thread = threading.Thread(target=worker, name="motion-query")
    worker_thread.start()
    try:
        # Wait until the query is pinned inside `populate_table()`
        blocked.wait_matched()

        flushed = instance.sql(f"DISCARD {kind}")
        assert flushed["row_count"] >= 1, "the executing statement should have been cached"
    finally:
        instance.call("pico._inject_error", POPULATE_INJECTION, False)
        worker_thread.join(timeout=60)

    assert not worker_thread.is_alive(), "the query did not finish after unblocking"

    instance.check_process_alive()
    assert "err" not in error, f"query failed after cache flush: {error.get('err')}"
    assert result.get("rows") == SQL_EXPECTED

    # Sanity check
    assert instance.sql(SQL_QUERY) == SQL_EXPECTED
