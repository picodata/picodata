"""Check SQL results of the facts-based 0-1 bucket proof (with its executor
witness) on a cluster of two replicasets, against a sqlite oracle on the same
rows (bucket_id copied from the cluster), on populated and empty data.

`test_facts_bucket_proof_matches_oracle` asserts a compact list of read cases
over iproto, then applies a list of DML cases to the cluster and the oracle
and compares the rows; `test_facts_bucket_proof_over_pgproto` runs the read
cases as prepared statements, with the parameter types of the client. They
hold with and without the `enrich_restrictions` feature. A stage that routes
a node proven single-node wider fails, so a broken proof fails the test as an
error.
"""

import decimal
import re
import sqlite3
import uuid

import psycopg
import pytest
from conftest import Cluster

DDL = [
    "CREATE TABLE t5 (k INT PRIMARY KEY, a INT, b INT) DISTRIBUTED BY (a)",
    "CREATE TABLE t (k INT PRIMARY KEY, a INT, b INT, c INT, d INT) DISTRIBUTED BY (a, b)",
    "CREATE TABLE t1 (k INT PRIMARY KEY, a STRING, b INT) DISTRIBUTED BY (a, b)",
    "CREATE TABLE dec_t (k INT PRIMARY KEY, a DECIMAL, b INT, c DOUBLE) DISTRIBUTED BY (a)",
    "CREATE TABLE global_t (k INT PRIMARY KEY, a INT, b INT) DISTRIBUTED GLOBALLY",
    "CREATE TABLE test_space (id INT PRIMARY KEY, sys_op INT) DISTRIBUTED BY (id)",
    "CREATE TABLE bool_t (k INT PRIMARY KEY, b BOOL, c INT) DISTRIBUTED BY (b)",
    "CREATE TABLE uuid_t (k INT PRIMARY KEY, a UUID, b INT) DISTRIBUTED BY (a)",
]
U1, U2 = "6f2ba4c4-0a4c-4d79-86ae-43d4f84b70e1", "0d5a5a8e-1b0c-4a3a-9d0e-2b3c4d5e6f70"

# Duplicates, NULL keys, unmatched join rows and keys on both replicasets.
ROWS = {
    "t5": ["k", "a", "b"],
    "t": ["k", "a", "b", "c", "d"],
    "t1": ["k", "a", "b"],
    "dec_t": ["k", "a", "b", "c"],
    "global_t": ["k", "a", "b"],
    "test_space": ["id", "sys_op"],
    "bool_t": ["k", "b", "c"],
    "uuid_t": ["k", "a", "b"],
}
DATA = {
    "t5": [
        (1, 1, 1),
        (2, 1, 2),
        (3, 1, None),
        (4, 2, 1),
        (5, 2, 2),
        (6, 3, 1),
        (7, 3, 3),
        (8, None, 1),
        (9, 4, 2),
        (10, 1, 1),
        (11, 5, 7),
        (12, 6, 2),
    ],
    "t": [
        (1, 1, 2, 3, 1),
        (2, 1, 2, 1, 2),
        (3, 1, 2, 2, 3),
        (4, 1, 1, 1, 1),
        (5, 2, 2, 1, 1),
        (6, 2, 1, 3, 3),
        (7, 3, 1, 1, 1),
    ],
    "t1": [(1, "admin", 1), (2, "guest", 1), (3, "x", 1), (4, "admin", 2)],
    "dec_t": [(1, "1", 10, 1.0), (2, "1.5", 100, 2.0), (3, "2", 10, 3.0), (4, "1", 100, 4.0)],
    "global_t": [(1, 1, 1), (2, 2, 2), (3, None, 3)],
    "test_space": [(i, i % 3) for i in range(1, 9)],
    "bool_t": [(1, True, 1), (2, False, 2), (3, None, 3), (4, True, 4)],
    "uuid_t": [(1, U1, 1), (2, U2, 2), (3, U1, 3), (4, None, 4)],
}


def sql_literal(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, str):
        if re.fullmatch(r"\d+(\.\d+)?", v):
            return v  # decimal literal
        if v in (U1, U2):
            return f"'{v}'::uuid"
        return f"'{v}'"
    return repr(v)


def normalize(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return normalize(int(value))
    if isinstance(value, (int, float, decimal.Decimal)):
        return str(decimal.Decimal(str(value)).normalize())
    return str(value)


def multiset(rows):
    return sorted((tuple(normalize(v) for v in row) for row in rows), key=repr)


def to_sqlite(sql: str, user: str) -> str | None:
    s = sql.replace("::decimal", "").replace("::uuid", "").replace("int '1'", "1")
    s = re.sub(r"\$(\d)", r"?\1", s)
    s = re.sub(r"\bcurrent_user\b", f"'{user}'", s)
    return s


def deploy(cluster: Cluster):
    for _ in range(2):
        cluster.add_instance(wait_online=False, init_replication_factor=1)
    cluster.wait_online()
    cluster.wait_until_buckets_balanced()
    i1 = cluster.instances[0]
    for ddl in DDL:
        i1.sql(ddl)
    return i1


def fill(i1, tables_rows):
    for table, rows in tables_rows.items():
        if not rows:
            continue
        cols = ", ".join(ROWS[table])
        values = ", ".join("(" + ", ".join(sql_literal(v) for v in row) + ")" for row in rows)
        i1.sql(f"INSERT INTO {table} ({cols}) VALUES {values}")


def oracle(i1, tables_rows) -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    for table, cols in ROWS.items():
        db.execute(f"CREATE TABLE {table} ({', '.join(cols)}, bucket_id)")
        bucket_id = "NULL" if table == "global_t" else "bucket_id"
        rows = i1.sql(f"SELECT {', '.join(cols)}, {bucket_id} FROM {table}") if tables_rows[table] else []
        for row in rows:
            row = to_sqlite_params(row)
            db.execute(f"INSERT INTO {table} VALUES ({', '.join('?' * len(row))})", row)
    return db


# The permanent check: a compact list of the shapes the facts proof must keep
# correct, compared with the sqlite oracle in every `pytest` run. Each case is
# (sql, bindings); in a binding, `BUCKET`, `BUCKET_DEC` and `BUCKET_HALF` are
# the bucket of `test_space.id = 1` as an integer, a decimal and plus 0.5.
BUCKET, BUCKET_DEC, BUCKET_HALF = object(), object(), object()
D = decimal.Decimal
NULL_1_2 = [(None,), (1,), (2,)]
CASES = [
    # duplicates, empty input, a scalar aggregate on no rows
    ("SELECT a, b FROM t5 WHERE a = $1", NULL_1_2),
    ("SELECT count(*) FROM t5 WHERE a = $1", NULL_1_2),
    ("SELECT a, count(*) FROM t5 WHERE a = $1 GROUP BY a", NULL_1_2),
    # unmatched LEFT JOIN rows
    ("SELECT l.k, r.k FROM t5 AS l LEFT JOIN t5 AS r ON l.a = r.b WHERE l.a = $1", NULL_1_2),
    ("SELECT count(*) FROM t5 AS l LEFT JOIN (SELECT a FROM t5 WHERE a = $1) AS r ON l.a = r.a", NULL_1_2),
    # nested UNION, shared and different keys
    (
        "SELECT a FROM t5 WHERE a = $1 UNION ALL "
        "SELECT a FROM (SELECT a FROM t5 WHERE a = $1 UNION SELECT a FROM t5 WHERE a = $1) AS u",
        NULL_1_2,
    ),
    ("SELECT count(*) FROM t5 WHERE a = $1 UNION ALL SELECT count(*) FROM t WHERE a = $1 AND b = 2", NULL_1_2),
    ("SELECT count(*) FROM t5 WHERE a = 1 UNION ALL SELECT count(*) FROM t5 WHERE a = 2", [()]),
    # shared CTE
    ("WITH s AS (SELECT a, b FROM t5 WHERE a = $1) SELECT s1.b FROM s AS s1 JOIN s AS s2 ON s1.b = s2.b", NULL_1_2),
    (
        "WITH s AS (SELECT a FROM t5) "
        "SELECT count(*) FROM (SELECT a FROM s WHERE a = 1 UNION ALL SELECT a FROM s WHERE a = 2) AS u",
        [()],
    ),
    # Segment, Full and the key of the other join side
    ("SELECT l.a, r.k FROM (SELECT a FROM t5 WHERE a = $1) AS l JOIN t5 AS r ON l.a = r.b", NULL_1_2),
    ("SELECT g.k, s.a FROM global_t AS g JOIN (SELECT a FROM t5 WHERE a = $1) AS s ON g.a = s.a", NULL_1_2),
    ("SELECT count(*) FROM (SELECT b AS a FROM t5 GROUP BY b) AS s WHERE a = $1", NULL_1_2),
    # hashable types: the ordered composite key, strings, decimals
    ("SELECT k FROM t WHERE b = 2 AND a = $1", NULL_1_2),
    ("SELECT k FROM t1 WHERE a = $1 AND b = $2", [("admin", 1), ("guest", 1), ("admin", 2), (None, 1)]),
    ("SELECT k FROM t1 WHERE a = current_user AND b = 1", [()]),
    ("SELECT k FROM dec_t WHERE a = $1", [(D("1"),), (D("1.0"),), (D("1.5"),), (None,)]),
    ("SELECT k FROM dec_t WHERE a = 1.5", [()]),
    # A pin of another type than the key, hashed as written (as on master).
    ("SELECT k FROM dec_t WHERE a = 1", [()]),
    # bucket_id: integer, decimal, fractional, NULL and out of range
    (
        "SELECT id FROM test_space WHERE bucket_id = $1",
        [(BUCKET,), (BUCKET_DEC,), (BUCKET_HALF,), (None,), (-1,), (100000,)],
    ),
    # a key fixed only through another column, and through a join
    ("SELECT count(*) FROM t5 WHERE b = $1 AND a = b", NULL_1_2),
    ("SELECT k FROM t5 WHERE a = b AND b = $1", NULL_1_2),
    ("SELECT count(*) FROM (SELECT a FROM t5 WHERE a = $1) AS x JOIN t5 AS y ON x.a = y.a", NULL_1_2),
    # set operations, EXCEPT of a global input included
    ("SELECT a FROM t5 WHERE a = $1 UNION SELECT b FROM t5 WHERE a = $1", NULL_1_2),
    ("SELECT b FROM t5 WHERE a = $1 EXCEPT SELECT b FROM t5 WHERE a = $1 AND k > 2", NULL_1_2),
    ("SELECT a FROM global_t EXCEPT SELECT a FROM t5 WHERE a = $1", NULL_1_2),
    # subqueries on the bucket of their consumer, SELECT without FROM
    ("SELECT k FROM t5 WHERE a = $1 AND b IN (SELECT b FROM t5 WHERE a = $1)", NULL_1_2),
    ("SELECT k FROM t5 WHERE a = $1 AND EXISTS (SELECT 1 FROM t5 WHERE a = $1 AND b = 2)", NULL_1_2),
    ("SELECT (SELECT count(*) FROM t5 WHERE a = $1) AS c", NULL_1_2),
    # one-row operators on one bucket
    ("SELECT k FROM t5 WHERE a = $1 ORDER BY k LIMIT 2", NULL_1_2),
    ("SELECT k, row_number() OVER (ORDER BY k) AS rn FROM t5 WHERE a = $1", NULL_1_2),
    # BOOL and UUID keys, IS TRUE and IS FALSE as pins
    ("SELECT k FROM bool_t WHERE b IS TRUE", [()]),
    ("SELECT count(*) FROM bool_t WHERE b IS FALSE", [()]),
    ("SELECT count(*) FROM bool_t WHERE (b = true) IS TRUE", [()]),
    ("SELECT k FROM bool_t WHERE b = $1", [(True,), (False,), (None,)]),
    (f"SELECT k FROM uuid_t WHERE a = '{U1}'::uuid", [()]),
    ("SELECT count(*) FROM uuid_t WHERE a = $1", [(uuid.UUID(U1),), (uuid.UUID(U2),), (None,)]),
]
# (sql, binding) whose result on the populated rows differs from the oracle
# without the `enrich_restrictions` feature as well, as on master: the reason,
# and the rows the cluster returns, from the oracle and the bucket of
# `test_space.id = 1`. Asserted: a fix or another wrong result fails the test.
# Bindings are keyed by `repr`: `Decimal("1") == Decimal("1.0")` must not hide
# a regression.
KNOWN = {
    ("SELECT k FROM dec_t WHERE a = $1", repr((D("1.0"),))): (
        "decimal 1.0 routes by the hash of its representation, not to the bucket of 1",
        lambda db, bucket: [],
    ),
    ("SELECT id FROM test_space WHERE bucket_id = $1", repr((BUCKET_HALF,))): (
        "bucket_id = <bucket> + 0.5 matches the row of <bucket>; sqlite finds none",
        lambda db, bucket: db.execute("SELECT id FROM test_space WHERE bucket_id = ?", (bucket,)).fetchall(),
    ),
}

# DML over the read shapes above, applied to the cluster and the oracle in
# turn: after each binding both report the same row count and hold the same
# rows. In order: the later statements see the rows of the earlier ones.
DML_CASES = [
    ("INSERT INTO t5 (k, a, b) SELECT k + 100, a, b FROM t5 WHERE a = $1", NULL_1_2),
    ("UPDATE t5 SET a = a + 10 WHERE a = b AND b = $1", NULL_1_2),
    ("UPDATE t5 SET b = b + 10 WHERE a = $1", NULL_1_2),
    ("UPDATE t5 SET b = (SELECT count(*) FROM t5 WHERE a = $1) WHERE a = $1", NULL_1_2),
    ("DELETE FROM t5 WHERE a = $1 AND b IN (SELECT b FROM t5 WHERE a = $1)", NULL_1_2),
    ("DELETE FROM t5 WHERE b = $1 AND a = b", [(None,), (3,), (4,)]),
    ("DELETE FROM t5 WHERE a = $1", [(None,), (3,), (11,)]),
]


def bind(params, bucket):
    subst = {id(BUCKET): bucket, id(BUCKET_DEC): D(bucket), id(BUCKET_HALF): D(bucket) + D("0.5")}
    return tuple(subst.get(id(p), p) for p in params)


def to_sqlite_params(params):
    return tuple(float(p) if isinstance(p, D) else str(p) if isinstance(p, uuid.UUID) else p for p in params)


def read_mismatches(run, db, user, bucket, populated):
    """The CASES whose rows from `run(sql, params)` differ from the oracle,
    or from the result KNOWN for the populated rows."""
    mismatches = []
    for sql, bindings in CASES:
        for raw in bindings:
            params = bind(raw, bucket)
            got = multiset(run(sql, params))
            expected = multiset(db.execute(to_sqlite(sql, user), to_sqlite_params(params)).fetchall())
            known = KNOWN.get((sql, repr(raw)))
            if known and populated:
                expected = multiset(known[1](db, bucket))
            if got != expected:
                reason = f" (known: {known[0]})" if known else ""
                mismatches.append(f"{sql} {params}{reason}: got {got}, expected {expected}")
    return mismatches


@pytest.mark.parametrize("dataset", ["populated", "empty"])
def test_facts_bucket_proof_matches_oracle(cluster_factory, dataset):
    i1 = deploy(cluster_factory())
    rows = DATA if dataset == "populated" else {t: [] for t in DATA}
    fill(i1, rows)
    user = i1.sql("SELECT current_user")[0][0]
    db = oracle(i1, rows)
    bucket = i1.sql("SELECT bucket_id FROM test_space WHERE id = 1")
    bucket = bucket[0][0] if bucket else 1
    populated = dataset == "populated"
    mismatches = read_mismatches(lambda sql, params: i1.sql(sql, *params), db, user, bucket, populated)

    touched = 0
    for sql, bindings in DML_CASES:
        for raw in bindings:
            params = to_sqlite_params(bind(raw, bucket))
            got = i1.sql(sql, *params)["row_count"]
            expected = db.execute(to_sqlite(sql, user), params).rowcount
            touched += expected
            if got != expected:
                mismatches.append(f"{sql} {params}: {got} rows, oracle {expected}")
            got = multiset(i1.sql("SELECT k, a, b FROM t5"))
            expected = multiset(db.execute("SELECT k, a, b FROM t5").fetchall())
            if got != expected:
                mismatches.append(f"after {sql} {params}: got {got}, oracle {expected}")
    assert not mismatches, "\n".join(mismatches)
    assert touched or not populated, "the DML cases changed no row"


def test_facts_bucket_proof_over_pgproto(cluster_factory):
    """The read cases as prepared statements of a pgproto client: the
    parameters have the types the client declares, not those of their
    values. Each binding runs twice, the second time prepared."""
    i1 = deploy(cluster_factory())
    fill(i1, DATA)
    db = oracle(i1, DATA)
    bucket = i1.sql("SELECT bucket_id FROM test_space WHERE id = 1")[0][0]
    user, password = "facts_pg", "Passw0rd"
    i1.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    for table in ROWS:
        i1.sql(f'GRANT READ ON TABLE {table} TO "{user}"', sudo=True)
    conn = psycopg.connect(user=user, password=password, host=i1.pg_host, port=i1.pg_port, sslmode="disable")
    conn.autocommit = True
    cur = psycopg.RawCursor(conn)

    def run(sql, params):
        rows = []
        for _ in range(2):
            cur.execute(sql, params, prepare=True)
            rows.append(cur.fetchall())
        assert multiset(rows[0]) == multiset(rows[1]), sql
        return rows[0]

    assert not (mismatches := read_mismatches(run, db, user, bucket, True)), "\n".join(mismatches)


def buckets_line(explain: list[str]) -> str:
    return next(line for line in explain if line.startswith("buckets"))


@pytest.mark.parametrize("dataset", ["populated", "empty"])
def test_facts_route_disjoint_from_condition_route(cluster_factory, dataset):
    """Facts pin the key by `a = b AND b = 1` to the bucket of 1, the walk of
    the condition routes by `a IN (2, 3)`: both routes are nonempty and
    disjoint. The node is still proven single, so no Motion goes under the
    aggregate, and the scalar aggregate keeps its row on any node."""
    cluster = cluster_factory()
    i1 = deploy(cluster)
    i1.sql("CREATE TABLE tr (a INT PRIMARY KEY, b INT) DISTRIBUTED BY (a)")
    if dataset == "populated":
        i1.sql("INSERT INTO tr VALUES (1, 1), (2, 2), (3, 3)")
    one = buckets_line(i1.sql("EXPLAIN SELECT count(*) FROM tr WHERE a = 1"))
    query = "SELECT count(*) FROM tr WHERE a = b AND b = 1 AND a IN ({})"
    for in_list, buckets, count in [
        ("2, 3", "buckets = any", 0),
        ("1, 2", one, 1 if dataset == "populated" else 0),
    ]:
        sql = query.format(in_list)
        explain = i1.sql("EXPLAIN " + sql)
        assert not any("motion" in line for line in explain), explain
        assert buckets_line(explain) == buckets, explain
        for instance in cluster.instances:
            assert instance.sql(sql) == [[count]], (instance.name, sql)


def test_contradictory_inner_join_count_is_one_zero(cluster_factory):
    """`a = x` and `a = y` meet in one class across an INNER JOIN: the facts
    prove the join empty, so the one-stage `count(*)` runs on one node. With
    x and y on different replicasets, a lost row or a zero from each of them
    breaks the exact result."""
    cluster = cluster_factory()
    i1 = deploy(cluster)
    fill(i1, {"t5": DATA["t5"]})
    local_keys = "local r = {} for _, t in box.space.t5:pairs() do r[#r + 1] = t.a end return r"
    x, y = (min(a for a in instance.eval(local_keys) if a is not None) for instance in cluster.instances)
    assert x != y
    sql = (
        f"SELECT count(*) FROM (SELECT a FROM t5 WHERE a = {x}) AS l "
        f"JOIN (SELECT a FROM t5 WHERE a = {y}) AS r ON l.a = r.a"
    )
    assert not any("motion" in line for line in i1.sql("EXPLAIN " + sql))
    for instance in cluster.instances:
        assert instance.sql(sql) == [[0]], (instance.name, sql)
