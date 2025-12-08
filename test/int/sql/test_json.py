import psycopg
import pytest
from conftest import (
    Cluster,
)


def test_json_extract(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1)
    user = "postgres"
    password = "Passw0rd"
    i1.sql(f"CREATE USER {user} WITH PASSWORD '{password}'")
    i1.sql(f"GRANT CREATE TABLE TO {user}", sudo=True)
    host, port = i1.pg_host, i1.pg_port
    conn = psycopg.connect(f"postgres://{user}:{password}@{host}:{port}")
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("CREATE TABLE t (id int, col json, primary key (id))")
    i1.eval("""box.space.t:insert{1, 1, require('json').decode('{"a":{"b": "c"},"d":{"e":[1,2,3]},"f":42}')}""")
    i1.eval("""box.space.t:insert{2, 1, require('json').decode('{"a":2}')}""")
    res = cur.execute("SELECT * FROM t").fetchall()
    assert res == [
        (
            1,
            {
                "a": {"b": "c"},
                "d": {"e": [1, 2, 3]},
                "f": 42,
            },
        ),
        (2, {"a": 2}),
    ]

    res = cur.execute("SELECT JSON_EXTRACT_PATH(col, 'a') FROM t WHERE id = 1").fetchall()
    assert res == [({"b": "c"},)]

    res = cur.execute("SELECT JSON_EXTRACT_PATH(col, 'a') FROM t").fetchall()
    assert res == [({"b": "c"},), (2,)]

    res = cur.execute("SELECT JSON_EXTRACT_PATH(col, 'a', 'b') FROM t").fetchall()
    assert res == [("c",), (None,)]

    res = cur.execute("SELECT JSON_EXTRACT_PATH(col, 'a', 'b', 'c') FROM t").fetchall()
    assert res == [(None,), (None,)]

    res = cur.execute("SELECT JSON_EXTRACT_PATH(col, 'a', 'b', 'c', 'd') FROM t").fetchall()
    assert res == [(None,), (None,)]

    res = cur.execute("SELECT JSON_EXTRACT_PATH(col, 'b') FROM t").fetchall()
    assert res == [(None,), (None,)]

    with pytest.raises(
        psycopg.errors.InternalError, match="could not resolve function overload for json_extract_path\\(map\\)"
    ):
        res = cur.execute("SELECT JSON_EXTRACT_PATH(col) FROM t").fetchall()

    with pytest.raises(
        psycopg.errors.InternalError, match="could not resolve function overload for json_extract_path\\(\\)"
    ):
        res = cur.execute("SELECT JSON_EXTRACT_PATH() FROM t").fetchall()

    with pytest.raises(
        psycopg.errors.InternalError, match="could not resolve function overload for json_extract_path\\(int, text\\)"
    ):
        res = cur.execute("SELECT JSON_EXTRACT_PATH(id, 'a') FROM t").fetchall()

    with pytest.raises(
        psycopg.errors.InternalError,
        match="could not resolve function overload for json_extract_path\\(map, text, int, text\\)",
    ):
        res = cur.execute("SELECT JSON_EXTRACT_PATH(col, 'a', 1, 'b') FROM t").fetchall()

    with pytest.raises(
        psycopg.errors.InternalError, match="could not resolve function overload for json_extract_path\\(map, int\\)"
    ):
        res = cur.execute("SELECT JSON_EXTRACT_PATH(col, 1) FROM t").fetchall()

    res = cur.execute("SELECT JSON_EXTRACT_PATH(col, 'a', 'b')::text FROM t").fetchall()
    assert res == [("c",), (None,)]

    res = cur.execute("SELECT JSON_EXTRACT_PATH(col, 'a', (SELECT 'b')) FROM t").fetchall()
    assert res == [("c",), (None,)]

    res = cur.execute("SELECT JSON_EXTRACT_PATH(col, 'd', 'e') FROM t").fetchall()
    assert res == [([1, 2, 3],), (None,)]

    with pytest.raises(psycopg.errors.InternalError, match="cannot index expression of type any"):
        res = cur.execute("SELECT JSON_EXTRACT_PATH(col, 'd', 'e')[1] FROM t").fetchall()

    res = cur.execute("SELECT JSON_EXTRACT_PATH(col, 'd', NULL) FROM t").fetchall()
    assert res == [(None,), (None,)]

    res = cur.execute("SELECT JSON_EXTRACT_PATH(col, NULL) FROM t").fetchall()
    assert res == [(None,), (None,)]

    res = cur.execute("SELECT JSON_EXTRACT_PATH(NULL, NULL) FROM t").fetchall()
    assert res == [(None,), (None,)]

    res = cur.execute("SELECT JSON_EXTRACT_PATH(col, 'f')::int FROM t").fetchall()
    assert res == [(42,), (None,)]

    res = cur.execute("SELECT id, col FROM t WHERE id = 2 AND id = JSON_EXTRACT_PATH(col, 'a')::int").fetchall()
    assert [[2, {"a": 2}]]
