from conftest import Cluster


# The columns of a table function have the names `value` and `ordinality` for
# iproto and pgproto clients, not the tarantool names.
def test_table_function_column_names(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1 = cluster.instances[0]

    queries = [
        ("SELECT * FROM generate_series(1, 2) WITH ORDINALITY", ["value", "ordinality"]),
        ("SELECT * FROM unnest(ARRAY[1], ARRAY['a']) WITH ORDINALITY", ["value_1", "value_2", "ordinality"]),
        ("SELECT g.value FROM generate_series(1, 2) g", ["value"]),
        ("SELECT * FROM (SELECT * FROM generate_series(1, 2)) s", ["value"]),
        ("SELECT value, count(*) FROM generate_series(1, 2) GROUP BY value", ["value", "col_1"]),
    ]
    for query, names in queries:
        result = i1.sql(query, strip_metadata=False)
        assert [column["name"] for column in result["metadata"]] == names, query

    i1.sql("CREATE USER postgres WITH PASSWORD 'Passw0rd'", sudo=True)
    pg_conn = i1.connect_via_pgproto(user="postgres", password="Passw0rd")
    for query, names in queries:
        cursor = pg_conn.execute(query)
        assert [column.name for column in cursor.description] == names, query
