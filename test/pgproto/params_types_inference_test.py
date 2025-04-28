import pytest
from conftest import Postgres
from decimal import Decimal
from uuid import UUID

# We use psycopg when we want the client to send parameters types explicitly.
import psycopg

# We use pg8000 when we want to prepare statements explicitly or when we don't want the client
# to send parameters types, which is useful when we test parameter types inference.
import pg8000.native as pg  # type: ignore
from pg8000.exceptions import DatabaseError  # type: ignore


def test_params_specified_via_cast(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    conn = pg.Connection(user, password=password, host=postgres.host, port=postgres.port)

    conn.run(
        """
        create table "tall" (
            "id" integer not null,
            "str" string,
            "bool" boolean,
            "real" double,
            primary key ("id")
        )
        using memtx distributed by ("id")
        option (timeout = 3);
    """
    )

    # Now all the types were specified.
    conn.run(
        """
        INSERT INTO "tall" VALUES (:p1::integer, :p2::text, :p3::bool, :p4::double);
        """,
        p1=-2,
        p2="string",
        p3=True,
        p4=3.141592,
    )

    rows = conn.run(""" SELECT * FROM "tall"; """)
    assert rows == [[-2, "string", True, 3.141592]]

    # Test an ambiguous parameter type error.
    with pytest.raises(DatabaseError, match=r"inconsistent types int and unsigned deduced for parameter \$1"):
        conn.run(
            """ SELECT "id" FROM "tall" WHERE "id" = :p1::integer + :p1::unsigned; """,
            p1=-1,
        )

    # Test an even more ambiguous parameter type error.
    with pytest.raises(DatabaseError, match=r"inconsistent types int and unsigned deduced for parameter \$1"):
        conn.run(
            """ SELECT "id" FROM "tall" \
                WHERE "id" = :p1::integer + :p1::unsigned + :p1::decimal; """,
            p1=-1,
        )

    rows = conn.run(""" SELECT * FROM "tall"; """)
    assert rows == [[-2, "string", True, 3.141592]]

    # Parameter can be cast to the same type in several places.
    rows = conn.run(""" SELECT "id" FROM "tall" WHERE "id" = :p1::integer + :p1::integer; """, p1=-1)
    assert rows == [[-2]]

    # It's OK to cast parameter only once and then use it without any cast.
    rows = conn.run(
        """ SELECT "id" FROM "tall" WHERE "id" = :p1::integer + :p1 + :p1 - :p1; """,
        p1=-1,
    )
    assert rows == [[-2]]

    # Test that we can calculate the type of an arithmetic expression like $1::integer + $1.
    rows = conn.run(
        """ SELECT :p1::integer + :p1 FROM "tall"; """,
        p1=-1,
    )
    assert rows == [[-2]]


def cols_oids(conn: pg.Connection) -> list[int]:
    return [col["type_oid"] for col in conn.columns]


def type_oid(name: str) -> int:
    return psycopg.adapters.types[name].oid


def test_params_inference_in_select(postgres: Postgres):
    user = "Парам Парамыч"
    password = "P@ssw0rd"

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    conn = pg.Connection(user, password=password, host=postgres.host, port=postgres.port)
    conn.run("CREATE TABLE t (i INT PRIMARY KEY, f DOUBLE, t TEXT, u UUID, d datetime);")

    rows = conn.run("SELECT 1 + :p", p=1)
    assert rows == [[2]]
    assert cols_oids(conn) == [type_oid("int8")]

    rows = conn.run("SELECT :p + 1", p=1)
    assert rows == [[2]]
    assert cols_oids(conn) == [type_oid("int8")]

    rows = conn.run("SELECT :p * 1.5", p=1)
    assert rows == [[Decimal(1.5)]]
    assert cols_oids(conn) == [type_oid("numeric")]

    rows = conn.run("SELECT :p / 1.0::double", p=1)
    assert rows == [[1.0]]
    assert cols_oids(conn) == [type_oid("float8")]

    rows = conn.run("SELECT :p / 1.0::double", p=1)
    assert rows == [[1.0]]
    assert cols_oids(conn) == [type_oid("float8")]

    # Ensure that associativity does not influence parameter types
    # `1 + :p + 1.5` and `1.5 + :p + 1` should have the same types
    rows = conn.run("SELECT 1 + :p + 1.5, :p ", p=1)
    assert rows == [[Decimal(3.5), Decimal(1)]]
    assert cols_oids(conn) == [type_oid("numeric"), type_oid("numeric")]

    rows = conn.run("SELECT 1.5 + :p + 1, :p ", p=1)
    assert rows == [[Decimal(3.5), Decimal(1)]]
    assert cols_oids(conn) == [type_oid("numeric"), type_oid("numeric")]

    # Ensure parentheses don't influence typing.
    rows = conn.run("SELECT 1.5 + ((:p) + (1)), :p ", p=1)
    assert rows == [[Decimal(3.5), Decimal(1)]]
    assert cols_oids(conn) == [type_oid("numeric"), type_oid("numeric")]

    # Ensure parameter is resolved to double if one of the operands is double.
    rows = conn.run("WITH t(d) as (select 1::double) SELECT :p + d, :p FROM t", p=1)
    assert rows == [[2.0, 1.0]]
    assert cols_oids(conn) == [type_oid("float8"), type_oid("float8")]

    # Ensure parameter is resolved to double if one of the operands is double.
    rows = conn.run("WITH t(d) as (select 1::double) SELECT :p + 1 + d, :p FROM t", p=1)
    assert rows == [[3.0, 1.0]]
    assert cols_oids(conn) == [type_oid("float8"), type_oid("float8")]

    # Ensure parameter is resolved to double if one of the operands is double.
    rows = conn.run("WITH t(d) as (select 1::double) SELECT :p + 1.5 + d, :p FROM t", p=1)
    assert rows == [[3.5, 1.0]]
    assert cols_oids(conn) == [type_oid("float8"), type_oid("float8")]

    # Infer types in comparison expressions
    conn.run("SELECT :p1, :p2 FROM t WHERE (i, f) = (:p1, :p2) ", p1=1, p2=1.5)
    assert cols_oids(conn) == [type_oid("int8"), type_oid("float8")]

    conn.run("SELECT :p1, :p2 FROM t WHERE (:p1, f) = (i, :p2)", p1=1, p2=1.5)
    assert cols_oids(conn) == [type_oid("int8"), type_oid("float8")]

    conn.run("SELECT :p1, :p2 FROM t WHERE :p1 = i AND (i, :p2) = (NULL, f)", p1=1, p2=1.5)
    assert cols_oids(conn) == [type_oid("int8"), type_oid("float8")]

    conn.run("SELECT :p1, :p2 FROM t WHERE :p1 = i AND f = :p2", p1=1, p2=1.5)
    assert cols_oids(conn) == [type_oid("int8"), type_oid("float8")]

    conn.run(
        "SELECT :p1, :p2 FROM t WHERE :p1 = u AND :p2 = d",
        p1="a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
        p2="Fri, 07 Jul 2023 12:34:56 +0200",
    )
    assert cols_oids(conn) == [type_oid("uuid"), type_oid("timestamptz")]

    # Infer types in a subuqery
    rows = conn.run("SELECT (SELECT :p1 + 1)::int, :p1", p1=1)
    assert rows == [[2, 1]]
    assert cols_oids(conn) == [type_oid("int8"), type_oid("int8")]

    # Infer types in cte
    rows = conn.run("WITH cte AS (SELECT :p1 * 1.5) SELECT :p1 FROM cte", p1=1)
    assert rows == [[Decimal(1)]]
    assert cols_oids(conn) == [type_oid("numeric")]

    # Roughly speaking, types are inferred from left to right,
    # so some expressions sets that can be resolved in one order, but not in another.

    # `$1 || ''` is on the left, `$1` is inferred to text,
    # allowing to resolve next expression `$1` to text.
    rows = conn.run("SELECT :p || '', :p", p="a")
    assert rows == [["a", "a"]]
    assert cols_oids(conn) == [type_oid("text"), type_oid("text")]

    # `$1` is on the left, cannot infer parameter type, type analysis fails with an error.
    with pytest.raises(DatabaseError, match=r"could not determine data type of parameter \$1"):
        conn.prepare("SELECT :p, :p || ''")


def test_params_inference_in_values(postgres: Postgres):
    user = "Парам Парамыч"
    password = "P@ssw0rd"

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    conn = pg.Connection(user, password=password, host=postgres.host, port=postgres.port)

    rows = conn.run("VALUES (:p1, :p2, '3'), (1, :p2, :p3), (:p1, 2.0, :p3)", p1=1, p2=2, p3=3)
    assert rows == [[1, Decimal(2.0), "3"]] * 3
    assert cols_oids(conn) == [type_oid("int8"), type_oid("numeric"), type_oid("text")]

    # Note: we use cte here because sbroad resolves `VALUES (1 + :p1 + 1.5)` to unknown type
    rows = conn.run("WITH t AS (VALUES (1 + :p1 + 1.5)) SELECT :p1", p1=1)
    assert rows == [[Decimal(1)]]
    assert cols_oids(conn) == [type_oid("numeric")]

    # Ensure inferred types are consistent.
    rows = conn.run("WITH t(a) AS (VALUES (:p + 1), (:p + 1.5)) SELECT $1 FROM t LIMIT 1", p=1)
    assert rows == [[Decimal(1.0)]]
    assert cols_oids(conn) == [type_oid("numeric")]

    # Ensure inferred types are consistent.
    rows = conn.run("WITH t(a) AS (VALUES (:p + 1.5), (:p + 1)) SELECT $1 FROM t LIMIT 1", p=1)
    assert rows == [[Decimal(1.0)]]
    assert cols_oids(conn) == [type_oid("numeric")]


def test_params_inference_in_insert(postgres: Postgres):
    user = "Парам Парамыч"
    password = "P@ssw0rd"

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    conn = pg.Connection(user, password=password, host=postgres.host, port=postgres.port)
    conn.run("CREATE TABLE t (i INT PRIMARY KEY, f DOUBLE, t TEXT);")

    # Infer parameter types from column types.
    conn.run("INSERT INTO t VALUES (:p1, :p2, :p3)", p1="1", p2="2", p3="3")
    rows = conn.run("SELECT * FROM t")
    assert rows == [[1, 2.0, "3"]]

    # Infer parameter types from explicit column types.
    conn.run("INSERT INTO t (t, f, i) VALUES (:p1, :p2, :p3)", p1="1", p2="2", p3="3")
    rows = conn.run("SELECT * FROM t WHERE t = :p", p="1")
    assert rows == [[3, 2.0, "1"]]

    # Parameterized VALUES with 2 rows.
    conn.run(
        "INSERT INTO t VALUES (:p1, :p2, :p3), (:p4, :p5, :p6)", p1="11", p2="22", p3="33", p4="12", p5="23", p6="34"
    )
    rows = conn.run("SELECT * FROM t WHERE i > 10")
    assert sorted(rows) == [[11, 22.0, "33"], [12, 23.0, "34"]]

    # Parameterized VALUES with 2 rows.
    conn.run(
        "INSERT INTO t (i, f, t) VALUES (:p1, :p2, :p3), (:p4, :p5, :p6)",
        p1="91",
        p2="92",
        p3="93",
        p4="92",
        p5="93",
        p6="94",
    )
    rows = conn.run("SELECT * FROM t WHERE i > 90")
    assert sorted(rows) == [[91, 92.0, "93"], [92, 93.0, "94"]]

    # Ensure inferred types are consistent.
    with pytest.raises(DatabaseError, match=r"inconsistent types int and text deduced for parameter \$3"):
        conn.run(
            "INSERT INTO t VALUES (:p1, :p2, :p3), (:p3, :p2, :p1)",
            p1="1",
            p2="2",
            p3="3",
        )

    # Complicate VALUES expressions.
    conn.run("INSERT INTO t VALUES (:p1 - 100, :p2 - 100, :p3 || :p3)", p1="1", p2="2.5", p3="3")
    rows = conn.run("SELECT * FROM t WHERE i < 0")
    assert rows == [[-99, -97.5, "33"]]

    # Infer unsuitable type.
    with pytest.raises(
        DatabaseError, match="INSERT column at position 1 is of type int, but expression is of type text"
    ):
        conn.run("INSERT INTO t (i) VALUES (:p1::text)", p1="1")


def test_params_inference_in_update_and_delete(postgres: Postgres):
    user = "Парам Парамыч"
    password = "P@ssw0rd"

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    conn = pg.Connection(user, password=password, host=postgres.host, port=postgres.port)
    conn.run("CREATE TABLE t (i INT PRIMARY KEY, f DOUBLE, t TEXT, u UUID, d datetime);")

    conn.run("INSERT INTO t (i) VALUES (0), (2)")

    # Cannot infer types from table columns in UPDATE
    # https://git.picodata.io/core/picodata/-/work_items/1643
    with pytest.raises(DatabaseError, match=r"could not determine data type of parameter \$1"):
        conn.run("UPDATE t SET f = :p2 WHERE i = :p1", p1=2, p2=1.5)

    # Infer parameter types from neighbor expressions.
    conn.run("UPDATE t SET f = :p2 + COALESCE(f, 0.0) WHERE i = :p1 OR i = 0", p1=2, p2=1.5)
    rows = conn.run("SELECT f FROM t")
    assert rows == [[1.5], [1.5]]

    conn.run("DELETE FROM t WHERE i = :p", p=0)
    rows = conn.run("SELECT i FROM t")
    assert rows == [[2]]

    conn.run("DELETE FROM t WHERE u = :p", p="a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
    rows = conn.run("SELECT i FROM t")
    assert rows == [[2]]


def test_params_inference_errors(postgres: Postgres):
    user = "Парам Парамыч"
    password = "P@ssw0rd"

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    conn = pg.Connection(user, password=password, host=postgres.host, port=postgres.port)
    conn.run("CREATE TABLE t (i INT PRIMARY KEY, f DOUBLE, t TEXT, u UUID, d datetime);")

    # Cannot be resolved without default parameter types.
    # (numeric vs text vs bool vs something else?)
    with pytest.raises(DatabaseError, match=r"could not resolve function overload for max\(unknown\)"):
        conn.run("SELECT max(:p1) FROM t", p1=1)

    # Cannot be resolved without default parameter types.
    with pytest.raises(DatabaseError, match=r"could not determine data type of parameter \$2"):
        conn.run("SELECT :p1 = :p2", p1="1", p2="2")

    # Cannot be resolved without default parameter types.
    with pytest.raises(DatabaseError, match=r"could not determine data type of parameter \$1"):
        conn.run("SELECT :p = NULL", p=1)

    with pytest.raises(DatabaseError, match=r"inconsistent types int and double deduced for parameter \$1"):
        conn.run("SELECT :p::int + :p::double", p=1)

    with pytest.raises(DatabaseError, match=r"inconsistent types unsigned and text deduced for parameter \$1"):
        conn.run("SELECT * FROM (SELECT 1) WHERE :p = 1 AND :p = '1.5'", p=1)

    with pytest.raises(DatabaseError, match=r"row value misused"):
        conn.run("SELECT (:p,:p)", p=1)

    with pytest.raises(DatabaseError, match=r"row value misused"):
        conn.run("SELECT max((:p,:p))", p=1)

    with pytest.raises(DatabaseError, match=r"row value misused"):
        conn.run("SELECT substring((:p,:p), 1)", p=1)


def test_params_inference_in_complex_queries(postgres: Postgres):
    user = "Парам Парамыч"
    password = "P@ssw0rd"

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    conn = pg.Connection(user, password=password, host=postgres.host, port=postgres.port)
    conn.run("CREATE TABLE t (i INT PRIMARY KEY, f DOUBLE, t TEXT, u UUID, d datetime);")
    conn.run("INSERT INTO t(i, f, t) VALUES (1, 1.5, 't')")

    # Infer type in the left UNION query and use it in the right query
    rows = conn.run("WITH t(a) AS (SELECT :p + 1 UNION SELECT :p) SELECT $1", p=1)
    assert rows == [[1]]
    assert cols_oids(conn) == [type_oid("int8")]

    # Infer parameter type in accordance with COALESCE type.
    rows = conn.run("WITH t(a) AS (SELECT COALESCE(:p + 1, 1 + 1.5)) SELECT $1", p=1)
    assert rows == [[Decimal(1)]]
    assert cols_oids(conn) == [type_oid("numeric")]

    # Complicate the previous test.
    rows = conn.run("WITH t AS (SELECT COALESCE(:p + 1, (COALESCE(1, 1 + 1.5)) + 1, :p)) SELECT $1", p=1)
    assert rows == [[Decimal(1)]]
    assert cols_oids(conn) == [type_oid("numeric")]

    # Resolve ambiguous function overload with neighbor.
    rows = conn.run("WITH t(a) AS (SELECT max(:p) + 1) SELECT $1", p=1)
    assert rows == [[1]]
    assert cols_oids(conn) == [type_oid("int8")]

    # Complicate the previous test by making neighbor dependent on parameter type.
    rows = conn.run("WITH t(a) AS (SELECT max(:p) + (1.5 + :p)) SELECT $1", p=1)
    assert rows == [[Decimal(1)]]
    assert cols_oids(conn) == [type_oid("numeric")]

    # TODO: remove unsigned type to fix that
    with pytest.raises(DatabaseError, match=r"inconsistent types int and unsigned deduced for parameter \$1"):
        conn.run("WITH t(a) AS (SELECT max(:p) + (1 + :p)) SELECT :p", p=1)

    # TODO: remove unsigned type to fix that
    with pytest.raises(DatabaseError, match=r"inconsistent types int and unsigned deduced for parameter \$1"):
        conn.run("WITH t(a) AS (SELECT max(:p) + (1 + :p)) SELECT :p", p=1)

    # Ensure types are consistent.
    rows = conn.run("SELECT i + :p, :p FROM t WHERE i + :p <> f + :p", p=1)
    assert rows == [[2.0, 1.0]]
    assert cols_oids(conn) == [type_oid("float8"), type_oid("float8")]


def test_params_inference_with_client_provided_types(postgres: Postgres):
    user = "Парам Парамыч"
    password = "P@ssw0rd"

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    conn = pg.Connection(user, password=password, host=postgres.host, port=postgres.port)
    conn.run("CREATE TABLE t (i INT PRIMARY KEY, f DOUBLE, t TEXT);")

    # Ensure provided type influence typing.
    rows = conn.run("SELECT 1 + :p, :p", p=1, types={"p": type_oid("numeric")})
    assert rows == [[Decimal(2.0), Decimal(1.0)]]
    assert cols_oids(conn) == [type_oid("numeric"), type_oid("numeric")]

    # Ensure provided type influence typing.
    rows = conn.run("SELECT 1.0 + :p, :p", p=1, types={"p": type_oid("int8")})
    assert rows == [[Decimal(2.0), Decimal(1.0)]]
    assert cols_oids(conn) == [type_oid("numeric"), type_oid("int8")]

    # Ambiguity can be resolved by client.
    rows = conn.run("SELECT :p::int + :p::double, :p", p=1, types={"p": type_oid("numeric")})
    assert rows == [[2.0, Decimal(1.0)]]
    assert cols_oids(conn) == [type_oid("float8"), type_oid("numeric")]

    # Infer 2nd parameter from 1st. (0 oid means that parameter type is unspecified)
    rows = conn.run("SELECT :p1 + :p2, :p1, :p2", p1=1, p2=1, types={"p1": type_oid("float8"), "p2": 0})
    assert rows == [[2.0, 1.0, 1.0]]
    assert cols_oids(conn) == [type_oid("float8"), type_oid("float8"), type_oid("float8")]

    # Infer 1st parameter from 2st. (0 oid means that parameter type is unspecified)
    rows = conn.run("SELECT :p1 + :p2, :p1, :p2", p1=1, p2=1, types={"p1": 0, "p2": type_oid("float8")})
    assert rows == [[2.0, 1.0, 1.0]]
    assert cols_oids(conn) == [type_oid("float8"), type_oid("float8"), type_oid("float8")]

    # Specify unsuitable type and catch an error.
    with pytest.raises(DatabaseError, match=r"could not resolve operator overload for ||(uuid, text)"):
        rows = conn.run("SELECT :p || 'text'", p=1, types={"p": type_oid("uuid")})


def test_caching_depends_on_parameter_types(postgres: Postgres):
    host = postgres.host
    port = postgres.port
    user, password = ("Кэш Кэшич", "P@ssw0rd")

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    conn = psycopg.connect(f"user='{user}' password={password} host={host} port={port}")

    # Cache query, parameters are [Unsigned]
    cur = conn.execute("WITH t AS (VALUES (1 + %(p)s)) SELECT %(p)s", {"p": 1})
    assert cur.fetchall() == [(1,)]

    # Read query from cache
    cur = conn.execute("WITH t AS (VALUES (1 + %(p)s)) SELECT %(p)s", {"p": 1})
    assert cur.fetchall() == [(1,)]

    # Change parameter type and ensure query is not read from the cache by catching a typing error,
    # which cannot happen for cached queries, because type analysis is not performed for them.
    with pytest.raises(psycopg.InternalError, match=r"could not resolve operator overload for \+\(unsigned, uuid\)"):
        uuid = UUID("6f2ba4c4-0a4c-4d79-86ae-43d4f84b70e1")
        conn.execute("WITH t AS (VALUES (1 + %(p)s)) SELECT %(p)s", {"p": uuid})
