import datetime
from decimal import Decimal
from uuid import UUID

import pg8000.native as pg8000  # type: ignore
import psycopg
import pytest
from conftest import PgClient, Postgres
from psycopg.types.json import Json, Jsonb


def test_decimal(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    # create a postgres user using a postgres compatible password
    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    # allow user to create tables
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    # connect to the server and enable autocommit as we
    # don't support interactive transactions
    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    conn.execute(
        """
        CREATE TABLE T (
            W_ID DECIMAL NOT NULL,
            PRIMARY KEY (W_ID)
        )
        USING MEMTX DISTRIBUTED BY (W_ID);
        """
    )

    pi = Decimal("3.1415926535897932384626433832")
    e = Decimal("2.7182818284590452353602874713")
    nines = Decimal("999999999999999999999999999")
    ones = nines / 9

    # test text decoding
    conn.execute(
        """
        INSERT INTO T VALUES (%t), (%t)
        """,
        (pi, nines),
    )

    # test binary decoding
    conn.execute(
        """
        INSERT INTO T VALUES (%b), (%b)
        """,
        (e, ones),
    )

    # test text encoding
    cur = conn.execute(
        """
        SELECT * FROM T
        """,
        binary=False,
    )
    assert sorted(cur.fetchall()) == [
        (e,),
        (pi,),
        (ones,),
        (nines,),
    ]

    # test binary encoding
    cur = conn.execute(
        """
        SELECT * FROM T
        """,
        binary=True,
    )
    assert sorted(cur.fetchall()) == [
        (e,),
        (pi,),
        (ones,),
        (nines,),
    ]


def test_double(postgres: Postgres):
    user = "postgres"
    password = "Passw0rd"
    host = postgres.host
    port = postgres.port

    # create a postgres user using a postgres compatible password
    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    # allow user to create tables
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    # connect to the server and enable autocommit as we
    # don't support interactive transactions
    conn = psycopg.connect(f"postgres://{user}:{password}@{host}:{port}?sslmode=disable")
    conn.autocommit = True

    conn.execute(
        """
        CREATE TABLE T (
            N DOUBLE NOT NULL,
            PRIMARY KEY (N)
        )
        USING MEMTX DISTRIBUTED BY (N);
        """
    )

    n1 = 1
    n2 = 1.5

    # test text decoding
    conn.execute(""" INSERT INTO T VALUES(%t); """, (n1,))

    # test binary decoding
    conn.execute(""" INSERT INTO T VALUES(%b); """, (n2,))

    # test text encoding
    cur = conn.execute(""" SELECT * FROM T; """, binary=False)
    assert sorted(cur.fetchall()) == [(n1,), (n2,)]

    # test binary encoding
    cur = conn.execute(""" SELECT * FROM T; """, binary=True)
    assert sorted(cur.fetchall()) == [(n1,), (n2,)]


def test_uuid(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    # create a postgres user using a postgres compatible password
    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    # allow user to create tables
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    # connect to the server and enable autocommit as we
    # don't support interactive transactions
    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    conn.execute(
        """
        CREATE TABLE T (
            ID UUID NOT NULL,
            PRIMARY KEY (ID)
        )
        USING MEMTX DISTRIBUTED BY (ID);
        """
    )

    id1 = UUID("6f2ba4c4-0a4c-4d79-86ae-43d4f84b70e1")
    id2 = UUID("e4166fc5-e113-46c5-8ae9-970882ca8842")

    # test text decoding
    conn.execute(""" INSERT INTO T VALUES(%t); """, (id1,))

    # test binary decoding
    conn.execute(""" INSERT INTO T VALUES(%b); """, (id2,))

    # test text encoding
    cur = conn.execute(""" SELECT * FROM T; """, binary=False)
    assert sorted(cur.fetchall()) == [(id1,), (id2,)]

    # test binary encoding
    cur = conn.execute(""" SELECT * FROM T; """, binary=True)
    assert sorted(cur.fetchall()) == [(id1,), (id2,)]


def test_text_and_varchar(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    conn = pg8000.Connection(user, password=password, host=postgres.host, port=postgres.port)

    conn.run(
        """
        CREATE TABLE T (
            S TEXT NOT NULL,
            PRIMARY KEY (S)
        )
        USING MEMTX DISTRIBUTED BY (S);
        """
    )

    # encode string parameter as varchar
    varchar_oid = 1043
    conn.run("INSERT INTO T VALUES (:p);", p="value1", types={"p": varchar_oid})

    # encode string parameter as text
    text_oid = 25
    conn.run("INSERT INTO T VALUES (:p);", p="value2", types={"p": text_oid})

    # verify that the values were insert
    rows = conn.run("SELECT * FROM T;")
    assert sorted(rows) == [["value1"], ["value2"]]


def test_unsigned(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    # create a postgres user using a postgres compatible password
    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    # allow user to create tables
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    # connect to the server and enable autocommit as we
    # don't support interactive transactions
    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    conn.execute(
        """
        CREATE TABLE T (
            ID UNSIGNED NOT NULL,
            PRIMARY KEY (ID)
        )
        USING MEMTX DISTRIBUTED BY (ID);
        """
    )

    u = 1
    u64_above_max = 9223372036854775808

    conn.execute(""" INSERT INTO T VALUES(%t); """, (u,))

    # test text encoding
    cur = conn.execute(""" SELECT * FROM T; """, binary=False)
    assert sorted(cur.fetchall()) == [(u,)]

    cur = conn.execute(""" SELECT 1 FROM T; """, binary=False)
    assert sorted(cur.fetchall()) == [(1,)]

    # test binary encoding
    cur = conn.execute(""" SELECT * FROM T; """, binary=True)
    assert sorted(cur.fetchall()) == [(u,)]

    cur = conn.execute(""" SELECT 1 FROM T; """, binary=True)
    assert sorted(cur.fetchall()) == [(1,)]

    # Note: u64::MAX can be sent, despite the fact, that PostgreSQL supports only
    # signed integers, because psycopg sends it as numeric. Also it explicitly sets
    # parameter type, so cast is needed to make type system happy. As a matter of fact
    # it will fail, because sql can't handle values that are bigger than i64::MAX anymore.
    with pytest.raises(
        psycopg.InternalError,
        match="failed decoding i64: out of range integral type conversion attempted",
    ):
        conn.execute(""" INSERT INTO T select %t::int; """, (u64_above_max,))


def test_arrays(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql("CREATE TABLE t1 (a INT PRIMARY KEY, b INT ARRAY, c DOUBLE[], d TEXT[][], e BOOL[5])")

    postgres.instance.sql(f'GRANT READ ON TABLE "_pico_user" TO "{user}"', sudo=True)
    postgres.instance.sql(f'GRANT READ ON TABLE "t1" TO "{user}"', sudo=True)
    postgres.instance.sql(f'GRANT WRITE ON TABLE "t1" TO "{user}"', sudo=True)

    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    # test text encoding
    cur = conn.execute(""" SELECT \"auth\" FROM \"_pico_user\" WHERE \"id\" = 0; """, binary=False)
    assert cur.fetchall() == [(["md5", "md5084e0343a0486ff05530df6c705c8bb4"],)]

    # test binary encoding
    cur = conn.execute(""" SELECT \"auth\" FROM \"_pico_user\" WHERE \"id\" = 0; """, binary=True)
    assert cur.fetchall() == [(["md5", "md5084e0343a0486ff05530df6c705c8bb4"],)]

    # test empty strings representation in arrays
    # * in text repr empty strings in arrays are sent as quotes ""
    # * in bin repr empty strings in array are sent as any other string

    # text repr case
    cur = conn.execute(
        """ SELECT \"auth\" FROM \"_pico_user\" WHERE \"name\" = 'admin'; """,
        binary=False,
    )
    assert cur.fetchall() == [(["md5", ""],)]

    # bin repr case
    cur = conn.execute(
        """ SELECT \"auth\" FROM \"_pico_user\" WHERE \"name\" = 'admin'; """,
        binary=True,
    )
    assert cur.fetchall() == [(["md5", ""],)]

    conn.execute(
        """ INSERT INTO t1 VALUES
            (1, NULL, NULL, NULL, NULL),
            (2, ARRAY[], ARRAY[], ARRAY[], ARRAY[]),
            (3, ARRAY[1, 2, 3], ARRAY[1::double, 2, 3], ARRAY['amogus'], ARRAY[false]),
            (4, ARRAY[NULL], ARRAY[NULL], ARRAY[NULL], ARRAY[NULL]),
            (5, ARRAY[1, NULL, '2'], ARRAY[1, NULL, '2'], ARRAY['1', '', NULL], ARRAY[true, NULL, true]);
        """,
    )
    exp = [
        (1, None, None, None, None),
        (2, [], [], [], []),
        (3, [1, 2, 3], [1.0, 2.0, 3.0], ["amogus"], [False]),
        (4, [None], [None], [None], [None]),
        (5, [1, None, 2], [1.0, None, 2.0], ["1", "", None], [True, None, True]),
    ]

    cur = conn.execute(""" SELECT * FROM t1; """, binary=False)
    assert cur.fetchall() == exp

    cur = conn.execute(""" SELECT * FROM t1; """, binary=True)
    assert cur.fetchall() == exp

    cur = conn.execute(""" SELECT ARRAY[1.4, 2.5, -1.5]::int[]; """, binary=False)
    assert cur.fetchall() == [([1, 2, -1],)]

    cur = conn.execute(""" SELECT ARRAY[1.4, 2.5, -1.5]::int[]; """, binary=True)
    assert cur.fetchall() == [([1, 2, -1],)]

    cur = conn.execute(""" SELECT %t::int[][1]; """, ([1.4, 2.5, -1.5],), binary=False)
    assert cur.fetchall() == [(1,)]

    cur = conn.execute(""" SELECT %b::int[][1]; """, ([1.4, 2.5, -1.5],), binary=True)
    assert cur.fetchall() == [(1,)]

    cur = conn.execute(""" SELECT \"a\", \"b\"::int[] FROM t1 WHERE \"a\" = 3; """, binary=False)
    assert cur.fetchall() == [(3, [1, 2, 3])]

    cur = conn.execute(""" SELECT \"a\", \"b\"::int[] FROM t1 WHERE \"a\" = 3; """, binary=True)
    assert cur.fetchall() == [(3, [1, 2, 3])]


def _array_table(postgres: Postgres, columns: str):
    """Create table `t` with the given column definition (as admin), grant the
    fresh `postgres` user access to it and return an autocommit connection."""
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    postgres.instance.sql(f"CREATE TABLE t ({columns})")
    postgres.instance.sql(f'GRANT READ ON TABLE "t" TO "{user}"', sudo=True)
    postgres.instance.sql(f'GRANT WRITE ON TABLE "t" TO "{user}"', sudo=True)

    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True
    return conn


def _assert_array_roundtrip(conn, exp):
    # text encoding
    cur = conn.execute(""" SELECT * FROM t ORDER BY a; """, binary=False)
    assert cur.fetchall() == exp

    # binary encoding
    cur = conn.execute(""" SELECT * FROM t ORDER BY a; """, binary=True)
    assert cur.fetchall() == exp


def test_array_int(postgres: Postgres):
    conn = _array_table(postgres, "a INT PRIMARY KEY, b INT ARRAY")
    conn.execute(
        """ INSERT INTO t VALUES
            (1, NULL),
            (2, ARRAY[]),
            (3, ARRAY[1, 2, 3]),
            (4, ARRAY[NULL]),
            (5, ARRAY[-100, 0, 9223372036854775807]),
            (6, ARRAY[10, NULL, 30]);
        """
    )
    exp = [
        (1, None),
        (2, []),
        (3, [1, 2, 3]),
        (4, [None]),
        (5, [-100, 0, 9223372036854775807]),
        (6, [10, None, 30]),
    ]
    _assert_array_roundtrip(conn, exp)


def test_array_double(postgres: Postgres):
    conn = _array_table(postgres, "a INT PRIMARY KEY, b DOUBLE ARRAY")
    conn.execute(
        """ INSERT INTO t VALUES
            (1, NULL),
            (2, ARRAY[]),
            (3, ARRAY[1, 2.5, 3]),
            (4, ARRAY[NULL]),
            (5, ARRAY[-1.5, 0, 100.25]),
            (6, ARRAY[2.5, NULL, 3.5]);
        """
    )
    exp = [
        (1, None),
        (2, []),
        (3, [1.0, 2.5, 3.0]),
        (4, [None]),
        (5, [-1.5, 0.0, 100.25]),
        (6, [2.5, None, 3.5]),
    ]
    _assert_array_roundtrip(conn, exp)


def test_array_decimal(postgres: Postgres):
    conn = _array_table(postgres, "a INT PRIMARY KEY, b DECIMAL ARRAY")
    conn.execute(
        """ INSERT INTO t VALUES
            (1, NULL),
            (2, ARRAY[]),
            (3, ARRAY[1, 2, 3]),
            (4, ARRAY[NULL]),
            (5, ARRAY[1.5, 2.25, -3.125]),
            (6, ARRAY[10, NULL, 20]);
        """
    )
    exp = [
        (1, None),
        (2, []),
        (3, [Decimal("1"), Decimal("2"), Decimal("3")]),
        (4, [None]),
        (5, [Decimal("1.5"), Decimal("2.25"), Decimal("-3.125")]),
        (6, [Decimal("10"), None, Decimal("20")]),
    ]
    _assert_array_roundtrip(conn, exp)


def test_array_text(postgres: Postgres):
    conn = _array_table(postgres, "a INT PRIMARY KEY, b TEXT ARRAY")
    conn.execute(
        r""" INSERT INTO t VALUES
            (1, NULL),
            (2, ARRAY[]),
            (3, ARRAY['hello', 'world']),
            (4, ARRAY[NULL]),
            (5, ARRAY['', ' sp ', 'a,b', 'q"q', 'back\slash', '{braces}', 'it''s']),
            (6, ARRAY['NULL', 'null', 'plain', NULL]);
        """
    )
    exp = [
        (1, None),
        (2, []),
        (3, ["hello", "world"]),
        (4, [None]),
        (5, ["", " sp ", "a,b", 'q"q', "back\\slash", "{braces}", "it's"]),
        (6, ["NULL", "null", "plain", None]),
    ]
    _assert_array_roundtrip(conn, exp)


def test_array_bool(postgres: Postgres):
    conn = _array_table(postgres, "a INT PRIMARY KEY, b BOOL ARRAY")
    conn.execute(
        """ INSERT INTO t VALUES
            (1, NULL),
            (2, ARRAY[]),
            (3, ARRAY[true, false, true]),
            (4, ARRAY[NULL]),
            (5, ARRAY[false, NULL, true]);
        """
    )
    exp = [
        (1, None),
        (2, []),
        (3, [True, False, True]),
        (4, [None]),
        (5, [False, None, True]),
    ]
    _assert_array_roundtrip(conn, exp)


def test_array_datetime(postgres: Postgres):
    conn = _array_table(postgres, "a INT PRIMARY KEY, b DATETIME ARRAY")
    conn.execute(
        """ INSERT INTO t VALUES
            (1, NULL),
            (2, ARRAY[]),
            (3, ARRAY['2025-05-27T00:00:00Z', '1999-12-31T23:59:59Z']),
            (4, ARRAY[NULL]),
            (5, ARRAY['2025-05-27T00:00:00Z', NULL]);
        """
    )
    d1 = datetime.datetime(2025, 5, 27, 0, 0, 0, tzinfo=datetime.timezone.utc)
    d2 = datetime.datetime(1999, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)
    exp = [
        (1, None),
        (2, []),
        (3, [d1, d2]),
        (4, [None]),
        (5, [d1, None]),
    ]
    _assert_array_roundtrip(conn, exp)


def test_array_uuid(postgres: Postgres):
    conn = _array_table(postgres, "a INT PRIMARY KEY, b UUID ARRAY")
    conn.execute(
        """ INSERT INTO t VALUES
            (1, NULL),
            (2, ARRAY[]),
            (3, ARRAY['6f2ba4c4-0a4c-4d79-86ae-43d4f84b70e1',
                      'e4166fc5-e113-46c5-8ae9-970882ca8842']),
            (4, ARRAY[NULL]),
            (5, ARRAY['11111111-1111-1111-1111-111111111111', NULL]);
        """
    )
    u1 = UUID("6f2ba4c4-0a4c-4d79-86ae-43d4f84b70e1")
    u2 = UUID("e4166fc5-e113-46c5-8ae9-970882ca8842")
    u3 = UUID("11111111-1111-1111-1111-111111111111")
    exp = [
        (1, None),
        (2, []),
        (3, [u1, u2]),
        (4, [None]),
        (5, [u3, None]),
    ]
    _assert_array_roundtrip(conn, exp)


def _insert_array_params(conn, rows, *, binary: bool):
    """Insert ``(a, array)`` rows, binding each array as a query parameter."""
    fmt = "%b" if binary else "%t"
    for a, arr in rows:
        conn.execute(f"INSERT INTO t VALUES ({a}, {fmt})", (arr,))


def test_array_param_int(postgres: Postgres):
    conn = _array_table(postgres, "a INT PRIMARY KEY, b INT ARRAY")
    rows = [
        (1, [1, 2, 3]),
        (2, [-100, 0, 9223372036854775807]),
        (3, [10, None, 30]),
    ]
    _insert_array_params(conn, rows, binary=False)
    _insert_array_params(conn, [(a + 3, arr) for a, arr in rows], binary=True)
    exp = rows + [(a + 3, arr) for a, arr in rows]
    _assert_array_roundtrip(conn, exp)


def test_array_param_double(postgres: Postgres):
    conn = _array_table(postgres, "a INT PRIMARY KEY, b DOUBLE ARRAY")
    rows = [
        (1, [1.0, 2.5, 3.0]),
        (2, [-1.5, 0.0, 100.25]),
        (3, [2.5, None, 3.5]),
    ]
    _insert_array_params(conn, rows, binary=False)
    _insert_array_params(conn, [(a + 3, arr) for a, arr in rows], binary=True)
    exp = rows + [(a + 3, arr) for a, arr in rows]
    _assert_array_roundtrip(conn, exp)


def test_array_param_decimal(postgres: Postgres):
    conn = _array_table(postgres, "a INT PRIMARY KEY, b DECIMAL ARRAY")
    rows = [
        (1, [Decimal("1"), Decimal("2"), Decimal("3")]),
        (2, [Decimal("1.5"), Decimal("2.25"), Decimal("-3.125")]),
        (3, [Decimal("10"), None, Decimal("20")]),
    ]
    _insert_array_params(conn, rows, binary=False)
    _insert_array_params(conn, [(a + 3, arr) for a, arr in rows], binary=True)
    exp = rows + [(a + 3, arr) for a, arr in rows]
    _assert_array_roundtrip(conn, exp)


def test_array_param_text(postgres: Postgres):
    conn = _array_table(postgres, "a INT PRIMARY KEY, b TEXT ARRAY")
    rows = [
        (1, ["hello", "world"]),
        (2, ["", " sp ", "a,b", 'q"q', "back\\slash", "{braces}", "it's"]),
        (3, ["NULL", "null", "plain", None]),
    ]
    _insert_array_params(conn, rows, binary=False)
    _insert_array_params(conn, [(a + 3, arr) for a, arr in rows], binary=True)
    # pgwire behaviour
    text_rows = [
        (1, ["hello", "world"]),
        (2, [None, " sp ", "a,b", 'q"q', "back\\slash", "{braces}", "it's"]),
        (3, [None, None, "plain", None]),
    ]
    binary_rows = [(a + 3, arr) for a, arr in rows]
    _assert_array_roundtrip(conn, text_rows + binary_rows)


def test_array_param_bool(postgres: Postgres):
    conn = _array_table(postgres, "a INT PRIMARY KEY, b BOOL ARRAY")
    rows = [
        (1, [True, False, True]),
        (2, [False, None, True]),
    ]
    _insert_array_params(conn, rows, binary=False)
    _insert_array_params(conn, [(a + 2, arr) for a, arr in rows], binary=True)
    exp = rows + [(a + 2, arr) for a, arr in rows]
    _assert_array_roundtrip(conn, exp)


def test_array_param_datetime(postgres: Postgres):
    conn = _array_table(postgres, "a INT PRIMARY KEY, b DATETIME ARRAY")
    d1 = datetime.datetime(2025, 5, 27, 0, 0, 0, tzinfo=datetime.timezone.utc)
    d2 = datetime.datetime(1999, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)
    rows = [
        (1, [d1, d2]),
        (2, [d1, None]),
    ]
    _insert_array_params(conn, rows, binary=False)
    _insert_array_params(conn, [(a + 2, arr) for a, arr in rows], binary=True)
    exp = rows + [(a + 2, arr) for a, arr in rows]
    _assert_array_roundtrip(conn, exp)


def test_array_param_uuid(postgres: Postgres):
    conn = _array_table(postgres, "a INT PRIMARY KEY, b UUID ARRAY")
    u1 = UUID("6f2ba4c4-0a4c-4d79-86ae-43d4f84b70e1")
    u2 = UUID("e4166fc5-e113-46c5-8ae9-970882ca8842")
    u3 = UUID("11111111-1111-1111-1111-111111111111")
    rows = [
        (1, [u1, u2]),
        (2, [u3, None]),
    ]
    _insert_array_params(conn, rows, binary=False)
    _insert_array_params(conn, [(a + 2, arr) for a, arr in rows], binary=True)
    exp = rows + [(a + 2, arr) for a, arr in rows]
    _assert_array_roundtrip(conn, exp)


def test_map(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")

    postgres.instance.sql(f'GRANT READ ON TABLE "_pico_table" TO "{user}"', sudo=True)

    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    data = postgres.instance.sql(""" SELECT "distribution" FROM "_pico_table" """)
    distribution = [tuple(row) for row in data]

    # test text encoding
    cur = conn.execute(""" SELECT "distribution" FROM "_pico_table" """, binary=False)
    assert cur.fetchall() == distribution

    # test binary encoding
    cur = conn.execute(""" SELECT "distribution" FROM "_pico_table" """, binary=True)
    assert cur.fetchall() == distribution

    # text json parameters should throw an error
    with pytest.raises(
        psycopg.errors.FeatureNotSupported,
        match="feature is not supported: json parameters",
    ):
        cur = conn.execute(
            """ SELECT \"distribution\" FROM \"_pico_table\" WHERE \"distribution\" = %t; """,
            (Json(distribution[0]),),
        )

    # text jsonb parameters should throw an error
    with pytest.raises(
        psycopg.errors.FeatureNotSupported,
        match="feature is not supported: jsonb parameters",
    ):
        cur = conn.execute(
            """ SELECT \"distribution\" FROM \"_pico_table\" WHERE \"distribution\" = %t; """,
            (Jsonb(distribution[0]),),
        )

    # binary json parameters should throw an error
    with pytest.raises(
        psycopg.errors.FeatureNotSupported,
        match="feature is not supported: json parameters",
    ):
        cur = conn.execute(
            """ SELECT \"distribution\" FROM \"_pico_table\" WHERE \"distribution\" = %b; """,
            (Json(distribution[0]),),
        )

    # binary jsonb parameters should throw an error
    with pytest.raises(
        psycopg.errors.FeatureNotSupported,
        match="feature is not supported: jsonb parameters",
    ):
        cur = conn.execute(
            """ SELECT \"distribution\" FROM \"_pico_table\" WHERE \"distribution\" = %b; """,
            (Jsonb(distribution[0]),),
        )


def test_datetime(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    # create a postgres user using a postgres compatible password
    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    # allow user to create tables
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    # connect to the server and enable autocommit as we
    # don't support interactive transactions
    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    conn.execute(
        """
        CREATE TABLE T (
            ID DATETIME NOT NULL,
            PRIMARY KEY (ID)
        )
        USING MEMTX DISTRIBUTED BY (ID);
        """
    )

    d1 = datetime.datetime(2042, 7, 1, tzinfo=datetime.timezone.utc)
    d2 = datetime.datetime(2044, 7, 1, tzinfo=datetime.timezone.utc)

    # test text decoding
    conn.execute(""" INSERT INTO T VALUES(%t); """, (d1,))

    # test binary decoding
    conn.execute(""" INSERT INTO T VALUES(%b); """, (d2,))

    # test text encoding
    cur = conn.execute(""" SELECT * FROM T; """, binary=False)
    assert sorted(cur.fetchall()) == [(d1,), (d2,)]

    # test binary encoding
    cur = conn.execute(""" SELECT * FROM T; """, binary=True)
    assert sorted(cur.fetchall()) == [(d1,), (d2,)]


# Verify that we can read from all system tables without errors.
def test_select_from_system_tables(postgres: Postgres):
    user = "postgres"
    password = "Passw0rd"
    host = postgres.host
    port = postgres.port

    # create a postgres user using a postgres compatible password
    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    # grant read on tables
    postgres.instance.sql(f'GRANT READ TABLE TO "{user}"', sudo=True)

    # connect to the server and enable autocommit as we
    # don't support interactive transactions
    conn = psycopg.connect(f"postgres://{user}:{password}@{host}:{port}?sslmode=disable")
    conn.autocommit = True

    # at the beginning there should be only '_pico' tables
    cursor = conn.execute(""" SELECT "name" from "_pico_table" """)
    tables = [row[0] for row in cursor.fetchall()]

    # read from all tables
    for table in tables:
        sql = f""" SELECT * from "{table}" """
        cur = conn.execute(bytes(sql, "utf-8"))
        cur.fetchall()


def test_any(postgres: Postgres):
    user = "admin"
    password = "Passw0rd"

    postgres.instance.sql(f"ALTER USER {user} WITH PASSWORD '{password}'", sudo=True)
    conn = pg8000.Connection(user, password=password, host=postgres.host, port=postgres.port)
    conn.autocommit = True

    # ensure standalone any is allowed
    conn.prepare("SELECT key, value FROM _pico_db_config WHERE key = 'sql_vdbe_opcode_max';")

    # ensure standalone any is allowed
    conn.prepare("SELECT key, value FROM _pico_db_config WHERE value is not null")

    # test that type system prohibits to sum any with other types
    with pytest.raises(pg8000.DatabaseError, match=r"could not resolve operator overload for \+\(any, int\)"):
        conn.run("SELECT key, value + 2 FROM _pico_db_config WHERE key = 'sql_vdbe_opcode_max';")

    # comparison for any is not allowed
    with pytest.raises(
        pg8000.DatabaseError, match=r"unexpected expression of type any\, explicit cast to the actual type is required"
    ):
        conn.prepare("SELECT key FROM _pico_db_config WHERE value = value")

    # fix comparison with explicit casts
    conn.prepare("SELECT key FROM _pico_db_config WHERE value::text = value::text")

    # ensure type system allows to assign an expression to a column of type any
    conn.prepare("UPDATE _pico_db_config set value = 1 WHERE key = 'auth_login_attempt_max'")

    # ensure type system allows to assign a parameter to a column of type any
    conn.prepare("UPDATE _pico_db_config set value = :p WHERE key = 'auth_login_attempt_max'")

    # ensure type system allows to assign a casted parameter to a column of type any
    conn.prepare("UPDATE _pico_db_config set value = :p::int WHERE key = 'auth_login_attempt_max'")

    # fix typing error with cast
    conn.run("SELECT value::int + 2 FROM _pico_db_config WHERE key = 'sql_vdbe_opcode_max';")

    # ensure parameters of type any are not allowed
    with pytest.raises(
        pg8000.DatabaseError, match=r"unexpected expression of type any\, explicit cast to the actual type is required"
    ):
        conn.run("SELECT value = :p FROM _pico_db_config WHERE key = 'sql_vdbe_opcode_max';", p=1)

    # ensure parameters of type any are not allowed
    with pytest.raises(
        pg8000.DatabaseError, match=r"unexpected expression of type any\, explicit cast to the actual type is required"
    ):
        conn.prepare("SELECT key FROM _pico_db_config WHERE value = :p;")


def test_gl_1125_f64_cannot_be_represented_as_int8(postgres: Postgres):
    """https://git.picodata.io/core/picodata/-/issues/1125"""
    user = "admin"
    password = "Passw0rd"

    postgres.instance.sql(f"ALTER USER {user} WITH PASSWORD '{password}'", sudo=True)

    conn = pg8000.Connection(user, password=password, host=postgres.host, port=postgres.port)

    conn.run("create table clients (id integer not null, name string not null, primary key (id));")

    nrows = 100
    for i in range(nrows):
        conn.run(f"insert into clients values ({i}, 'Character#{i}');")

    rows = conn.run("select id from clients;")
    assert sorted(rows) == [[x] for x in range(nrows)]


def test_gl_1730_via_bindings_varchar(pg_client: PgClient):
    # How to check: `select 'varchar'::regtype::oid;`
    varchar_oid = 1043
    unknown_oid = 705
    text_oid = 25

    id = "100"
    pg_client.parse(id, "select $1", [varchar_oid])
    pg_client.bind(id, id, ["hello"], [])
    desc = pg_client.describe_stmt(id)
    assert desc["param_oids"] == [varchar_oid]
    res = pg_client.execute(id)["rows"][0]
    assert res == ["hello"]

    id = "200"
    pg_client.parse(id, "select $1", [text_oid])
    pg_client.bind(id, id, ["hello"], [])
    desc = pg_client.describe_stmt(id)
    assert desc["param_oids"] == [text_oid]
    res = pg_client.execute(id)["rows"][0]
    assert res == ["hello"]

    id = "300"
    pg_client.parse(id, "select $1, $2", [text_oid, varchar_oid])
    pg_client.bind(id, id, ["hello", "world"], [])
    desc = pg_client.describe_stmt(id)
    assert desc["param_oids"] == [text_oid, varchar_oid]
    res = pg_client.execute(id)["rows"][0]
    assert res == ["hello", "world"]

    id = "400"
    pg_client.parse(id, "select $1, $2", [0, varchar_oid])
    pg_client.bind(id, id, ["hello", "world"], [])
    desc = pg_client.describe_stmt(id)
    assert desc["param_oids"] == [text_oid, varchar_oid]
    res = pg_client.execute(id)["rows"][0]
    assert res == ["hello", "world"]

    id = "500"
    pg_client.parse(id, "select $1, $2", [varchar_oid, unknown_oid])
    pg_client.bind(id, id, ["hello", "world"], [])
    desc = pg_client.describe_stmt(id)
    assert desc["param_oids"] == [varchar_oid, text_oid]
    res = pg_client.execute(id)["rows"][0]
    assert res == ["hello", "world"]

    id = "600"
    pg_client.parse(id, "select $1 = $2", [varchar_oid, unknown_oid])
    pg_client.bind(id, id, ["foobar", "foobar"], [])
    desc = pg_client.describe_stmt(id)
    assert desc["param_oids"] == [varchar_oid, text_oid]
    res = pg_client.execute(id)["rows"][0]
    assert res == [True]


def test_gl_1730_via_bindings_int_2_4_8(pg_client: PgClient):
    # How to check: `select 'int8'::regtype::oid;`
    int2_oid = 21
    int4_oid = 23
    int8_oid = 20

    # we should default to int8
    # (at least until we implement int2 & int4 in picodata)
    id = "100"
    pg_client.parse(id, "select $1 = 100", [])
    pg_client.bind(id, id, [100], [])
    desc = pg_client.describe_stmt(id)
    assert desc["param_oids"] == [int8_oid]
    res = pg_client.execute(id)["rows"][0]
    assert res == [True]

    # basic test for int2
    id = "200"
    pg_client.parse(id, "select $1", [int2_oid])
    pg_client.bind(id, id, [2], [])
    desc = pg_client.describe_stmt(id)
    assert desc["param_oids"] == [int2_oid]
    res = pg_client.execute(id)["rows"][0]
    assert res == [2]

    # basic test for int4
    id = "300"
    pg_client.parse(id, "select $1", [int4_oid])
    pg_client.bind(id, id, [4], [])
    desc = pg_client.describe_stmt(id)
    assert desc["param_oids"] == [int4_oid]
    res = pg_client.execute(id)["rows"][0]
    assert res == [4]

    # basic test for int8
    id = "400"
    pg_client.parse(id, "select $1", [int8_oid])
    pg_client.bind(id, id, [8], [])
    desc = pg_client.describe_stmt(id)
    assert desc["param_oids"] == [int8_oid]
    res = pg_client.execute(id)["rows"][0]
    assert res == [8]


# XXX: see the comment for Int2 below.
class Varchar(str):
    # psycopg.types.string.StrBinaryDumperVarchar
    class Dumper(psycopg.adapt.Dumper):
        oid = psycopg.adapters.types["varchar"].oid
        format = psycopg.pq.Format.BINARY

        def dump(self, obj):
            return obj.encode("utf-8")

    @classmethod
    def register_for(cls, conn):
        conn.adapters.register_dumper(cls, cls.Dumper)


def test_gl_1730_via_psycopg_varchar(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")

    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True
    Varchar.register_for(conn)

    cur = conn.execute("select %b", [Varchar("foobar")])
    assert sorted(cur.fetchall()) == [("foobar",)]


# psycopg 3 will choose int's binary representation according to
# its value, meaning it may send it as int2, int4 or int8.
# However, we don't want to gamble. The test should be futureproof.
#
# See https://www.psycopg.org/psycopg3/docs/basic/adapt.html
class Int2(int):
    # psycopg.types.numeric.Int2BinaryDumper
    class Dumper(psycopg.adapt.Dumper):
        oid = psycopg.adapters.types["int2"].oid
        format = psycopg.pq.Format.BINARY

        def dump(self, obj):
            return obj.to_bytes(2, byteorder="big")

    @classmethod
    def register_for(cls, conn):
        conn.adapters.register_dumper(cls, cls.Dumper)


def test_gl_1730_via_psycopg_int2(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")

    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True
    Int2.register_for(conn)

    cur = conn.execute("select %b", [Int2(128)])
    assert sorted(cur.fetchall()) == [(128,)]
