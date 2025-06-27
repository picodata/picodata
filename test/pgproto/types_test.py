from conftest import Postgres, PgClient
import psycopg
from psycopg.types.json import Jsonb, Json
from decimal import Decimal
from uuid import UUID
import pg8000.native as pg8000  # type: ignore
import pytest
import datetime


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
    u64_max = 18_446_744_073_709_551_615

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
    # parameter type, so cast is needed to make type system happy,
    conn.execute(""" INSERT INTO T VALUES(%t::unsigned); """, (u64_max,))

    # text encoding fails as u64::MAX can't be encoded as i64
    with pytest.raises(
        psycopg.InternalError,
        match="encoding error: out of range integral type conversion attempted",
    ):
        cur = conn.execute(""" SELECT * FROM T; """, binary=False)

    # binary encoding fails as u64::MAX can't be encoded as i64
    with pytest.raises(
        psycopg.InternalError,
        match="encoding error: out of range integral type conversion attempted",
    ):
        cur = conn.execute(""" SELECT * FROM T; """, binary=True)


def test_arrays(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")

    postgres.instance.sql(f'GRANT READ ON TABLE "_pico_user" TO "{user}"', sudo=True)

    conn = psycopg.connect(f"user = {user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    # test text encoding
    cur = conn.execute(""" SELECT \"auth\" FROM \"_pico_user\" WHERE \"id\" = 0; """, binary=False)
    assert cur.fetchall() == [(["md5", "md5084e0343a0486ff05530df6c705c8bb4"],)]

    # test binary encoding
    cur = conn.execute(""" SELECT \"auth\" FROM \"_pico_user\" WHERE \"id\" = 0; """, binary=True)
    assert cur.fetchall() == [(["md5", "md5084e0343a0486ff05530df6c705c8bb4"],)]

    # text array parameters should throw an error
    with pytest.raises(
        psycopg.errors.FeatureNotSupported,
        match="feature is not supported: _int2 parameters",  # _int2 -> array of integers
    ):
        cur = conn.execute(""" SELECT \"auth\" FROM \"_pico_user\" WHERE \"auth\" = %t; """, ([1, 2],))

    # binary array parameters should throw an error
    with pytest.raises(
        psycopg.errors.FeatureNotSupported,
        match="feature is not supported: _int2 parameters",  # _int2 -> array of integers
    ):
        cur = conn.execute(""" SELECT \"auth\" FROM \"_pico_user\" WHERE \"auth\" = %b; """, ([1, 2],))

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
    with pytest.raises(pg8000.DatabaseError, match=r"could not resolve operator overload for \+\(any, unsigned\)"):
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
