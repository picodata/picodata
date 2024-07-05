from conftest import Postgres
import psycopg
from psycopg.types.json import Jsonb, Json
from decimal import Decimal
from uuid import UUID
import pg8000.native as pg8000  # type: ignore
import pytest


def test_decimal(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    # create a postgres user using a postgres compatible password
    postgres.instance.sql(
        f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5"
    )
    # allow user to create tables
    postgres.instance.sudo_sql(f'GRANT CREATE TABLE TO "{user}"')

    # connect to the server and enable autocommit as we
    # don't support interactive transactions
    conn = psycopg.connect(
        f"user = {user} password={password} host={host} port={port} sslmode=disable"
    )
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


def test_uuid(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    # create a postgres user using a postgres compatible password
    postgres.instance.sql(
        f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5"
    )
    # allow user to create tables
    postgres.instance.sudo_sql(f'GRANT CREATE TABLE TO "{user}"')

    # connect to the server and enable autocommit as we
    # don't support interactive transactions
    conn = psycopg.connect(
        f"user = {user} password={password} host={host} port={port} sslmode=disable"
    )
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

    postgres.instance.sql(
        f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5"
    )
    postgres.instance.sudo_sql(f'GRANT CREATE TABLE TO "{user}"')

    conn = pg8000.Connection(
        user, password=password, host=postgres.host, port=postgres.port
    )

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
    assert rows == [["value1"], ["value2"]]


def test_unsigned(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    # create a postgres user using a postgres compatible password
    postgres.instance.sql(
        f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5"
    )
    # allow user to create tables
    postgres.instance.sudo_sql(f'GRANT CREATE TABLE TO "{user}"')

    # connect to the server and enable autocommit as we
    # don't support interactive transactions
    conn = psycopg.connect(
        f"user = {user} password={password} host={host} port={port} sslmode=disable"
    )
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

    # Note: u64::MAX can be sent because psycopg sends it as numeric
    conn.execute(""" INSERT INTO T VALUES(%t); """, (u64_max,))

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

    postgres.instance.sql(
        f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5"
    )

    postgres.instance.sudo_sql(f'GRANT READ ON TABLE "_pico_user" TO "{user}"')

    conn = psycopg.connect(
        f"user = {user} password={password} host={host} port={port} sslmode=disable"
    )
    conn.autocommit = True

    # test text encoding
    cur = conn.execute(
        """ SELECT \"auth\" FROM \"_pico_user\" WHERE \"id\" = 0; """, binary=False
    )
    assert cur.fetchall() == [(["chap-sha1", "vhvewKp0tNyweZQ+cFKAlsyphfg="],)]

    # test binary encoding
    cur = conn.execute(
        """ SELECT \"auth\" FROM \"_pico_user\" WHERE \"id\" = 0; """, binary=True
    )
    assert cur.fetchall() == [(["chap-sha1", "vhvewKp0tNyweZQ+cFKAlsyphfg="],)]

    # text array parameters should throw an error
    with pytest.raises(
        psycopg.errors.FeatureNotSupported,
        match="feature is not supported: type _int2",  # _int2 -> array of integers
    ):
        cur = conn.execute(
            """ SELECT \"auth\" FROM \"_pico_user\" WHERE \"auth\" = %t; """, ([1, 2],)
        )

    # binary array parameters should throw an error
    with pytest.raises(
        psycopg.errors.FeatureNotSupported,
        match="feature is not supported: type _int2",  # _int2 -> array of integers
    ):
        cur = conn.execute(
            """ SELECT \"auth\" FROM \"_pico_user\" WHERE \"auth\" = %b; """, ([1, 2],)
        )

    # test empty strings representation in arrays
    # * in text repr empty strings in arrays are sent as quotes ""
    # * in bin repr empty strings in array are sent as any other string

    # text repr case
    cur = conn.execute(
        """ SELECT \"auth\" FROM \"_pico_user\" WHERE \"name\" = 'admin'; """, binary=False
    )
    assert cur.fetchall() == [(["chap-sha1", ""],)]

    # bin repr case
    cur = conn.execute(
        """ SELECT \"auth\" FROM \"_pico_user\" WHERE \"name\" = 'admin'; """, binary=True
    )
    assert cur.fetchall() == [(["chap-sha1", ""],)]


def test_map(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(
        f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5"
    )

    postgres.instance.sudo_sql(f'GRANT READ ON TABLE "_pico_table" TO "{user}"')

    conn = psycopg.connect(
        f"user = {user} password={password} host={host} port={port} sslmode=disable"
    )
    conn.autocommit = True

    data = postgres.instance.sql(""" SELECT "distribution" FROM "_pico_table" """)
    distribution = [tuple(row) for row in data["rows"]]

    # test text encoding
    cur = conn.execute(""" SELECT "distribution" FROM "_pico_table" """, binary=False)
    assert cur.fetchall() == distribution

    # test binary encoding
    cur = conn.execute(""" SELECT "distribution" FROM "_pico_table" """, binary=True)
    assert cur.fetchall() == distribution

    # text json parameters should throw an error
    with pytest.raises(
        psycopg.errors.FeatureNotSupported,
        match="feature is not supported: cannot encode json: .*",
    ):
        cur = conn.execute(
            """ SELECT \"distribution\" FROM \"_pico_table\" WHERE \"distribution\" = %t; """,
            (Json(distribution[0]),),
        )

    # text jsonb parameters should throw an error
    with pytest.raises(
        psycopg.errors.FeatureNotSupported,
        match="feature is not supported: cannot encode json: .*",
    ):
        cur = conn.execute(
            """ SELECT \"distribution\" FROM \"_pico_table\" WHERE \"distribution\" = %t; """,
            (Jsonb(distribution[0]),),
        )

    # binary json parameters should throw an error
    with pytest.raises(
        psycopg.errors.FeatureNotSupported,
        match="feature is not supported: cannot encode json: .*",
    ):
        cur = conn.execute(
            """ SELECT \"distribution\" FROM \"_pico_table\" WHERE \"distribution\" = %b; """,
            (Json(distribution[0]),),
        )

    # binary jsonb parameters should throw an error
    with pytest.raises(
        psycopg.errors.FeatureNotSupported,
        match="feature is not supported: cannot encode json: .*",
    ):
        cur = conn.execute(
            """ SELECT \"distribution\" FROM \"_pico_table\" WHERE \"distribution\" = %b; """,
            (Jsonb(distribution[0]),),
        )
