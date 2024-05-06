from conftest import Postgres
import psycopg
from decimal import Decimal


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
