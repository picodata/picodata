import psycopg
import pg8000.native  # type: ignore
import pytest
import datetime
from conftest import Postgres


def setup_psycopg_test_env(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    # Create a Postgres user with a compatible password
    postgres.instance.sql(
        f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5"
    )
    # Allow the user to create tables
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    # Connect to the server and enable autocommit
    conn = psycopg.connect(
        f"user={user} password={password} host={host} port={port} sslmode=disable"
    )
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
    return conn


def setup_pg8000_test_env(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"

    # Create a Postgres user with a compatible password
    postgres.instance.sql(
        f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5"
    )
    # Allow the user to create tables
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    # Connect to the server and enable autocommit
    conn = pg8000.native.Connection(
        user, password=password, host=postgres.host, port=postgres.port
    )
    conn.autocommit = True

    conn.run(
        """
        CREATE TABLE T (
            ID DATETIME NOT NULL,
            PRIMARY KEY (ID)
        )
        USING MEMTX DISTRIBUTED BY (ID);
        """
    )
    return conn


def test_various_datetime_formats(postgres: Postgres):
    from datetime import datetime, timedelta, timezone

    conn = setup_pg8000_test_env(postgres)

    # Test ISO 8601 format
    dt_iso = "2023-07-07T12:34:56Z"
    conn.run(
        """INSERT INTO T (ID) VALUES (:p);""", p=dt_iso, types={"p": pg8000.TIMESTAMPTZ}
    )
    result = conn.run(
        """SELECT * FROM T WHERE ID = :p;""", p=dt_iso, types={"p": pg8000.TIMESTAMPTZ}
    )
    expected_iso = datetime(2023, 7, 7, 12, 34, 56, tzinfo=timezone.utc)
    assert result == [[expected_iso]]

    # Test RFC 2822 format
    dt_rfc2822 = "Fri, 07 Jul 2023 12:34:56 +0200"
    conn.run(
        """INSERT INTO T (ID) VALUES (:p);""",
        p=dt_rfc2822,
        types={"p": pg8000.TIMESTAMPTZ},
    )
    result = conn.run(
        """SELECT * FROM T WHERE ID = :p;""",
        p=dt_rfc2822,
        types={"p": pg8000.TIMESTAMPTZ},
    )
    expected_dt_rfc2822 = datetime(
        2023, 7, 7, 12, 34, 56, tzinfo=timezone(timedelta(hours=2))
    )
    assert result == [[expected_dt_rfc2822]]

    # Test RFC 3339 format
    dt_rfc3339 = "2023-07-07T12:34:56.123456Z"
    conn.run(
        """INSERT INTO T (ID) VALUES (:p);""",
        p=dt_rfc3339,
        types={"p": pg8000.TIMESTAMPTZ},
    )
    result = conn.run(
        """SELECT * FROM T WHERE ID = :p;""",
        p=dt_rfc3339,
        types={"p": pg8000.TIMESTAMPTZ},
    )
    expected_rfc3339 = datetime(2023, 7, 7, 12, 34, 56, 123456, tzinfo=timezone.utc)
    assert result == [[expected_rfc3339]]


def test_edge_cases(postgres: Postgres):
    conn = setup_psycopg_test_env(postgres)
    cur = conn.cursor()

    # Test Unix epoch start
    dt_epoch_start = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    cur.execute("""INSERT INTO T (ID) VALUES (%t);""", (dt_epoch_start,))
    cur.execute("""SELECT * FROM T WHERE ID = %t;""", (dt_epoch_start,))
    result = cur.fetchall()
    assert result == [(dt_epoch_start,)]

    # Test far future date
    dt_future = datetime.datetime(
        9999, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc
    )
    cur.execute("""INSERT INTO T (ID) VALUES (%t);""", (dt_future,))
    cur.execute("""SELECT * FROM T WHERE ID = %t;""", (dt_future,))
    result = cur.fetchall()
    assert result == [(dt_future,)]

    # Test leap year date
    dt_leap_year = datetime.datetime(
        2020, 2, 29, 12, 0, 0, tzinfo=datetime.timezone.utc
    )
    cur.execute("""INSERT INTO T (ID) VALUES (%t);""", (dt_leap_year,))
    cur.execute("""SELECT * FROM T WHERE ID = %t;""", (dt_leap_year,))
    result = cur.fetchall()
    assert result == [(dt_leap_year,)]


def test_timezones(postgres: Postgres):
    conn = setup_psycopg_test_env(postgres)
    cur = conn.cursor()

    # Test different timezones
    dt_utc_plus_5 = datetime.datetime(
        2023, 7, 7, 12, 34, 56, tzinfo=datetime.timezone(datetime.timedelta(hours=5))
    )
    cur.execute("""INSERT INTO T (ID) VALUES (%t);""", (dt_utc_plus_5,))
    cur.execute("""SELECT * FROM T WHERE ID = %t;""", (dt_utc_plus_5,))
    result = cur.fetchall()
    assert result == [(dt_utc_plus_5,)]

    dt_utc_minus_8 = datetime.datetime(
        2023, 7, 7, 12, 34, 56, tzinfo=datetime.timezone(datetime.timedelta(hours=-8))
    )
    cur.execute("""INSERT INTO T (ID) VALUES (%t);""", (dt_utc_minus_8,))
    cur.execute("""SELECT * FROM T WHERE ID = %t;""", (dt_utc_minus_8,))
    result = cur.fetchall()
    assert result == [(dt_utc_minus_8,)]


def test_subseconds(postgres: Postgres):
    conn = setup_psycopg_test_env(postgres)
    cur = conn.cursor()

    # Test microseconds
    dt_microseconds = datetime.datetime(
        2023, 7, 7, 12, 34, 56, 789012, tzinfo=datetime.timezone.utc
    )
    cur.execute("""INSERT INTO T (ID) VALUES (%t);""", (dt_microseconds,))
    cur.execute("""SELECT * FROM T WHERE ID = %t;""", (dt_microseconds,))
    result = cur.fetchall()
    assert result == [(dt_microseconds,)]

    # Test no subseconds
    dt_no_microseconds = datetime.datetime(
        2023, 7, 7, 12, 34, 56, tzinfo=datetime.timezone.utc
    )
    cur.execute("""INSERT INTO T (ID) VALUES (%t);""", (dt_no_microseconds,))
    cur.execute("""SELECT * FROM T WHERE ID = %t;""", (dt_no_microseconds,))
    result = cur.fetchall()
    assert result == [(dt_no_microseconds,)]


def test_invalid_dates(postgres: Postgres):
    conn = setup_pg8000_test_env(postgres)

    # Test invalid date with 13th month
    dt_invalid_month = "2023-13-01 12:00:00+00"
    with pytest.raises(
        pg8000.exceptions.DatabaseError, match="failed to parse datetime value"
    ):
        conn.run(
            """INSERT INTO T (ID) VALUES (:p);""",
            p=dt_invalid_month,
            types={"p": pg8000.TIMESTAMPTZ},
        )

    # Test invalid date with 32nd day
    dt_invalid_day = "2023-01-32 12:00:00+00"
    with pytest.raises(
        pg8000.exceptions.DatabaseError, match="failed to parse datetime value"
    ):
        conn.run(
            """INSERT INTO T (ID) VALUES (:p);""",
            p=dt_invalid_day,
            types={"p": pg8000.TIMESTAMPTZ},
        )

    # Test invalid date with time 24:00:00
    dt_invalid_time = "2023-01-01 24:00:00+00"
    with pytest.raises(
        pg8000.exceptions.DatabaseError, match="failed to parse datetime value"
    ):
        conn.run(
            """INSERT INTO T (ID) VALUES (:p);""",
            p=dt_invalid_time,
            types={"p": pg8000.TIMESTAMPTZ},
        )

    # Test invalid date format
    dt_invalid_format = "07-07-2023 12:00:00+00"
    with pytest.raises(
        pg8000.exceptions.DatabaseError, match="failed to parse datetime value"
    ):
        conn.run(
            """INSERT INTO T (ID) VALUES (:p);""",
            p=dt_invalid_format,
            types={"p": pg8000.TIMESTAMPTZ},
        )
