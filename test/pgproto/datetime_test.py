import psycopg
import pg8000.native  # type: ignore
import pytest
import datetime
import time
import re
from conftest import Postgres


def setup_psycopg_test_env(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    # Create a Postgres user with a compatible password
    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    # Allow the user to create tables
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    # Connect to the server and enable autocommit
    conn = psycopg.connect(f"user={user} password={password} host={host} port={port} sslmode=disable")
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
    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    # Allow the user to create tables
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    # Connect to the server and enable autocommit
    conn = pg8000.native.Connection(user, password=password, host=postgres.host, port=postgres.port)
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
    conn.run("""INSERT INTO T (ID) VALUES (:p);""", p=dt_iso, types={"p": pg8000.TIMESTAMPTZ})
    result = conn.run("""SELECT * FROM T WHERE ID = :p;""", p=dt_iso, types={"p": pg8000.TIMESTAMPTZ})
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
    expected_dt_rfc2822 = datetime(2023, 7, 7, 12, 34, 56, tzinfo=timezone(timedelta(hours=2)))
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
    dt_future = datetime.datetime(9999, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)
    cur.execute("""INSERT INTO T (ID) VALUES (%t);""", (dt_future,))
    cur.execute("""SELECT * FROM T WHERE ID = %t;""", (dt_future,))
    result = cur.fetchall()
    assert result == [(dt_future,)]

    # Test leap year date
    dt_leap_year = datetime.datetime(2020, 2, 29, 12, 0, 0, tzinfo=datetime.timezone.utc)
    cur.execute("""INSERT INTO T (ID) VALUES (%t);""", (dt_leap_year,))
    cur.execute("""SELECT * FROM T WHERE ID = %t;""", (dt_leap_year,))
    result = cur.fetchall()
    assert result == [(dt_leap_year,)]


def test_timezones(postgres: Postgres):
    conn = setup_psycopg_test_env(postgres)
    cur = conn.cursor()

    # Test different timezones
    dt_utc_plus_5 = datetime.datetime(2023, 7, 7, 12, 34, 56, tzinfo=datetime.timezone(datetime.timedelta(hours=5)))
    cur.execute("""INSERT INTO T (ID) VALUES (%t);""", (dt_utc_plus_5,))
    cur.execute("""SELECT * FROM T WHERE ID = %t;""", (dt_utc_plus_5,))
    result = cur.fetchall()
    assert result == [(dt_utc_plus_5,)]

    dt_utc_minus_8 = datetime.datetime(2023, 7, 7, 12, 34, 56, tzinfo=datetime.timezone(datetime.timedelta(hours=-8)))
    cur.execute("""INSERT INTO T (ID) VALUES (%t);""", (dt_utc_minus_8,))
    cur.execute("""SELECT * FROM T WHERE ID = %t;""", (dt_utc_minus_8,))
    result = cur.fetchall()
    assert result == [(dt_utc_minus_8,)]


def test_subseconds(postgres: Postgres):
    conn = setup_psycopg_test_env(postgres)
    cur = conn.cursor()

    # Test microseconds
    dt_microseconds = datetime.datetime(2023, 7, 7, 12, 34, 56, 789012, tzinfo=datetime.timezone.utc)
    cur.execute("""INSERT INTO T (ID) VALUES (%t);""", (dt_microseconds,))
    cur.execute("""SELECT * FROM T WHERE ID = %t;""", (dt_microseconds,))
    result = cur.fetchall()
    assert result == [(dt_microseconds,)]

    # Test no subseconds
    dt_no_microseconds = datetime.datetime(2023, 7, 7, 12, 34, 56, tzinfo=datetime.timezone.utc)
    cur.execute("""INSERT INTO T (ID) VALUES (%t);""", (dt_no_microseconds,))
    cur.execute("""SELECT * FROM T WHERE ID = %t;""", (dt_no_microseconds,))
    result = cur.fetchall()
    assert result == [(dt_no_microseconds,)]


def test_invalid_dates(postgres: Postgres):
    conn = setup_pg8000_test_env(postgres)

    # Test invalid date with 13th month
    dt_invalid_month = "2023-13-01 12:00:00+00"
    with pytest.raises(
        pg8000.native.DatabaseError,
        match="failed to bind parameter \\$1: decoding error: '.*' is not a valid.*",
    ):
        conn.run(
            """INSERT INTO T (ID) VALUES (:p);""",
            p=dt_invalid_month,
            types={"p": pg8000.TIMESTAMPTZ},
        )

    # Test invalid date with 32nd day
    dt_invalid_day = "2023-01-32 12:00:00+00"
    with pytest.raises(
        pg8000.native.DatabaseError,
        match="failed to bind parameter \\$1: decoding error: '.*' is not a valid.*",
    ):
        conn.run(
            """INSERT INTO T (ID) VALUES (:p);""",
            p=dt_invalid_day,
            types={"p": pg8000.TIMESTAMPTZ},
        )

    # Test invalid date with time 24:00:00
    dt_invalid_time = "2023-01-01 24:00:00+00"
    with pytest.raises(
        pg8000.native.DatabaseError,
        match="failed to bind parameter \\$1: decoding error: '.*' is not a valid.*",
    ):
        conn.run(
            """INSERT INTO T (ID) VALUES (:p);""",
            p=dt_invalid_time,
            types={"p": pg8000.TIMESTAMPTZ},
        )

    # Test invalid date format
    dt_invalid_format = "07-07-2023 12:00:00+00"
    with pytest.raises(
        pg8000.native.DatabaseError,
        match="failed to bind parameter \\$1: decoding error: '.*' is not a valid.*",
    ):
        conn.run(
            """INSERT INTO T (ID) VALUES (:p);""",
            p=dt_invalid_format,
            types={"p": pg8000.TIMESTAMPTZ},
        )


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def test_localtimestamp(postgres: Postgres):
    conn = setup_psycopg_test_env(postgres)
    cur = conn.cursor()

    # Test simple SELECT
    cur.execute("SELECT LOCALTIMESTAMP;")
    result = cur.fetchall()
    assert len(result) == 1

    # Test that localtimestamp implements the same thing as current_timestamp
    cur.execute("""SELECT 
            localtimestamp(1), current_timestamp(1),
            localtimestamp(2), current_timestamp(2),
            localtimestamp(3), current_timestamp(3),
            localtimestamp(4), current_timestamp(4),
            localtimestamp(5), current_timestamp(5),
            localtimestamp(6), current_timestamp(6),
            localtimestamp(7), current_timestamp(7);
        """)
    result = cur.fetchall()
    for localtimestamp, current_timestamp in chunks(result[0], 2):
        assert localtimestamp == current_timestamp

    # Test that extremely large precision is maxxed at 6
    cur.execute(
        """
        SELECT localtimestamp(999999999999999999999999999999999999999999) = localtimestamp(6);
    """
    )
    result = cur.fetchall()
    assert result == [(True,)]

    # Test that precision > 6 is capped at 6
    cur.execute(
        """
        SELECT localtimestamp(7) = localtimestamp(6);
    """
    )
    result = cur.fetchall()
    assert result == [(True,)]

    # Verify that precision 0 has no fractional sec
    cur.execute(
        """
        SELECT localtimestamp(0)::text;
    """
    )
    result = cur.fetchall()
    timestamp_str = result[0][0]
    parsed_time = datetime.datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S%z")
    assert parsed_time
    # check that the local time is returned in the same timezone as local timezone as determined by python
    # note that this might break due to different TZ configuration for pytest & picodata or due to some DST weirdness
    tzinfo = parsed_time.tzinfo
    assert tzinfo
    tzoffset = tzinfo.utcoffset(parsed_time)
    assert tzoffset
    assert tzoffset.total_seconds() == -time.mktime(time.gmtime(0))

    # Test localtimestamp in a WHERE clause not NULL
    cur.execute("SELECT localtimestamp from (VALUES (1)) WHERE localtimestamp IS NOT NULL;")
    result = cur.fetchall()
    assert len(result) == 1

    # Test localtimestamp in a WHERE clause is NULL
    cur.execute("SELECT localtimestamp from (VALUES (1)) WHERE localtimestamp IS NULL;")
    result = cur.fetchall()
    assert len(result) == 0

    # Test localtimestamp in a subquery
    cur.execute("SELECT * FROM (SELECT localtimestamp AS time) AS subquery;")
    result = cur.fetchall()
    assert len(result) == 1

    # Test localtimestamp in VALUES clause
    cur.execute("INSERT INTO T (ID) VALUES (localtimestamp);")

    # Test localtimestamp in a projection
    cur.execute("SELECT localtimestamp AS time FROM T;")
    result = cur.fetchall()
    assert len(result) == 1


def test_current_date(postgres: Postgres):
    conn = setup_psycopg_test_env(postgres)
    cur = conn.cursor()

    # Test simple SELECT
    cur.execute("SELECT CURRENT_DATE;")
    result = cur.fetchall()
    assert len(result) == 1

    # Test that current_date implements the same thing as current_timestamp, down to the date
    cur.execute("""SELECT
            TO_CHAR(current_date, '%d %b %Y %z'), TO_CHAR(current_timestamp, '%d %b %Y %z');
        """)
    result = cur.fetchall()[0]
    assert result[0] == result[1]

    cur.execute("""SELECT current_date::text;""")
    result = cur.fetchall()
    timestamp_str = result[0][0]
    parsed_time = datetime.datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S%z")
    assert parsed_time
    # check that the local time is returned in the same timezone as local timezone as determined by python
    # note that this might break due to different TZ configuration for pytest & picodata or due to some DST weirdness
    tzinfo = parsed_time.tzinfo
    assert tzinfo
    tzoffset = tzinfo.utcoffset(parsed_time)
    assert tzoffset
    assert tzoffset.total_seconds() == -time.mktime(time.gmtime(0))
    # check that the time portion is zeroed
    assert parsed_time.hour == 0
    assert parsed_time.minute == 0
    assert parsed_time.second == 0


def test_unimplemented_time_functions(postgres: Postgres):
    conn = setup_psycopg_test_env(postgres)
    cur = conn.cursor()

    # those functions are parsed but raise an error when executing
    with pytest.raises(
        psycopg.errors.InternalError,
        match=re.escape("sbroad: SQL function `CURRENT_TIME` not implemented"),
    ):
        cur.execute("""SELECT current_time;""")
    with pytest.raises(
        psycopg.errors.InternalError,
        match=re.escape("sbroad: SQL function `CURRENT_TIME` not implemented"),
    ):
        cur.execute("""SELECT current_time(7);""")
    with pytest.raises(
        psycopg.errors.InternalError,
        match=re.escape("sbroad: SQL function `LOCALTIME` not implemented"),
    ):
        cur.execute("""SELECT localtime;""")
    with pytest.raises(
        psycopg.errors.InternalError,
        match=re.escape("sbroad: SQL function `LOCALTIME` not implemented"),
    ):
        cur.execute("""SELECT localtime(7);""")
