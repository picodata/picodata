from conftest import Postgres
import psycopg


def test_psql_session_from_the_guide(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    # create a postgres user using a postgres compatible password
    postgres.instance.sql(
        f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5"
    )
    # allow user to create tables
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    # connect to the server and enable autocommit as we
    # don't support interactive transactions
    conn = psycopg.connect(
        f"user = {user} password={password} host={host} port={port} sslmode=disable"
    )
    conn.autocommit = True

    conn.execute(
        """
        CREATE TABLE WAREHOUSE (
            W_ID INTEGER NOT NULL,
            W_NAME VARCHAR(10) NOT NULL,
            W_TAX DOUBLE,
            W_YTD DOUBLE,
            PRIMARY KEY (W_ID)
        )
        USING MEMTX DISTRIBUTED BY (W_ID)
        """
    )

    conn.execute(
        """
        INSERT INTO WAREHOUSE (W_ID, W_NAME) VALUES
            (1, 'aaaa'), (2, 'aaab'), (3, 'aaac'), (4, 'aaad')
    """
    )

    cur = conn.execute(
        """
        SELECT W_ID, W_NAME FROM WAREHOUSE
    """
    )
    rows = sorted(cur.fetchall())  # sorted is used to ensure the rows order
    assert rows == [(1, "aaaa"), (2, "aaab"), (3, "aaac"), (4, "aaad")]

    cur = conn.execute(
        """
        SELECT W_NAME FROM WAREHOUSE WHERE W_ID=1
    """
    )
    rows = cur.fetchall()
    assert [("aaaa",)] == rows
