from conftest import Postgres
import psycopg


def test_large_messages(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"
    host = postgres.host
    port = postgres.port

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)
    conn = psycopg.connect(f"user={user} password={password} host={host} port={port} sslmode=disable")
    conn.autocommit = True

    conn.execute(
        """
        create table "t" (
            "id" int not null primary key,
            "value" text
        )
        option (timeout = 3);
        """
    )

    count = 100
    payload = "A" * 2**17  # 128 KiB

    # check if pgproto can read messages that are larger than the initial buffer size
    dml = """ INSERT INTO "t" VALUES """
    # TODO: dml += ",".join(f""" ({i}, %(payload)s::text """ for i in range(count))
    dml += ",".join(f""" ({i}, cast (%(payload)s as text)) """ for i in range(count))
    conn.execute(dml, {"payload": payload})

    # check if pgproto can send messages that are larger than the initial buffer size
    cur = conn.execute(""" SELECT * FROM "t" """)
    assert sorted(cur.fetchall()) == [(i, payload) for i in range(count)]
