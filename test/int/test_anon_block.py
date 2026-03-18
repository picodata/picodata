import psycopg
import threading

from conftest import Cluster, find_routed_pk, Postgres


def setup_user_and_table(instance):
    # Setup user
    user = "postgres"
    password = "Passw0rd"
    instance.sql(f"CREATE USER {user} WITH PASSWORD '{password}'", sudo=True)
    instance.sql(f"GRANT CREATE TABLE TO {user}", sudo=True)

    conn_info = f"host={instance.pg_host} port={instance.pg_port} user={user} password = {password}"

    # Setup table
    with psycopg.connect(conn_info, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, a INTEGER);")
            cur.execute("INSERT INTO t (pk, a) VALUES (1, 0);")

    return conn_info


def test_remote_block_dispatch(cluster: Cluster):
    leader, *_ = cluster.deploy(instance_count=2)
    cluster.wait_balanced()

    conn_info = setup_user_and_table(leader)
    remote_pk = find_routed_pk(leader, is_local=False, start=2)

    with psycopg.connect(conn_info, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO t (pk, a) VALUES (%s, %s)", (remote_pk, 42))
            cur.execute(
                """
                DO $$ BEGIN
                    RETURN QUERY SELECT a FROM t WHERE pk = %s;
                END $$;
                """,
                (remote_pk,),
            )
            assert cur.fetchall() == [(42,)]


def test_local_filtered_dml_block_dispatch(cluster: Cluster):
    leader, *_ = cluster.deploy(instance_count=2)
    cluster.wait_balanced()

    local_pk = find_routed_pk(leader, is_local=True)

    leader.sql("CREATE TABLE t (pk INTEGER PRIMARY KEY, a INTEGER)")
    leader.sql("INSERT INTO t VALUES (?, ?)", local_pk, 0)

    dml = leader.sql(
        f"""
        DO $$ BEGIN
            UPDATE t SET a = a + 1 WHERE pk = {local_pk};
        END $$;
        """
    )
    assert dml == {"row_count": 1}
    assert leader.sql("SELECT a FROM t WHERE pk = ?", local_pk) == [[1]]


def test_r_block_atomicity(postgres: Postgres):
    """
    Ensure that 2 reads within the same block always return
    the same results despite concurrent updates.
    """

    conn_info = setup_user_and_table(postgres.instance)

    failures = []

    # Read the same value twice within the block and expecte to get the same value,
    # otherwise blocks are not atomic
    def reader():
        thread_failures = []
        with psycopg.connect(conn_info, autocommit=True) as conn:
            with conn.cursor() as cur:
                for _ in range(1000):
                    cur.execute("""
                        DO $$
                        BEGIN
                            RETURN QUERY SELECT a FROM t WHERE pk = 1;
                            RETURN QUERY SELECT a FROM t WHERE pk = 1;
                        END $$;
                    """)
                    results = cur.fetchall()

                    if results[0] != results[1]:
                        thread_failures.append((results[0], results[1]))

        if thread_failures:
            failures.extend(thread_failures)

    # Update the value that readers read.
    def writer():
        pass
        with psycopg.connect(conn_info, autocommit=True) as conn:
            with conn.cursor() as cur:
                for _ in range(1000):
                    cur.execute("UPDATE t SET a = a + 1 WHERE pk = 1;")

    # Run readers and writers in parallel
    threads = []
    for _ in range(2):
        threads.append(threading.Thread(target=writer))
        threads.append(threading.Thread(target=reader))

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Ensure all updates were successful
    with psycopg.connect(conn_info, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT a FROM t WHERE pk = 1;")
            assert cur.fetchall() == [(2000,)]

    assert failures == [], f"Block reads are not atomic: {failures}"


def test_rw_block_atomicity(postgres: Postgres):
    """
    Ensure block reads and updates are atomic.
    Every block reads and updates the same value twice.
    If blocks are atomic, we must never observe intermediate value between 2 updates.
    """

    conn_info = setup_user_and_table(postgres.instance)

    failures = []

    # Read value and update it twice.
    def worker():
        thread_failures = []
        with psycopg.connect(conn_info, autocommit=True) as conn:
            with conn.cursor() as cur:
                for _ in range(1000):
                    cur.execute("""
                        DO $$
                        BEGIN
                            RETURN QUERY SELECT a FROM t WHERE pk = 1;
                            UPDATE t SET a = a + 1 WHERE pk = 1;
                            UPDATE t SET a = a + 1 WHERE pk = 1;
                        END $$;
                    """)
                    results = cur.fetchall()

                    if results[0][0] % 2 != 0:
                        thread_failures.append(results)

        if thread_failures:
            failures.extend(thread_failures)

    # Run workers in parallel
    threads = []
    for _ in range(4):
        threads.append(threading.Thread(target=worker))

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Ensure all updates were successful
    with psycopg.connect(conn_info, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT a FROM t WHERE pk = 1;")
            assert cur.fetchall() == [(8000,)]

    assert failures == [], f"Blocks are not atomic: {failures}"


def test_parameterized_blocks(postgres: Postgres):
    conn_info = setup_user_and_table(postgres.instance)

    with psycopg.connect(conn_info, autocommit=True) as conn:
        with conn.cursor() as cur:
            assert cur.execute("SELECT %s", ("kek",)).fetchall() == [("kek",)]

            # basic query with parameter
            cur.execute(
                """
                DO $$ BEGIN
                    RETURN QUERY SELECT %s;
                END $$;
                """,
                ("kek",),
            )
            assert cur.fetchall() == [("kek",)]

            # reuse the same parameter
            cur.execute(
                """
                DO $$ BEGIN
                    RETURN QUERY SELECT %(p)s;
                    RETURN QUERY SELECT %(p)s;
                END $$;
                """,
                {"p": 1},
            )
            assert cur.fetchall() == [(1,), (1,)]

            # parameter in filter
            cur.execute(
                """
                DO $$ BEGIN
                    RETURN QUERY SELECT a FROM t WHERE pk = %(p)s;
                    UPDATE t SET a = %(p)s WHERE pk = %(p)s;
                END $$;
                """,
                {"p": 1},
            )
            assert cur.fetchall() == [(0,)]

            # ensure updated
            cur.execute(
                """
                DO $$ BEGIN
                    RETURN QUERY SELECT a FROM t WHERE pk = %(p)s;
                    UPDATE t SET a = %(p)s WHERE pk = %(p)s;
                END $$;
                """,
                {"p": 1},
            )
            assert cur.fetchall() == [(1,)]
