import pg8000.native  # type: ignore
import pytest
from conftest import Postgres


def setup_pg8000_test_env(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"

    # Create a Postgres user with a compatible password
    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")

    # Connect to the server and enable autocommit
    conn = pg8000.native.Connection(user, password=password, host=postgres.host, port=postgres.port)
    conn.autocommit = True
    return conn


def test_volatility(postgres: Postgres):
    conn = setup_pg8000_test_env(postgres)
    with pytest.raises(
        pg8000.exceptions.DatabaseError,
        match="volatile function is not allowed in filter clause",
    ):
        conn.run("""select * from _pico_instance where _pico_bucket('default') is null""")
    conn.close()


def test_nil_tier_name(postgres: Postgres):
    conn = setup_pg8000_test_env(postgres)
    with pytest.raises(
        pg8000.exceptions.DatabaseError,
        match="_pico_bucket: tier name is null",
    ):
        conn.run("select _pico_bucket(null)")
    conn.close()


def test_empty_tier_name(postgres: Postgres):
    conn = setup_pg8000_test_env(postgres)
    with pytest.raises(
        pg8000.exceptions.DatabaseError,
        match="_pico_bucket: tier '' doesn't exist",
    ):
        conn.run("select _pico_bucket('')")
    conn.close()


def test_unknown_tier_name(postgres: Postgres):
    conn = setup_pg8000_test_env(postgres)
    with pytest.raises(
        pg8000.exceptions.DatabaseError,
        match="_pico_bucket: tier 'no_such_tier' doesn't exist",
    ):
        conn.run("select _pico_bucket('no_such_tier')")
    conn.close()


def test_use_indexing_inside_sql(postgres: Postgres):
    conn = setup_pg8000_test_env(postgres)
    result = conn.run("""select _pico_bucket('default')[1]['tier_name']""")
    assert result == [["default"]]
    conn.close()


def test_known_tier_name_single_instance(postgres: Postgres):
    conn = setup_pg8000_test_env(postgres)
    res = conn.run("select _pico_bucket('default')")
    assert res[0][0][0]["tier_name"] == "default"
    assert res[0][0][0]["bucket_id_start"] == 1
    assert res[0][0][0]["bucket_id_end"] == 3000
    assert res[0][0][0]["state"] == "active"
    assert res[0][0][0]["current_replicaset_uuid"] == postgres.instance.replicaset_uuid()
    assert res[0][0][0]["target_replicaset_uuid"] == postgres.instance.replicaset_uuid()
    conn.close()


def test_known_tier_name_multiple_instances(postgres: Postgres):
    DEFAULT_BUCKET_COUNT = 3000

    def merge_intervals(intervals):
        sorted_intervals = sorted((tuple(interval) for interval in intervals), key=lambda x: x[0])
        merged = []
        for interval in sorted_intervals:
            if merged and interval[0] <= merged[-1][1] + 1:
                # overlap or adjacent -> extend the last interval
                merged[-1] = (merged[-1][0], max(merged[-1][1], interval[1]))
            else:
                # no overlap -> append as new interval
                merged.append(interval)
        return merged

    conn = setup_pg8000_test_env(postgres)
    res = conn.run("select _pico_bucket('default')")

    uuids = []
    bucket_intervals = []

    for i in range(0, len(postgres.cluster.instances)):
        replicaset = res[0][0][i]
        uuids.append(replicaset["current_replicaset_uuid"])
        bucket_intervals.append((replicaset["bucket_id_start"], replicaset["bucket_id_end"]))

    # Check if all replicaset presents
    for i in range(0, len(uuids)):
        assert postgres.cluster.instances[i].replicaset_uuid() in uuids

    # Merge buckets from replicasets
    bucket_intervals_merged = merge_intervals(bucket_intervals)
    # Check if replicasets have all buckets
    for i in range(1, DEFAULT_BUCKET_COUNT + 1):
        assert i >= bucket_intervals_merged[0][0] and i <= bucket_intervals_merged[0][1]
    conn.close()
