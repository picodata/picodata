from conftest import Cluster


def test_http_address_migration_on_restart(cluster: Cluster):
    """Test that HTTP address is migrated to _pico_peer_address on restart."""
    cluster.set_config_file(
        yaml="""
cluster:
    tier:
        default:
instance:
    cluster_name: test
"""
    )

    i1 = cluster.add_instance(wait_online=True, enable_http=True)

    instance_name = i1.name
    raft_id = i1.sql(f"SELECT raft_id FROM _pico_instance WHERE name = '{instance_name}'")[0][0]
    http_port = i1.http_port
    expected_http_address = f"127.0.0.1:{http_port}"

    result = i1.sql(f"SELECT address FROM _pico_peer_address WHERE raft_id = {raft_id} AND connection_type = 'http'")
    assert len(result) == 1, "HTTP address should exist after initial start"
    assert result[0][0] == expected_http_address

    i1.sql(f"DELETE FROM _pico_peer_address WHERE raft_id = {raft_id} AND connection_type = 'http'")

    result = i1.sql(f"SELECT address FROM _pico_peer_address WHERE raft_id = {raft_id} AND connection_type = 'http'")
    assert len(result) == 0, "HTTP address should be deleted"

    i1.restart()
    i1.wait_online()

    result = i1.sql(f"SELECT address FROM _pico_peer_address WHERE raft_id = {raft_id} AND connection_type = 'http'")
    assert len(result) == 1, "HTTP address should be migrated after restart"
    assert result[0][0] == expected_http_address, f"Expected {expected_http_address}, got {result[0][0]}"

    result = i1.sql(
        f"SELECT connection_type FROM _pico_peer_address WHERE raft_id = {raft_id} ORDER BY connection_type"
    )
    connection_types = [row[0] for row in result]
    assert "http" in connection_types
    assert "iproto" in connection_types
    assert "pgproto" in connection_types


def test_http_address_migration_multiple_restarts(cluster: Cluster):
    """Test that HTTP address migration is idempotent (safe to run multiple times)."""
    cluster.set_config_file(
        yaml="""
cluster:
    tier:
        default:
instance:
    cluster_name: test
"""
    )

    i1 = cluster.add_instance(wait_online=True, enable_http=True)

    instance_name = i1.name
    raft_id = i1.sql(f"SELECT raft_id FROM _pico_instance WHERE name = '{instance_name}'")[0][0]

    i1.sql(f"DELETE FROM _pico_peer_address WHERE raft_id = {raft_id} AND connection_type = 'http'")

    i1.restart()
    i1.wait_online()

    result = i1.sql(f"SELECT address FROM _pico_peer_address WHERE raft_id = {raft_id} AND connection_type = 'http'")
    assert len(result) == 1, "HTTP address should be migrated after first restart"
    first_address = result[0][0]

    i1.restart()
    i1.wait_online()

    result = i1.sql(f"SELECT address FROM _pico_peer_address WHERE raft_id = {raft_id} AND connection_type = 'http'")
    assert len(result) == 1, "HTTP address should still be single entry after second restart"
    assert result[0][0] == first_address, "HTTP address should remain the same"


def test_http_address_no_migration_if_exists(cluster: Cluster):
    """Test that migration is skipped if HTTP address already exists."""
    cluster.set_config_file(
        yaml="""
cluster:
    tier:
        default:
instance:
    cluster_name: test
"""
    )

    i1 = cluster.add_instance(wait_online=True, enable_http=True)

    instance_name = i1.name
    raft_id = i1.sql(f"SELECT raft_id FROM _pico_instance WHERE name = '{instance_name}'")[0][0]

    result = i1.sql(f"SELECT address FROM _pico_peer_address WHERE raft_id = {raft_id} AND connection_type = 'http'")
    assert len(result) == 1, "HTTP address should exist for new instance"
    original_address = result[0][0]

    i1.restart()
    i1.wait_online()

    result = i1.sql(f"SELECT address FROM _pico_peer_address WHERE raft_id = {raft_id} AND connection_type = 'http'")
    assert len(result) == 1, "HTTP address should still be single entry"
    assert result[0][0] == original_address, "HTTP address should not change"
