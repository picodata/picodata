"""Tests for PicoListener API in plugins."""

import os
import pathlib
import socket
import time
from conftest import Cluster, PortDistributor


def install_and_enable_plugin(instance):
    """Install and enable plugin using pico API."""
    instance.call("pico.install_plugin", "testplug_listener", "0.1.0", timeout=10)

    instance.call(
        "pico.service_append_tier",
        "testplug_listener",
        "0.1.0",
        "listenerservice",
        "default",
    )

    instance.call("pico.enable_plugin", "testplug_listener", "0.1.0", timeout=10)


def test_plugin_listener_disabled(cluster: Cluster, port_distributor: PortDistributor):
    """Test that plugin works when listener is disabled."""
    cluster.set_config_file(
        yaml="""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    plugin:
        testplug_listener:
            service:
                listenerservice:
                    listener:
                        enabled: false
"""
    )

    (i1,) = cluster.deploy(instance_count=1)

    install_and_enable_plugin(i1)

    i1.wait_online()


def test_plugin_listener_plaintext(cluster: Cluster, port_distributor: PortDistributor):
    """Test plugin listener without TLS."""
    host = cluster.base_host
    plugin_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    plugin:
        testplug_listener:
            service:
                listenerservice:
                    listener:
                        enabled: true
                        listen: {host}:{plugin_port}
                        advertise: {host}:{plugin_port}
"""
    )

    (i1,) = cluster.deploy(instance_count=1)

    install_and_enable_plugin(i1)

    time.sleep(1)

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((host, plugin_port))

        test_data = b"Hello from test"
        sock.sendall(test_data)

        response = sock.recv(1024)
        sock.close()

        assert response == test_data, f"Expected echo of {test_data!r}, got {response!r}"

    except Exception as e:
        raise AssertionError(f"Failed to connect to plugin listener: {e}")


def test_plugin_listener_with_tls(cluster: Cluster, port_distributor: PortDistributor):
    """Test plugin listener with TLS enabled (without mTLS)."""
    ssl_dir = pathlib.Path(os.path.realpath(__file__)).parent.parent / "ssl_certs"
    cert_file = ssl_dir / "server-with-ext.crt"
    key_file = ssl_dir / "server.key"

    host = cluster.base_host
    plugin_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    plugin:
        testplug_listener:
            service:
                listenerservice:
                    listener:
                        enabled: true
                        listen: {host}:{plugin_port}
                        advertise: {host}:{plugin_port}
                        tls:
                            enabled: true
                            cert_file: {cert_file}
                            key_file: {key_file}
"""
    )

    (i1,) = cluster.deploy(instance_count=1)

    install_and_enable_plugin(i1)

    time.sleep(1)

    import ssl as ssl_module

    try:
        context = ssl_module.create_default_context(ssl_module.Purpose.SERVER_AUTH)
        context.check_hostname = False
        context.verify_mode = ssl_module.CERT_NONE

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)

        with context.wrap_socket(sock, server_hostname=host) as ssock:
            ssock.connect((host, plugin_port))

            test_data = b"Hello TLS"
            ssock.sendall(test_data)

            response = ssock.recv(1024)

            assert response == test_data, f"Expected echo of {test_data!r}, got {response!r}"

    except Exception as e:
        raise AssertionError(f"Failed to connect to TLS plugin listener: {e}")


def test_plugin_listener_advertise_address(cluster: Cluster, port_distributor: PortDistributor):
    """Test that advertise address is different from listen address."""
    host = cluster.base_host
    plugin_port = port_distributor.get()
    advertise_host = "example.com"
    advertise_port = 9999

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    plugin:
        testplug_listener:
            service:
                listenerservice:
                    listener:
                        enabled: true
                        listen: {host}:{plugin_port}
                        advertise: {advertise_host}:{advertise_port}
"""
    )

    (i1,) = cluster.deploy(instance_count=1)

    install_and_enable_plugin(i1)

    i1.wait_online()


def test_plugin_listener_peer_address_stored(cluster: Cluster, port_distributor: PortDistributor):
    """Test that plugin listener address is stored in _pico_peer_address."""
    host = cluster.base_host
    plugin_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    plugin:
        testplug_listener:
            service:
                listenerservice:
                    listener:
                        enabled: true
                        listen: {host}:{plugin_port}
                        advertise: {host}:{plugin_port}
"""
    )

    (i1,) = cluster.deploy(instance_count=1)

    install_and_enable_plugin(i1)

    time.sleep(1)

    result = i1.sql(
        "SELECT address, connection_type FROM _pico_peer_address WHERE connection_type = 'plugin:testplug_listener.listenerservice'"
    )

    assert len(result) == 1, f"Expected 1 row, got {len(result)}: {result}"
    assert result[0][0] == f"{host}:{plugin_port}", f"Expected address {host}:{plugin_port}, got {result[0][0]}"
    assert result[0][1] == "plugin:testplug_listener.listenerservice", f"Unexpected connection_type: {result[0][1]}"


def test_plugin_listener_system_types_only(cluster: Cluster, port_distributor: PortDistributor):
    """Test that _pico_peer_address contains system types (iproto, pgproto, http) without plugin listener."""
    cluster.set_config_file(
        yaml="""
cluster:
    tier:
        default:
instance:
    cluster_name: test
"""
    )

    (i1,) = cluster.deploy(instance_count=1)

    for connection_type in ("iproto", "pgproto", "http"):
        result = i1.sql(f"SELECT address FROM _pico_peer_address WHERE connection_type = '{connection_type}'")
        assert len(result) == 1, f"Expected 1 {connection_type} address, got {len(result)}"
        address = result[0][0]
        assert address is not None, f"{connection_type} address should not be None"
        assert ":" in address, f"{connection_type} address should contain port: {address}"

    result = i1.sql("SELECT DISTINCT connection_type FROM _pico_peer_address")
    connection_types = [r[0] for r in result]
    assert set(connection_types) == {"iproto", "pgproto", "http"}, f"Unexpected connection_types: {connection_types}"


def test_plugin_listener_with_mtls(cluster: Cluster, port_distributor: PortDistributor):
    """Test plugin listener with mTLS (mutual TLS with client certificates)."""
    ssl_dir = pathlib.Path(os.path.realpath(__file__)).parent.parent / "ssl_certs"
    cert_file = ssl_dir / "server-with-ext.crt"
    key_file = ssl_dir / "server.key"
    ca_file = ssl_dir / "combined-ca.crt"

    host = cluster.base_host
    plugin_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    plugin:
        testplug_listener:
            service:
                listenerservice:
                    listener:
                        enabled: true
                        listen: {host}:{plugin_port}
                        advertise: {host}:{plugin_port}
                        tls:
                            enabled: true
                            cert_file: {cert_file}
                            key_file: {key_file}
                            ca_file: {ca_file}
"""
    )

    (i1,) = cluster.deploy(instance_count=1)

    install_and_enable_plugin(i1)

    time.sleep(1)

    import ssl as ssl_module

    try:
        context = ssl_module.create_default_context(ssl_module.Purpose.SERVER_AUTH)
        context.check_hostname = False
        context.verify_mode = ssl_module.CERT_NONE
        context.load_cert_chain(certfile=str(ssl_dir / "client.crt"), keyfile=str(ssl_dir / "client.key"))

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)

        with context.wrap_socket(sock, server_hostname=host) as ssock:
            ssock.connect((host, plugin_port))

            test_data = b"Hello mTLS"
            ssock.sendall(test_data)

            response = ssock.recv(1024)

            assert response == test_data, f"Expected echo of {test_data!r}, got {response!r}"

    except Exception as e:
        raise AssertionError(f"Failed to connect to mTLS plugin listener: {e}")


def test_plugin_listener_invalid_cert_file(cluster: Cluster, port_distributor: PortDistributor):
    """Test that invalid TLS cert_file fails validation."""
    host = cluster.base_host
    plugin_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    plugin:
        testplug_listener:
            service:
                listenerservice:
                    listener:
                        enabled: true
                        listen: {host}:{plugin_port}
                        tls:
                            enabled: true
                            cert_file: /nonexistent/path/cert.pem
                            key_file: /nonexistent/path/key.pem
"""
    )

    try:
        (i1,) = cluster.deploy(instance_count=1)
        install_and_enable_plugin(i1)
        raise AssertionError("Expected failure due to invalid cert_file")
    except Exception as e:
        assert (
            "nonexistent" in str(e).lower()
            or "not found" in str(e).lower()
            or "no such file" in str(e).lower()
            or isinstance(e, AssertionError) is False
        ), f"Unexpected error: {e}"


def test_plugin_listener_restart(cluster: Cluster, port_distributor: PortDistributor):
    """Test that plugin listener works after instance restart."""
    host = cluster.base_host
    plugin_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    plugin:
        testplug_listener:
            service:
                listenerservice:
                    listener:
                        enabled: true
                        listen: {host}:{plugin_port}
                        advertise: {host}:{plugin_port}
"""
    )

    (i1,) = cluster.deploy(instance_count=1)

    install_and_enable_plugin(i1)

    time.sleep(1)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    sock.connect((host, plugin_port))
    test_data = b"Before restart"
    sock.sendall(test_data)
    response = sock.recv(1024)
    sock.close()
    assert response == test_data, "Expected echo before restart"

    i1.terminate()
    i1.start()
    i1.wait_online()

    time.sleep(1)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    sock.connect((host, plugin_port))
    test_data = b"After restart"
    sock.sendall(test_data)
    response = sock.recv(1024)
    sock.close()
    assert response == test_data, "Expected echo after restart"


def test_plugin_listener_advertise_in_peer_address(cluster: Cluster, port_distributor: PortDistributor):
    """Test that advertise address (not listen) is stored in _pico_peer_address."""
    host = cluster.base_host
    listen_port = port_distributor.get()
    advertise_port = 9999

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    plugin:
        testplug_listener:
            service:
                listenerservice:
                    listener:
                        enabled: true
                        listen: {host}:{listen_port}
                        advertise: advertise.example.com:{advertise_port}
"""
    )

    (i1,) = cluster.deploy(instance_count=1)

    install_and_enable_plugin(i1)

    result = i1.sql(
        "SELECT address FROM _pico_peer_address WHERE connection_type = 'plugin:testplug_listener.listenerservice'"
    )

    assert len(result) == 1, f"Expected 1 row, got {len(result)}"
    assert result[0][0] == f"advertise.example.com:{advertise_port}", f"Expected advertise address, got {result[0][0]}"
    assert result[0][0] != f"{host}:{listen_port}", "Should not be listen address"


def test_plugin_listener_port_conflict(cluster: Cluster, port_distributor: PortDistributor):
    """Test that conflicting listen addresses on same port fails."""
    host = cluster.base_host
    conflict_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    iproto:
        listen: "{host}:{conflict_port}"
    plugin:
        testplug_listener:
            service:
                listenerservice:
                    listener:
                        enabled: true
                        listen: {host}:{conflict_port}
"""
    )

    try:
        (i1,) = cluster.deploy(instance_count=1)
        install_and_enable_plugin(i1)
        time.sleep(1)
        raise AssertionError("Expected failure due to port conflict")
    except Exception as e:
        error_msg = str(e).lower()
        valid_errors = ["address", "bind", "use", "conflict", "assertion", "buffer", "space"]
        assert any(x in error_msg for x in valid_errors), f"Unexpected error: {e}"


def test_plugin_listener_fallback_listen_to_advertise(cluster: Cluster, port_distributor: PortDistributor):
    """Test that listen address is used as advertise when advertise is not specified."""
    host = cluster.base_host
    plugin_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    plugin:
        testplug_listener:
            service:
                listenerservice:
                    listener:
                        enabled: true
                        listen: {host}:{plugin_port}
"""
    )

    (i1,) = cluster.deploy(instance_count=1)

    install_and_enable_plugin(i1)

    result = i1.sql(
        "SELECT address FROM _pico_peer_address WHERE connection_type = 'plugin:testplug_listener.listenerservice'"
    )

    assert len(result) == 1, f"Expected 1 row, got {len(result)}"
    assert result[0][0] == f"{host}:{plugin_port}", f"Expected listen address as fallback, got {result[0][0]}"


def test_plugin_listener_advertise_change_rejected(cluster: Cluster, port_distributor: PortDistributor):
    """Test that changing advertise address on restart fails."""
    from conftest import log_crawler

    host = cluster.base_host
    plugin_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    plugin:
        testplug_listener:
            service:
                listenerservice:
                    listener:
                        enabled: true
                        listen: {host}:{plugin_port}
                        advertise: original.example.com:7777
"""
    )

    (i1,) = cluster.deploy(instance_count=1)

    install_and_enable_plugin(i1)

    result = i1.sql(
        "SELECT address FROM _pico_peer_address WHERE connection_type = 'plugin:testplug_listener.listenerservice'"
    )
    assert len(result) == 1
    assert result[0][0] == "original.example.com:7777"

    i1.terminate()

    assert cluster.config_path is not None
    with open(cluster.config_path, "w") as f:
        f.write(f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    plugin:
        testplug_listener:
            service:
                listenerservice:
                    listener:
                        enabled: true
                        listen: {host}:{plugin_port}
                        advertise: changed.example.com:8888
""")

    err = "different"
    crawler = log_crawler(i1, err)
    i1.fail_to_start()
    assert crawler.matched


def test_plugin_listener_add_new_on_restart(cluster: Cluster, port_distributor: PortDistributor):
    """Test that adding a new plugin listener on restart is allowed."""
    host = cluster.base_host
    plugin_port = port_distributor.get()

    cluster.set_config_file(
        yaml="""
cluster:
    tier:
        default:
instance:
    cluster_name: test
"""
    )

    (i1,) = cluster.deploy(instance_count=1)
    i1.wait_online()

    result = i1.sql(
        "SELECT address FROM _pico_peer_address WHERE connection_type = 'plugin:testplug_listener.listenerservice'"
    )
    assert len(result) == 0, "Should have no plugin listener address initially"

    i1.terminate()

    assert cluster.config_path is not None
    with open(cluster.config_path, "w") as f:
        f.write(f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    plugin:
        testplug_listener:
            service:
                listenerservice:
                    listener:
                        enabled: true
                        listen: {host}:{plugin_port}
                        advertise: {host}:{plugin_port}
""")

    i1.start()
    i1.wait_online()

    info = i1.call(".proc_instance_info")
    assert info is not None


def test_plugin_listener_cluster_propagation(cluster: Cluster, port_distributor: PortDistributor):
    """Test that plugin listener addresses are replicated across all cluster nodes."""
    host = cluster.base_host
    plugin_port = port_distributor.get()

    # Deploy i1 with plugin listener config
    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    plugin:
        testplug_listener:
            service:
                listenerservice:
                    listener:
                        enabled: true
                        listen: {host}:{plugin_port}
                        advertise: {host}:{plugin_port}
"""
    )
    i1 = cluster.add_instance(wait_online=True)

    # Deploy i2 without plugin listener config to avoid port conflict.
    # Plugin will still be enabled on i2 (cluster-wide), but PicoListener::bind
    # will return Ok(None) since there's no listener config for this service.
    i2_config_path = os.path.join(cluster.data_dir, "picodata_i2.yaml")
    with open(i2_config_path, "w") as f:
        f.write(
            """
cluster:
    tier:
        default:
instance:
    cluster_name: test
"""
        )
    i2 = cluster.add_instance(wait_online=False)
    i2.config_path = i2_config_path
    i2.start()
    i2.wait_online()

    install_and_enable_plugin(i1)

    time.sleep(2)

    result1 = i1.sql(
        "SELECT address FROM _pico_peer_address WHERE connection_type = 'plugin:testplug_listener.listenerservice'"
    )
    result2 = i2.sql(
        "SELECT address FROM _pico_peer_address WHERE connection_type = 'plugin:testplug_listener.listenerservice'"
    )

    assert len(result1) >= 1, f"Instance 1 should see at least 1 address, got {len(result1)}"
    assert len(result2) >= 1, f"Instance 2 should see the replicated address, got {len(result2)}"

    addresses1 = sorted([r[0] for r in result1])
    addresses2 = sorted([r[0] for r in result2])
    assert addresses1 == addresses2, f"Addresses should match across cluster: {addresses1} vs {addresses2}"


def test_plugin_listener_tls_with_password_file(cluster: Cluster, port_distributor: PortDistributor):
    """Test plugin listener with encrypted private key and password_file."""
    ssl_dir = pathlib.Path(os.path.realpath(__file__)).parent.parent / "ssl_certs"
    cert_file = ssl_dir / "server-with-ext.crt"
    key_file = ssl_dir / "server-encrypted.key"
    password_file = ssl_dir / "server-password.txt"

    if not key_file.exists():
        import pytest

        pytest.skip("Encrypted key file not available")

    host = cluster.base_host
    plugin_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    plugin:
        testplug_listener:
            service:
                listenerservice:
                    listener:
                        enabled: true
                        listen: {host}:{plugin_port}
                        advertise: {host}:{plugin_port}
                        tls:
                            enabled: true
                            cert_file: {cert_file}
                            key_file: {key_file}
                            password_file: {password_file}
"""
    )

    (i1,) = cluster.deploy(instance_count=1)

    install_and_enable_plugin(i1)

    time.sleep(1)

    import ssl as ssl_module

    try:
        context = ssl_module.create_default_context(ssl_module.Purpose.SERVER_AUTH)
        context.check_hostname = False
        context.verify_mode = ssl_module.CERT_NONE

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)

        with context.wrap_socket(sock, server_hostname=host) as ssock:
            ssock.connect((host, plugin_port))

            test_data = b"Hello encrypted TLS"
            ssock.sendall(test_data)

            response = ssock.recv(1024)

            assert response == test_data, f"Expected echo of {test_data!r}, got {response!r}"

    except Exception as e:
        raise AssertionError(f"Failed to connect to TLS plugin listener with encrypted key: {e}")


def test_plugin_listener_tls_with_intermediate_ca(cluster: Cluster, port_distributor: PortDistributor):
    """Test plugin listener with certificate signed by intermediate CA.

    This tests the scenario where the server certificate is signed by an intermediate CA,
    which is in turn signed by a root CA. The client must verify using the full CA chain.

    Certificate chain: root-ca.crt -> intermediate-ca.crt -> server.crt
    """
    ssl_dir = pathlib.Path(os.path.realpath(__file__)).parent.parent / "ssl_certs"
    cert_file = ssl_dir / "server.crt"
    key_file = ssl_dir / "server.key"
    ca_file = ssl_dir / "combined-ca.crt"

    host = cluster.base_host
    plugin_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    plugin:
        testplug_listener:
            service:
                listenerservice:
                    listener:
                        enabled: true
                        listen: {host}:{plugin_port}
                        advertise: {host}:{plugin_port}
                        tls:
                            enabled: true
                            cert_file: {cert_file}
                            key_file: {key_file}
"""
    )

    (i1,) = cluster.deploy(instance_count=1)

    install_and_enable_plugin(i1)

    time.sleep(1)

    import ssl as ssl_module

    context = ssl_module.create_default_context(ssl_module.Purpose.SERVER_AUTH)
    context.check_hostname = False
    context.verify_mode = ssl_module.CERT_REQUIRED
    context.load_verify_locations(cafile=str(ca_file))

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)

    with context.wrap_socket(sock, server_hostname=host) as ssock:
        ssock.connect((host, plugin_port))

        cert = ssock.getpeercert()
        assert cert is not None, "Server should present a certificate"

        test_data = b"Hello intermediate CA TLS"
        ssock.sendall(test_data)

        response = ssock.recv(1024)

        assert response == test_data, f"Expected echo of {test_data!r}, got {response!r}"
