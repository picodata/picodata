from conftest import Cluster, PortDistributor, log_crawler
import os
import pathlib


def test_http_unified_config(cluster: Cluster, port_distributor: PortDistributor):
    """Test the new unified http configuration section."""
    host = cluster.base_host
    http_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    http:
        enabled: true
        listen: {host}:{http_port}
        advertise: {host}:{http_port}
"""
    )

    instance = cluster.add_instance(wait_online=True)

    info = instance.call(".proc_instance_info")
    assert info is not None


def test_http_with_tls(cluster: Cluster, port_distributor: PortDistributor):
    """Test the new unified http configuration with TLS enabled."""
    ssl_dir = pathlib.Path(os.path.realpath(__file__)).parent.parent / "ssl_certs"
    cert_file = ssl_dir / "server-with-ext.crt"
    key_file = ssl_dir / "server.key"
    ca_file = ssl_dir / "combined-ca.crt"

    host = cluster.base_host
    http_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    http:
        enabled: true
        listen: {host}:{http_port}
        tls:
            enabled: true
            cert_file: {cert_file}
            key_file: {key_file}
            ca_file: {ca_file}
"""
    )

    instance = cluster.add_instance(wait_online=True)

    info = instance.call(".proc_instance_info")
    assert info is not None


def test_conflict_old_and_new_iproto_config(cluster: Cluster, port_distributor: PortDistributor):
    """Test that mixing old iproto_* and new iproto section causes an error."""
    host = cluster.base_host
    iproto_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    peer:
        - {host}:{iproto_port}
    iproto_listen: {host}:{iproto_port}
    iproto:
        enabled: true
        listen: {host}:{iproto_port}
"""
    )

    instance = cluster.add_instance(wait_online=False)
    err = "cannot use both old iproto settings"
    crawler = log_crawler(instance, err)

    instance.fail_to_start()
    assert crawler.matched


def test_conflict_old_and_new_http_config(cluster: Cluster, port_distributor: PortDistributor):
    """Test that mixing old http_listen and new http section causes an error."""
    host = cluster.base_host
    http_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    http_listen: {host}:{http_port}
    http:
        enabled: true
        listen: {host}:{http_port}
"""
    )

    instance = cluster.add_instance(wait_online=False)
    err = "cannot use both old http settings"
    crawler = log_crawler(instance, err)

    instance.fail_to_start()
    assert crawler.matched


def test_conflict_old_and_new_pg_config(cluster: Cluster, port_distributor: PortDistributor):
    """Test that mixing old pg section and new pgproto section causes an error."""
    host = cluster.base_host
    pg_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    pg:
        listen: {host}:{pg_port}
    pgproto:
        enabled: true
        listen: {host}:{pg_port}
"""
    )

    instance = cluster.add_instance(wait_online=False)
    err = "cannot use both old pg settings"
    crawler = log_crawler(instance, err)

    instance.fail_to_start()
    assert crawler.matched


def test_plugin_listener_config(cluster: Cluster, port_distributor: PortDistributor):
    """Test the plugin listener configuration section."""
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
        test_plugin:
            service:
                test_service:
                    listener:
                        enabled: true
                        listen: {host}:{plugin_port}
                        advertise: {host}:{plugin_port}
"""
    )

    instance = cluster.add_instance(wait_online=True)

    info = instance.call(".proc_instance_info")
    assert info is not None


def test_plugin_listener_tls_config(cluster: Cluster, port_distributor: PortDistributor):
    """Test that plugin listener TLS configuration is parsed and accepted."""
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
        test_plugin:
            service:
                test_service:
                    listener:
                        enabled: true
                        listen: {host}:{plugin_port}
                        tls:
                            enabled: true
                            cert_file: {cert_file}
                            key_file: {key_file}
                            ca_file: {ca_file}
"""
    )

    instance = cluster.add_instance(wait_online=True)

    info = instance.call(".proc_instance_info")
    assert info is not None


def test_http_disabled(cluster: Cluster):
    """Test that http can be explicitly disabled using the new config."""
    cluster.set_config_file(
        yaml="""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    http:
        enabled: false
"""
    )

    instance = cluster.add_instance(wait_online=True)

    info = instance.call(".proc_instance_info")
    assert info is not None


def test_iproto_new_section_with_enabled_true_conflicts(cluster: Cluster, port_distributor: PortDistributor):
    """Test that old iproto_listen in config conflicts with --iproto-listen (sets new iproto.listen)."""
    host = cluster.base_host
    iproto_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    peer:
        - {host}:{iproto_port}
    iproto_listen: {host}:{iproto_port}
    iproto:
        enabled: true
"""
    )

    instance = cluster.add_instance(wait_online=False)
    err = "cannot use both old iproto settings"
    crawler = log_crawler(instance, err)

    instance.fail_to_start()
    assert crawler.matched


def test_iproto_new_section_with_enabled_false_conflicts(cluster: Cluster, port_distributor: PortDistributor):
    """Test that iproto.enabled=false + old iproto_listen triggers conflict.

    ADR: iproto cannot be disabled (required for picodata). The conflict between
    old iproto_listen and new iproto section fires first.
    """
    host = cluster.base_host
    iproto_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    peer:
        - {host}:{iproto_port}
    iproto_listen: {host}:{iproto_port}
    iproto:
        enabled: false
"""
    )

    instance = cluster.add_instance(wait_online=False)
    err = "cannot use both old iproto settings"
    crawler = log_crawler(instance, err)

    instance.fail_to_start()
    assert crawler.matched


def test_http_section_defaults_to_enabled(cluster: Cluster, port_distributor: PortDistributor):
    """Test that http section without enabled defaults to enabled."""
    host = cluster.base_host
    http_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    http:
        listen: {host}:{http_port}
        advertise: {host}:{http_port}
"""
    )

    instance = cluster.add_instance(wait_online=True)

    info = instance.call(".proc_runtime_info")
    assert "http" in info, "HTTP should be enabled by default when section is present"


def test_plugin_listener_section_defaults_to_enabled(cluster: Cluster, port_distributor: PortDistributor):
    """Test that plugin listener section without enabled defaults to enabled."""
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
        test_plugin:
            service:
                test_service:
                    listener:
                        listen: {host}:{plugin_port}
"""
    )

    instance = cluster.add_instance(wait_online=True)

    assert instance.process is not None


def test_pgproto_new_section_conflicts_with_old_pg(cluster: Cluster, port_distributor: PortDistributor):
    """Test pgproto new section conflicts with old pg section.

    The framework injects --pg-listen which sets new pgproto.listen.
    Config file uses old pg section, causing a conflict.
    """
    host = cluster.base_host
    pg_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    pg:
        listen: {host}:{pg_port}
        advertise: {host}:{pg_port}
"""
    )

    instance = cluster.add_instance(wait_online=False)
    err = "cannot use both old pg settings"
    crawler = log_crawler(instance, err)

    instance.fail_to_start()
    assert crawler.matched


def test_http_tls_with_password_file(cluster: Cluster, port_distributor: PortDistributor):
    """Test http section with TLS and password_file for encrypted private key.

    ADR http tls config supports password_file for encrypted keys.
    """
    ssl_dir = pathlib.Path(os.path.realpath(__file__)).parent.parent / "ssl_certs"
    cert_file = ssl_dir / "server-with-ext.crt"
    key_file = ssl_dir / "server-encrypted.key"
    password_file = ssl_dir / "server-password.txt"

    if not key_file.exists():
        import pytest

        pytest.skip("Encrypted key file not available")

    host = cluster.base_host
    http_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    http:
        enabled: true
        listen: {host}:{http_port}
        tls:
            enabled: true
            cert_file: {cert_file}
            key_file: {key_file}
            password_file: {password_file}
"""
    )

    instance = cluster.add_instance(wait_online=True)

    info = instance.call(".proc_instance_info")
    assert info is not None


def test_http_only_old_keys_starts(cluster: Cluster, port_distributor: PortDistributor):
    """Test that using only old http_listen key (without new http section) starts successfully.

    ADR http table row 2: only old keys present -> instance starts.
    """
    host = cluster.base_host
    http_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    http_listen: {host}:{http_port}
"""
    )

    instance = cluster.add_instance(wait_online=False)

    # prevent the test harness from setting the http port with the "new" config
    instance.http_port = None

    instance.start()
    instance.wait_online()

    info = instance.call(".proc_instance_info")
    assert info is not None


def test_http_no_config_uses_defaults(cluster: Cluster):
    """Test that omitting both old and new http config starts with defaults.

    ADR http table row 3: neither old nor new config present -> instance starts with defaults.
    """
    cluster.set_config_file(
        yaml="""
cluster:
    tier:
        default:
instance:
    cluster_name: test
"""
    )

    instance = cluster.add_instance(wait_online=True)

    info = instance.call(".proc_instance_info")
    assert info is not None
