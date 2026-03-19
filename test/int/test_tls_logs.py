"""
Tests to check how TLS listener configuration is printed to picodata logs
"""

import os
from pathlib import Path
from dataclasses import dataclass
from typing import List

import pytest

from conftest import Cluster, Instance, PortDistributor, log_crawler


SSL_DIR = Path(os.path.realpath(__file__)).parent.parent / "ssl_certs"

"""certificate details to be printed for server.crt"""
SERVER_CERT_DETAILS = """
    Serial number: 1BC5D097E837237DF0635E624F4D69C0C15E12F3
    Issuer:  commonName = "Intermediate CA"
    Subject: commonName = "example.com"
    Validity:
        Not before: Sep 20 01:18:15 2025 GMT
        Not after:  Aug 27 01:18:15 2125 GMT
"""

"""certificate details to be printed for server-fullchain.crt"""
SERVER_CERT_FULLCHAIN_DETAILS = """
    Serial number: 1BC5D097E837237DF0635E624F4D69C0C15E12F3
    Issuer:  commonName = "Intermediate CA"
    Subject: commonName = "example.com"
    Validity:
        Not before: Sep 20 01:18:15 2025 GMT
        Not after:  Aug 27 01:18:15 2125 GMT

    Serial number: 42EBA378101B3476D3E30AD22EF23FE7E5F54835
    Issuer:  commonName = "Root CA"
    Subject: commonName = "Intermediate CA"
    Validity:
        Not before: Sep 20 01:18:15 2025 GMT
        Not after:  Aug 27 01:18:15 2125 GMT

    Serial number: 0D622E76D1C39CF456ED74313731A92FB024C0D6
    Issuer:  commonName = "Root CA"
    Subject: commonName = "Root CA"
    Validity:
        Not before: Sep 20 01:18:15 2025 GMT
        Not after:  Aug 27 01:18:15 2125 GMT
"""

"""certificate details to be printed for server-with-ext.crt"""
SERVER_CERT_WITH_EXT_DETAILS = """
    Serial number: 44E6F281B2BFA783210DAC89A782BD511365CFD3
    Issuer:  commonName = "Intermediate CA"
    Subject: commonName = "pico_service@example.com"
    Validity:
        Not before: Oct  2 07:43:12 2025 GMT
        Not after:  Sep  8 07:43:12 2125 GMT
"""

"""certificate details to be printed for root-ca.crt"""
ROOT_CA_DETAILS = """
    Serial number: 0D622E76D1C39CF456ED74313731A92FB024C0D6
    Issuer:  commonName = "Root CA"
    Subject: commonName = "Root CA"
    Validity:
        Not before: Sep 20 01:18:15 2025 GMT
        Not after:  Aug 27 01:18:15 2125 GMT
"""

"""certificate details to be printed for combined-ca.crt"""
COMBINED_CA_DETAILS = """
    Serial number: 0D622E76D1C39CF456ED74313731A92FB024C0D6
    Issuer:  commonName = "Root CA"
    Subject: commonName = "Root CA"
    Validity:
        Not before: Sep 20 01:18:15 2025 GMT
        Not after:  Aug 27 01:18:15 2125 GMT

    Serial number: 42EBA378101B3476D3E30AD22EF23FE7E5F54835
    Issuer:  commonName = "Root CA"
    Subject: commonName = "Intermediate CA"
    Validity:
        Not before: Sep 20 01:18:15 2025 GMT
        Not after:  Aug 27 01:18:15 2125 GMT
"""


@dataclass
class CertSet:
    cert_file: Path
    key_file: Path
    ca_file: Path | None

    cert_details: str
    ca_details: str | None

    def make_crawlers(self, instance: Instance, protocol: str) -> List[log_crawler]:
        crawlers = []

        crawlers.append(log_crawler(instance, f"TLS({protocol}): server certificate chain: {self.cert_details}"))
        if self.ca_details:
            assert self.ca_file  # that would be silly
            crawlers.append(
                log_crawler(instance, f"TLS({protocol}): CA certificate chain is configured, mTLS will be enforced")
            )
            crawlers.append(log_crawler(instance, f"TLS({protocol}): mTLS CA certificate chain: {self.ca_details}"))
        else:
            crawlers.append(
                log_crawler(instance, f"TLS({protocol}): CA certificate chain is not configured, mTLS will be disabled")
            )

        return crawlers


# this certificate set includes the CA (because mTLS is mandatory for iproto) and uses `server-with-ext.crt`, which includes SAN.IP field
IPROTO_SET = CertSet(
    cert_file=SSL_DIR / "server-with-ext.crt",
    key_file=SSL_DIR / "server.key",
    ca_file=SSL_DIR / "combined-ca.crt",
    cert_details=SERVER_CERT_WITH_EXT_DETAILS,
    ca_details=COMBINED_CA_DETAILS,
)

"""Single server certificate, mTLS disabled"""
SINGLE_MTLS_DISABLED_SET = CertSet(
    cert_file=SSL_DIR / "server.crt",
    key_file=SSL_DIR / "server.key",
    ca_file=None,
    cert_details=SERVER_CERT_DETAILS,
    ca_details=None,
)

"""Server certificate with the full chain, mTLS disabled"""
FULLCHAIN_MTLS_DISABLED_SET = CertSet(
    cert_file=SSL_DIR / "server-fullchain.crt",
    key_file=SSL_DIR / "server.key",
    ca_file=None,
    cert_details=SERVER_CERT_FULLCHAIN_DETAILS,
    ca_details=None,
)

"""Single server certificate, mTLS configured with the combined CA file"""
SINGLE_MTLS_COMBINED_SET = CertSet(
    cert_file=SSL_DIR / "server.crt",
    key_file=SSL_DIR / "server.key",
    ca_file=SSL_DIR / "combined-ca.crt",
    cert_details=SERVER_CERT_DETAILS,
    ca_details=COMBINED_CA_DETAILS,
)

"""Server certificate with the full chain, mTLS configured with just the root CA file"""
FULLCHAIN_MTLS_ROOT_SET = CertSet(
    cert_file=SSL_DIR / "server-fullchain.crt",
    key_file=SSL_DIR / "server.key",
    ca_file=SSL_DIR / "root-ca.crt",
    cert_details=SERVER_CERT_FULLCHAIN_DETAILS,
    ca_details=ROOT_CA_DETAILS,
)

# TLS sets usable for non-iproto protocols
# (iproto is weird since it requires some extensions in the certificate metadata and only works with mTLS)
COMMON_TLS_SETS = [
    SINGLE_MTLS_DISABLED_SET,
    FULLCHAIN_MTLS_DISABLED_SET,
    SINGLE_MTLS_COMBINED_SET,
    FULLCHAIN_MTLS_ROOT_SET,
]


def test_iproto_tls_log(cluster: Cluster):
    set = IPROTO_SET

    assert set.ca_file is not None  # for typecheck to pass

    cluster.set_config_file(
        yaml=f"""
cluster:
    name: test
    tier:
        default:
instance:
    iproto:
        tls:
            enabled: true
            cert_file: {set.cert_file}
            key_file: {set.key_file}
            ca_file: {set.ca_file}
"""
    )

    (i1,) = cluster.deploy(instance_count=1, wait_online=False)

    # make the test harness aware of TLS settings, so that RPCs to the instance (like `wait_online`) work correctly
    i1.iproto_tls = (Path(set.cert_file), Path(set.key_file), Path(set.ca_file))

    lcs = set.make_crawlers(i1, "iproto")

    i1.start()
    i1.wait_online()

    assert all(lc.matched for lc in lcs)


@pytest.mark.parametrize("set", COMMON_TLS_SETS)
def test_pgproto_tls_log(cluster: Cluster, set: CertSet):
    cluster.set_config_file(
        yaml=f"""
cluster:
    name: test
    tier:
        default:
instance:
    pgproto:
        tls:
            enabled: true
            cert_file: {set.cert_file}
            key_file: {set.key_file}
            ca_file: {set.ca_file or "null"}
"""
    )

    (i1,) = cluster.deploy(instance_count=1, wait_online=False)

    lcs = set.make_crawlers(i1, "pgproto")

    i1.start()
    i1.wait_online()

    assert all(lc.matched for lc in lcs)


@pytest.mark.parametrize("set", COMMON_TLS_SETS)
def test_http_tls_log(cluster: Cluster, set: CertSet):
    cluster.set_config_file(
        yaml=f"""
cluster:
    name: test
    tier:
        default:
instance:
    http:
        tls:
            enabled: true
            cert_file: {set.cert_file}
            key_file: {set.key_file}
            ca_file: {set.ca_file or "null"}
"""
    )

    (i1,) = cluster.deploy(instance_count=1, wait_online=False)

    lcs = set.make_crawlers(i1, "http")

    i1.start()
    i1.wait_online()

    assert all(lc.matched for lc in lcs)


@pytest.mark.parametrize("set", COMMON_TLS_SETS)
def test_plugin_tls_log(cluster: Cluster, set: CertSet, port_distributor: PortDistributor):
    host = cluster.base_host
    plugin_port = port_distributor.get()

    cluster.set_config_file(
        yaml=f"""
cluster:
    name: test
    tier:
        default:
instance:

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
                            cert_file: {set.cert_file}
                            key_file: {set.key_file}
                            ca_file: {set.ca_file or "null"}
"""
    )

    (i1,) = cluster.deploy(instance_count=1, wait_online=False)

    lcs = set.make_crawlers(i1, "plugin testplug_listener.listenerservice")

    i1.start()
    i1.wait_online()

    # to make picodata print the plugin TLS configuration we have to install & enable plugin, so that it would create request a listener from picodata
    i1.call("pico.install_plugin", "testplug_listener", "0.1.0", timeout=10)

    i1.call(
        "pico.service_append_tier",
        "testplug_listener",
        "0.1.0",
        "listenerservice",
        "default",
    )

    i1.call("pico.enable_plugin", "testplug_listener", "0.1.0", timeout=10)

    assert all(lc.matched for lc in lcs)
