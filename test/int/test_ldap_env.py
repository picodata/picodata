from conftest import Cluster, Instance
import pytest

TT_LDAP_URL = "ldap://localhost:1389"
TT_LDAP_DN_FMT = "cn=$USER,ou=users,dc=example,dc=org"


@pytest.fixture
def instance(cluster: Cluster):
    instance = cluster.add_instance(wait_online=False)
    instance.env["TT_LDAP_URL"] = TT_LDAP_URL
    instance.env["TT_LDAP_DN_FMT"] = TT_LDAP_DN_FMT
    instance.start()
    instance.wait_online()
    return instance


# Related: https://git.picodata.io/picodata/tarantool/-/issues/25
def test_ldap_env_variables(instance: Instance):
    res = instance.eval("return require('os').getenv('TT_LDAP_URL')")
    assert res == TT_LDAP_URL
    res = instance.eval("return require('os').getenv('TT_LDAP_DN_FMT')")
    assert res == TT_LDAP_DN_FMT
