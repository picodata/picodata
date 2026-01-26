from conftest import Cluster
import pytest


@pytest.mark.parametrize("value", ["true", "false", None])
def test_env_force_recovery(cluster: Cluster, value: str | None):
    instance = cluster.add_instance(wait_online=False)
    if value:
        instance.env["PICODATA_UNSAFE_FORCE_RECOVERY"] = value
    instance.start()
    instance.wait_online()
    cfg_value = instance.eval("return box.cfg.force_recovery")
    assert cfg_value == (True if value == "true" else False)
