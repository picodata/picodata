from conftest import Cluster
import pytest


@pytest.mark.xfail
def test_script_failure(cluster: Cluster):
    instance = cluster.add_instance(wait_online=False)
    script = f"{cluster.data_dir}/fail.lua"
    with open(script, "w") as f:
        f.write("assert(false)")
    instance.env["PICODATA_SCRIPT"] = script
    instance.fail_to_start()
    instance.terminate()


def test_script(cluster: Cluster):
    instance = cluster.add_instance(wait_online=False)
    script = f"{cluster.data_dir}/ok.lua"
    with open(script, "w") as f:
        f.write("assert(type(box.cfg) == 'table')")
    instance.env["PICODATA_SCRIPT"] = script
    instance.start()
    instance.wait_online()
