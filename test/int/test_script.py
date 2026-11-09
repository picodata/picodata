from conftest import Cluster, WAIT_ONLINE_TIMEOUT


def test_script_failure(cluster: Cluster):
    instance = cluster.add_instance(wait_online=False)
    script = f"{cluster.data_dir}/fail.lua"
    with open(script, "w") as f:
        f.write("assert(false)")
    instance.env["PICODATA_SCRIPT"] = script
    # Allow startup and the intentional panic's core dump to finish.
    instance.fail_to_start(timeout=WAIT_ONLINE_TIMEOUT, expected_log="failed to execute postjoin script")
    instance.terminate()


def test_script(cluster: Cluster):
    instance = cluster.add_instance(wait_online=False)
    script = f"{cluster.data_dir}/ok.lua"
    with open(script, "w") as f:
        f.write("assert(type(box.cfg) == 'table')")
    instance.env["PICODATA_SCRIPT"] = script
    instance.start()
    instance.wait_online()
