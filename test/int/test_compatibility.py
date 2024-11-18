import pytest
import os
import shutil


from conftest import Cluster, Compatibility, log_crawler, ProcessDead


@pytest.mark.xfail
def test_upgrade(cluster: Cluster):
    inst = cluster.add_instance(wait_online=False)
    compat = Compatibility()

    os.makedirs(inst.data_dir, exist_ok=True)
    compat.fetch_previous_tag()

    print(f"snapshot tag: {compat._tag}")
    snap = compat.get_snapshot_path()
    err = "Snapshot of the previous MAJOR version was not found. Generate one using `make generate`."  # noqa: E501
    assert snap, err
    shutil.copy(snap, f"{inst.data_dir}/")

    inst.start()
    inst.wait_online()


def test_instances_of_incompatible_versions(cluster: Cluster):
    error_injection = "INCOMPATIBLE_PICODATA_VERSION"
    injection_log = f"ERROR INJECTION '{error_injection}'"

    i1 = cluster.add_instance(wait_online=False)
    i1.env[f"PICODATA_ERROR_INJECTION_{error_injection}"] = "1"
    i1.start()

    i2 = cluster.add_instance(wait_online=False)
    i2.env[f"PICODATA_ERROR_INJECTION_{error_injection}"] = "1"
    lc = log_crawler(i2, injection_log)

    i2.start()

    with pytest.raises(ProcessDead) as err:
        i2.wait_online()
        lc.wait_matched()

    assert err.value.args[0] == "process exited unexpectedly, exit_code=1"
