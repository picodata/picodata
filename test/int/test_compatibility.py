import pytest
import os
import shutil


from conftest import (
    Cluster,
    Compatibility,
    ProcessDead,
    get_tt_snapshot_by_path,
    log_crawler,
)


@pytest.mark.xfail
def test_upgrade_major(cluster: Cluster):
    inst = cluster.add_instance(wait_online=False)
    os.makedirs(inst.data_dir, exist_ok=True)

    version, path = Compatibility().previous_tag()
    snapshot = get_tt_snapshot_by_path(path)
    assert (
        snapshot
    ), f"Snapshot of the previous MAJOR version was not found. Generate one using `make generate` on a previous MAJOR version. {version} is current version."  # noqa: E501

    shutil.copy(snapshot, f"{inst.data_dir}/")
    inst.start()
    inst.wait_online()


@pytest.mark.xfail
def test_upgrade_minor(cluster: Cluster):
    inst = cluster.add_instance(wait_online=False)
    os.makedirs(inst.data_dir, exist_ok=True)

    tag = Compatibility().previous_minor_tag()
    assert (
        tag
    ), "Current MINOR version is after MAJOR bump, so snapshot check with previous MINOR is not possible."  # noqa: E501
    version, path = tag

    snapshot = get_tt_snapshot_by_path(path)
    assert (
        snapshot
    ), f"Snapshot of the previous MINOR version was not found. Generate one using `make generate` on a previous MINOR version. {version} is current version."  # noqa: E501

    shutil.copy(snapshot, f"{inst.data_dir}/")
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
