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
    os.makedirs(inst.instance_dir, exist_ok=True)

    version, path = Compatibility().previous_tag()
    snapshot = get_tt_snapshot_by_path(path)
    assert (
        snapshot
    ), f"Snapshot of the previous MAJOR version was not found. Generate one using `make generate` on a previous MAJOR version. {version} is current version."  # noqa: E501

    shutil.copy(snapshot, f"{inst.instance_dir}/")
    inst.start()
    inst.wait_online()


@pytest.mark.xfail
def test_upgrade_minor(cluster: Cluster):
    inst = cluster.add_instance(wait_online=False)
    os.makedirs(inst.instance_dir, exist_ok=True)

    tag = Compatibility().previous_minor_tag()
    assert (
        tag
    ), "Current MINOR version is after MAJOR bump, so snapshot check with previous MINOR is not possible."  # noqa: E501
    version, path = tag

    snapshot = get_tt_snapshot_by_path(path)
    assert (
        snapshot
    ), f"Snapshot of the previous MINOR version was not found. Generate one using `make generate` on a previous MINOR version. {version} is current version."  # noqa: E501

    shutil.copy(snapshot, f"{inst.instance_dir}/")
    inst.start()
    inst.wait_online()


def test_instances_of_incompatible_versions(cluster: Cluster):
    error_injection = "UPDATE_PICODATA_VERSION"
    injection_log = f"ERROR INJECTION '{error_injection}'"

    i1 = cluster.add_instance(wait_online=False)
    i1.start()

    i2 = cluster.add_instance(wait_online=False)
    i2.env[f"PICODATA_ERROR_INJECTION_{error_injection}"] = "1"
    i2.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = "24.5.0-82-g79a5b6f0"
    lc = log_crawler(i2, injection_log)

    i2.start()
    with pytest.raises(ProcessDead) as err:
        i2.wait_online()
        lc.wait_matched()

    assert err.value.args[0] == "process exited unexpectedly, exit_code=1"


def test_instances_of_different_versions_in_cluster(cluster: Cluster):
    error_injection = "UPDATE_PICODATA_VERSION"
    injection_log = f"ERROR INJECTION '{error_injection}'"
    cluster.set_service_password("secret")

    i1 = cluster.add_instance(wait_online=False, init_replication_factor=4)
    i1.start()
    i1.wait_online()

    i2 = cluster.add_instance(wait_online=False)
    i2.env[f"PICODATA_ERROR_INJECTION_{error_injection}"] = "1"
    i2.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = "24.8.0-82-g79a5b6f0"
    lc = log_crawler(i2, injection_log)

    i2.start()
    i2.wait_online()
    lc.wait_matched()

    i3 = cluster.add_instance(wait_online=False)
    i3.env[f"PICODATA_ERROR_INJECTION_{error_injection}"] = "1"
    i3.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = "24.8.0-82-g79a5b6f0"
    lc = log_crawler(i3, injection_log)

    i3.start()
    i3.wait_online()
    lc.wait_matched()

    # At cluster boot _cluster_version is PICODATA_VERSION on all instances
    picodata_version = i1.call("box.space._pico_property:get", "cluster_version")[1]

    i1_version = i1.instance_info()["picodata_version"]
    i2_version = i2.instance_info()["picodata_version"]
    i3_version = i3.instance_info()["picodata_version"]

    assert i1_version == picodata_version
    assert i2_version != picodata_version
    assert i3_version != picodata_version

    # expel instance so remaining instances will have new version
    # and governor should update _cluster_version in _pico_property
    cluster.expel(i1, timeout=5)

    # After all instances in the cluster has a new version, _cluster_version should be changed
    # new version is the same on all instances since _pico_property is a global table
    new_picodata_version = i2.call("box.space._pico_property:get", "cluster_version")[1]
    assert i2_version == new_picodata_version
    assert i3_version == new_picodata_version
