import pytest

from conftest import (
    Cluster,
    Compatibility,
    Instance,
    ProcessDead,
    Retriable,
    log_crawler,
    copy_dir,
)


@pytest.mark.xdist_group(name="compat")
def test_upgrade_minor(compat_instance: Instance):
    """
    Tests backward compatibility with the previous MINOR version's latest PATCH.

    This test verifies that the current version can properly start from recovery
    files of the latest PATCH version of the previous MINOR release `X.(Y-1).*`.
    For example, version `25.3.0` would test against `25.2.2`.

    The test serves two purposes:
    1. Ensures MINOR version boot works correctly.
    2. Implicitly covers PATCH compatibility when there is no previous PATCH version
       (e.g., for `X.Y.0`, since no `X.Y.-1` exists).

    It only runs when there exists a recovery files of previous MINOR version to test against.
    """
    compatibility = Compatibility()

    backup_files = compatibility.previous_minor_path
    copy_dir(backup_files, compat_instance.instance_dir)

    compat_instance.start()
    compat_instance.wait_online()


@pytest.mark.xdist_group(name="compat")
@pytest.mark.skip(reason="temporary skip, see https://git.picodata.io/core/picodata/-/jobs/566329")
def test_upgrade_patch(compat_instance: Instance):
    """
    Tests backward compatibility with the previous PATCH version.

    This test verifies that the current version can properly start from recovery
    files of the previous PATCH version `X.Y.(Z-1)`. It only runs when:
    - The current version has a PATCH number > 0 (e.g., `25.3.1` would test against `25.3.0`).
    - There exists a recovery files of previous PATCH version to test against.
    """
    compatibility = Compatibility()

    current_version = compatibility.current_tag_version
    if current_version.micro == 0:
        pytest.skip("newly released minor versions are not testable against previous patch versions")

    backup_files = compatibility.previous_patch_path
    copy_dir(backup_files, compat_instance.instance_dir)

    compat_instance.start()
    compat_instance.wait_online()


def test_instances_of_incompatible_versions(cluster: Cluster):
    error_injection = "UPDATE_PICODATA_VERSION"
    injection_log = f"ERROR INJECTION '{error_injection}'"

    i1 = cluster.add_instance(wait_online=False)
    i1.start()
    i1.wait_online()

    def upgrade_to_old_version(version):
        major = int(version.split(".")[0])
        minor = int(version.split(".")[1]) - 2
        if minor <= 0:
            major -= 1
            minor = 1
        return f"{major}.{minor}.0-xxxx"

    picodata_version = i1.call("box.space._pico_property:get", "cluster_version")[1]
    old_version = upgrade_to_old_version(picodata_version)

    i2 = cluster.add_instance(wait_online=False)
    i2.env[f"PICODATA_ERROR_INJECTION_{error_injection}"] = "1"
    i2.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = old_version
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

    def upgrade_to_next_minor_version(version):
        major = int(version.split(".")[0])
        minor = int(version.split(".")[1]) + 1
        return f"{major}.{minor}.0-xxxx"

    picodata_version = i1.call("box.space._pico_property:get", "cluster_version")[1]
    next_minor_version = upgrade_to_next_minor_version(picodata_version)

    i2 = cluster.add_instance(wait_online=False)
    i2.env[f"PICODATA_ERROR_INJECTION_{error_injection}"] = "1"
    i2.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = next_minor_version
    lc = log_crawler(i2, injection_log)

    i2.start()
    i2.wait_online()
    lc.wait_matched()

    i3 = cluster.add_instance(wait_online=False)
    i3.env[f"PICODATA_ERROR_INJECTION_{error_injection}"] = "1"
    i3.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = next_minor_version
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
    cluster.expel(i1, timeout=5, force=True)

    # After all instances in the cluster has a new version, _cluster_version should be changed
    # new version is the same on all instances since _pico_property is a global table
    def ensure_new_version():
        new_picodata_version = i2.call("box.space._pico_property:get", "cluster_version")[1]
        assert new_picodata_version == next_minor_version

    Retriable(timeout=5).call(ensure_new_version)

    new_picodata_version = i2.call("box.space._pico_property:get", "cluster_version")[1]
    assert i2_version == new_picodata_version
    assert i3_version == new_picodata_version

    assert new_picodata_version == next_minor_version
