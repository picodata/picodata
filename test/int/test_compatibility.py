from conftest import Cluster
from conftest import Instance
from framework.util import ExpectedError


def set_picodata_version(instance: Instance, version: str):
    instance.env["PICODATA_ERROR_INJECTION_UPDATE_PICODATA_VERSION"] = "1"
    instance.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = version


def test_major_upgrade_ok(unstarted_instance: Instance):
    i = unstarted_instance
    i.env["PICODATA_ERROR_INJECTION_BOOT_PICODATA_VERSION"] = "25.5.7"
    i.env["PICODATA_ERROR_INJECTION_UPDATE_PICODATA_VERSION"] = "1"
    i.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = "25.5.7"
    i.start_and_wait()
    [[res]] = i.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
    assert res == "25.5.7"
    i.terminate()

    i.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = "26.1.0"
    i.start_and_wait()
    [[res]] = i.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
    assert res == "26.1.0"


def test_major_upgrade_error(unstarted_instance: Instance):
    i = unstarted_instance
    i.env["PICODATA_ERROR_INJECTION_BOOT_PICODATA_VERSION"] = "25.5.7"
    i.env["PICODATA_ERROR_INJECTION_UPDATE_PICODATA_VERSION"] = "1"
    i.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = "25.5.7"
    i.start_and_wait()
    [[res]] = i.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
    assert res == "25.5.7"
    i.terminate()

    i.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = "26.2.0"
    i.fail_to_start()

    i.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = "27.1.0"
    i.fail_to_start()

    i.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = "24.1.0"
    i.fail_to_start()

    i.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = "25.4.9"
    i.fail_to_start()


def test_minor_upgrade_ok(unstarted_instance: Instance):
    i = unstarted_instance
    i.env["PICODATA_ERROR_INJECTION_BOOT_PICODATA_VERSION"] = "25.4.4"
    i.env["PICODATA_ERROR_INJECTION_UPDATE_PICODATA_VERSION"] = "1"
    i.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = "25.4.4"
    i.start_and_wait()
    [[res]] = i.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
    assert res == "25.4.4"
    i.terminate()

    i.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = "25.5.5"
    i.start_and_wait()
    [[res]] = i.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
    assert res == "25.5.5"


def test_minor_upgrade_error(unstarted_instance: Instance):
    i = unstarted_instance
    i.env["PICODATA_ERROR_INJECTION_BOOT_PICODATA_VERSION"] = "25.3.3"
    i.env["PICODATA_ERROR_INJECTION_UPDATE_PICODATA_VERSION"] = "1"
    i.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = "25.3.3"
    i.start_and_wait()
    [[res]] = i.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
    assert res == "25.3.3"
    i.terminate()

    i.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = "25.5.5"
    i.fail_to_start()

    i.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = "25.2.2"
    i.fail_to_start()


# A rolling upgrade may contain no more than two Picodata major-minor pairs.
#
# Start with 25.5 and 26.0, then verify that 26.1 is rejected both when a new
# instance joins and when an existing 25.5 instance restarts. Two 25.5 instances
# are needed so that restarting one of them still leaves 25.5 in the cluster and
# would therefore introduce a third pair.
#
# Comparing only with `cluster_version` would miss both cases because 25.5 is
# individually compatible with 26.0 and 26.1. The check must instead consider
# the resulting set of versions across all retained instances.
#
# See <https://git.picodata.io/core/picodata/-/work_items/978>.
def test_reject_third_minor_version_on_join_and_restart(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i1.env["PICODATA_ERROR_INJECTION_BOOT_PICODATA_VERSION"] = "25.5.7"
    set_picodata_version(i1, "25.5.7")
    i1.start_and_wait()

    i2 = cluster.add_instance(wait_online=False)
    set_picodata_version(i2, "26.0.0")
    i2.start_and_wait()

    i3 = cluster.add_instance(wait_online=False)
    set_picodata_version(i3, "25.5.8")
    i3.start_and_wait()

    error = ExpectedError(log_pattern="more than two Picodata minor versions")

    joining = cluster.add_instance(wait_online=False)
    set_picodata_version(joining, "26.1.0")
    joining.fail_to_start(error=error)

    i3.terminate()
    set_picodata_version(i3, "26.1.0")
    i3.fail_to_start(error=error)


def test_patch_upgrade_ok(unstarted_instance: Instance):
    i = unstarted_instance
    i.env["PICODATA_ERROR_INJECTION_BOOT_PICODATA_VERSION"] = "25.5.7"
    i.env["PICODATA_ERROR_INJECTION_UPDATE_PICODATA_VERSION"] = "1"
    i.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = "25.5.7"
    i.start_and_wait()
    [[res]] = i.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
    assert res == "25.5.7"
    i.terminate()

    i.env["PICODATA_INTERNAL_VERSION_OVERRIDE"] = "25.5.8"
    i.start_and_wait()
    [[res]] = i.sql("SELECT value FROM _pico_property WHERE key = 'cluster_version'")
    assert res == "25.5.8"
