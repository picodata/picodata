from conftest import Instance


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
