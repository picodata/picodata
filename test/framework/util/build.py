import json
import os
import shlex
import subprocess

from framework.log import log
from framework.util.path import project_tests_path
from framework.util import copy_plugin_library
from framework.util import eprint
from framework.util import is_in_ci
from pathlib import Path


def rustc_target_triple() -> str:
    """
    Rust compiler target triple, e.g. `x86_64-unknown-linux-gnu`.
    """
    rustc_command = ["rustc", "--print", "host-tuple"]
    rustc_output = subprocess.check_output(rustc_command, text=True)
    return rustc_output.strip()


def cargo_build_profile() -> str:
    """
    Cargo build profile, e.g. "dev".
    """
    return os.environ.get("BUILD_PROFILE", "dev")


def perform_cargo_build(enable_webui: bool = False) -> None:
    """
    Runs `cargo build` with specific flags.
    In most cases, ran before starting tests.

    NOTE: skipped if running in CI.
    """

    # NOTE: this makes the logs prettier - let it rest here.
    eprint("")

    if is_in_ci():
        log.info(
            "Skipping `cargo build` as we are running in CI. "
            "We don't need to build it here, as it was built and cached in a separate job."
        )
        return

    cargo_flags = ["--all"]  # this should be included for Makefile to work correctly
    if enable_webui:
        cargo_flags += ["--features", "webui"]

    profile = cargo_build_profile()
    make_command = [
        "make",
        f"CARGO_FLAGS={shlex.join(cargo_flags)}",
        f"build-{profile}",
    ]
    log.info(f"Running `{make_command}` command...")
    subprocess.check_call(make_command)


def picodata_executable_path(copy_plugins: bool = True) -> Path:
    """
    Path to Picodata executable binary according to Cargo, e.g. `target/debug/picodata`.

    NOTE: this function also sets up plugin libraries needed for tests.
    """
    metadata_command = [
        "cargo",
        "metadata",
        "--format-version=1",
        "--frozen",
        "--no-deps",
    ]

    # STEP: collect Cargo metadata with possible target directory.

    raw_metadata = subprocess.check_output(metadata_command)
    serialized_metadata = json.loads(raw_metadata)
    cargo_target_directory = Path(serialized_metadata["target_directory"])

    # NOTE: metadata from Cargo is huge and to avoid printing all of its
    # contents in case of raised exception somewhere in this function we should
    # delete them from stack to avoid interpreter see them.
    del raw_metadata
    del serialized_metadata

    # STEP: determine valid Cargo build profile and path to it.

    # HACK: in order to disable sanitizers (e.g., ASan) for build.rs, we have to
    # pass `--target` or set `CARGO_BUILD_TARGET`, which will affect the path.
    cargo_build_target = os.environ.get("CARGO_BUILD_TARGET")
    if cargo_build_target is None and cargo_build_profile().startswith("asan"):
        cargo_target_directory /= rustc_target_triple()

    profile = cargo_build_profile()
    build_profile_directory = "debug" if profile == "dev" else profile
    cargo_profile_directory = cargo_target_directory / build_profile_directory

    # STEP: copy test plugin, which is a library, into
    # project tests directory, to make it usable in tests.
    # TODO: it's too messy to have all of these files being copied in one place.
    # We should just remove this and call `copy_plugin_library` in each
    # corresponding test directly. That way the tests would be more self
    # contained and this function would be much simpler.

    tests_path = project_tests_path() / "testplug"
    plugin_destinations = [
        f"{tests_path}/testplug/0.1.0",
        f"{tests_path}/testplug/0.2.0",
        f"{tests_path}/testplug/0.3.0",
        f"{tests_path}/testplug/0.4.0",
        f"{tests_path}/testplug_small/0.1.0",
        f"{tests_path}/testplug_small_svc2/0.1.0",
        f"{tests_path}/testplug_w_migration/0.1.0",
        f"{tests_path}/testplug_w_migration_2/0.1.0",
        f"{tests_path}/testplug_w_migration/0.2.0",
        f"{tests_path}/testplug_w_migration/0.2.0_changed",
        f"{tests_path}/testplug_sdk/0.1.0",
    ]
    if copy_plugins:
        for destination_directory in plugin_destinations:
            copy_plugin_library(cargo_profile_directory, destination_directory, "libtestplug")

    # STEP: determine Picodata executable binary path.

    return cargo_profile_directory / "picodata"
