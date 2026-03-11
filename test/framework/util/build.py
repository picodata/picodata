import functools
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
from typing import Any


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


def _cargo_metadata_json() -> dict[str, Any]:
    """
    Returns the parsed JSON output from `cargo metadata` command. The command is
    invoked with `--frozen` and `--no-deps` flags so that it never touches the
    network or resolves transitive dependencies. The result is cached for the
    lifetime of the process. Note that it is not cached, because it is private
    and called only once, but Cargo output is very-very verbose, be careful.
    """
    metadata_command = [
        "cargo",
        "metadata",
        "--format-version=1",
        "--frozen",
        "--no-deps",
    ]
    raw_metadata = subprocess.check_output(metadata_command)
    return json.loads(raw_metadata)


@functools.cache
def cargo_build_path() -> Path:
    """
    Returns the directory where Cargo places build artifacts for the current profile.
    Note that When an ASan profile is active and `CARGO_BUILD_TARGET` is not set, the
    host target triple is inserted into the path to mirror the layout Cargo uses when
    flag `--target` is passed.
    """
    cargo_metadata_dict = _cargo_metadata_json()
    cargo_target_directory = Path(cargo_metadata_dict["target_directory"])

    # HACK: in order to disable sanitizers (e.g., ASan) for build.rs, we have to
    # pass `--target` or set `CARGO_BUILD_TARGET`, which will affect the path.
    cargo_build_target = os.environ.get("CARGO_BUILD_TARGET")
    if cargo_build_target is None and cargo_build_profile().startswith("asan"):
        cargo_target_directory /= rustc_target_triple()

    profile = cargo_build_profile()
    build_profile_directory = "debug" if profile == "dev" else profile
    cargo_profile_directory = cargo_target_directory / build_profile_directory

    return cargo_profile_directory


def copy_testable_plugins() -> None:
    """
    Copy compiled test plugin shared libraries into the expected test fixture directories.
    Must be called after `perform_cargo_build` so that the compiled library already exists.
    """
    source_directory = cargo_build_path()
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
    for destination_directory in plugin_destinations:
        copy_plugin_library(source_directory, destination_directory, "libtestplug")


def picodata_executable_path() -> Path:
    """
    Path to Picodata executable binary according to Cargo, e.g. `target/debug/picodata`.
    """
    return cargo_build_path() / "picodata"
