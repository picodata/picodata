#!/usr/bin/env python3

# NOTE(kbezuglyi):
#
# * This script is used for tests on rolling upgrades.
#   It downloads Picodata executables of needed version for the CI.
#
# * It is not possible to use only `dnf`/`rpm`/`yum` with our repository, as these
#   package managers do not allow installing two different versions of the same
#   software alongside each other.
#
# * Even though these executables are quite large, as long as we cache them
#   throughout the CI, it is a considerable trade-off.

import re
import os
import shutil
import subprocess
import sys
import tempfile

from packaging.version import Version
from pathlib import Path
from subprocess import CalledProcessError


VERSIONS_FILE = Path("required_rolling_versions.txt")
INSTALL_DIR = Path(os.environ["ROLLING_BINARIES_DIR"])
RPM_PICODATA_BIN = Path("usr/bin/picodata")
RPM_VERSION_PATTERN = r"^picodata-(\d+\.\d+\.\d+)"


def extract_version(rpm: Path) -> Version:
    match = re.match(RPM_VERSION_PATTERN, rpm.name)
    if match is None:
        error = "Could not extract version from RPM"
        hint = f"with filename: {rpm.name!r}"
        raise RuntimeError(f"{error} {hint}")

    version = match.group(1)
    return Version(version)


def download_rpm(version: Version, dest: Path) -> Path:
    subprocess.check_call(["dnf", "download", f"picodata-{version}.*"], cwd=dest)

    rpms = list(dest.glob(f"picodata-{version}*.rpm"))
    if len(rpms) != 1:
        error = f"Expected exactly one RPM"
        hint = f"for version {version!r}, found: {rpms}"
        raise RuntimeError(f"{error} {hint}")

    return rpms[0]


def extract_archive(rpm: Path, dest: Path) -> None:
    rpm2cpio = subprocess.Popen(
        ["rpm2cpio", str(rpm)],
        stdout=subprocess.PIPE,
    )
    subprocess.check_call(
        ["cpio", "-idm"],
        stdin=rpm2cpio.stdout,
        cwd=dest,
    )
    rpm2cpio.wait()
    # NOTE(kbezuglyi): When we transit to EL9, consider this bug:
    # <https://bugzilla.redhat.com/show_bug.cgi?id=2058426#c3>.
    if rpm2cpio.returncode != 0:
        raise CalledProcessError(rpm2cpio.returncode, "rpm2cpio")


def install_binary(version: Version) -> None:
    with tempfile.TemporaryDirectory(prefix=f"picodata-{version}-") as tmpdir:
        tmp = Path(tmpdir)

        rpm = download_rpm(version, tmp)
        version = extract_version(rpm)
        extract_archive(rpm, tmp)

        name = f"picodata-{version}"
        src = tmp / RPM_PICODATA_BIN
        dst = INSTALL_DIR / name
        shutil.move(src, dst)

        print(f"Installed {name!r} to {dst.absolute!r}")


def load_versions(file: Path) -> list[Version]:
    if not file.exists():
        error = f"{VERSIONS_FILE.absolute!r} was not found but is required"
        hint = "Did you run `make collect-required-rolling-versions`?"
        raise RuntimeError(f"{error}. {hint}")

    versions = [Version(line) for line in file.read_text().splitlines() if line.strip()]
    if not versions:
        error = f"{VERSIONS_FILE.absolute!r} is empty but is required"
        hint = "This is unexpected. Is something wrong with the setup?"
        raise RuntimeError(f"{error}. {hint}")

    return versions


def main():
    INSTALL_DIR.mkdir(parents=True, exist_ok=True)
    for version in load_versions(VERSIONS_FILE):
        install_binary(version)


if __name__ == "__main__":
    main()
