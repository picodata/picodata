#!/usr/bin/env python3

import argparse
from contextlib import contextmanager
import glob
import os
from pathlib import Path
import subprocess  # nosec
import sys

REPO_DIR = Path(__file__).parent.parent
PATCHES_DIR = REPO_DIR / "certification_patches"
SVACE_PATCHES = PATCHES_DIR / "svace_patches"
GAMAYUN_PATCHES = PATCHES_DIR / "gamayun_patches"
TARANTOOL_SYS = REPO_DIR / "tarantool-sys"
THIRD_PARTY = TARANTOOL_SYS / "third_party"

DEBUG = os.getenv("DEBUG")

# List of files that need to be deleted before stat analysis
# Usually these are supplementary files from third party libraries
# like test data, utility scripts etc.
# This is needed because we cant use exclusion rules for static
# analysis tools so we need to remove unwanted files from the artifact
DEAD_LIST = [
    # from 6b2a88b551e6940089cf248d88b050b65ab67262
    "tarantool-sys/vendor/icu4c-71_1/source/python/icutools/databuilder/renderers/common_exec.py",
    # static build doesn't work without this file. We build dynamically, so it's fine
    "tarantool-sys/third_party/nghttp2/script/fetch-ocsp-response",
    "tarantool-sys/vendor/openssl-1.1.1q/fuzz/helper.py",
    "tarantool-sys/third_party/zstd/.circleci/images/primary/Dockerfile",
    # from 2be2aab5096b202de8bab72bafe41470c479895d
    "tarantool-sys/third_party/nghttp2/src/ca.nghttp2.org-key.pem",
    "tarantool-sys/third_party/nghttp2/src/ca.nghttp2.org.pem",
    "tarantool-sys/third_party/nghttp2/src/test.example.com-key.pem",
    "tarantool-sys/third_party/nghttp2/src/test.example.com.pem",
    "tarantool-sys/third_party/nghttp2/src/test.nghttp2.org-key.pem",
    "tarantool-sys/third_party/nghttp2/src/test.nghttp2.org.pem",
    "tarantool-sys/third_party/curl/packages/OS400/initscript.sh",
    "tarantool-sys/third_party/decNumber/example*.c",
    "tarantool-sys/third_party/libeio/install-sh",
    "tarantool-sys/third_party/libeio/ltmain.sh",
    "tarantool-sys/third_party/libev/depcomp",
    "tarantool-sys/third_party/libev/install-sh",
    "tarantool-sys/third_party/libev/ltmain.sh",
    "tarantool-sys/tools/gen-release-notes",
    "sbroad/docker-compose.yml",
    # further
    "tarantool-sys/third_party/luajit/src/luajit_lldb.py",
    "tarantool-sys/perf/lua/1mops_write.lua",
    "tarantool-sys/third_party/metrics/rpm/prebuild.sh",
]


@contextmanager
def cd(target: Path):
    old = os.getcwd()
    os.chdir(target)
    yield
    os.chdir(old)


def apply(patch: Path):
    subprocess.check_call(["git", "apply", str(patch)])  # nosec


def apply_from_dir(path: Path):
    for patch in path.iterdir():
        patch = patch.resolve()
        print("Applying:", patch)

        libname = patch.stem.split("_", maxsplit=1)[0]

        if libname == "tarantool-sys":
            if patch.stem.split("_", maxsplit=2)[1] == "small":
                with cd(TARANTOOL_SYS / "src" / "lib" / "small"):
                    apply(patch)
            else:
                with cd(TARANTOOL_SYS):
                    apply(patch)
        elif libname in ("http", "vshard"):
            with cd(libname):
                apply(patch)
        else:
            with cd(THIRD_PARTY / libname):
                apply(patch)


def apply_patches():
    print("For svace:")
    apply_from_dir(SVACE_PATCHES)

    print("For gamayun:")
    for glob_pattern in DEAD_LIST:
        for fname in glob.glob(glob_pattern):
            print("Removing:", REPO_DIR / fname)

            (REPO_DIR / fname).unlink(missing_ok=DEBUG)

    apply_from_dir(GAMAYUN_PATCHES)


def restore():
    subprocess.check_call(
        "git submodule foreach --recursive git restore .",
        shell=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Apply various certification induced transformations to source tree"
    )
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("apply", help="Prepare for analysis by applying patches")
    subparsers.add_parser(
        "restore",
        help="Restore all changes applied by 'svace' or(and) 'gamayun' commands",
    )

    commands = {
        "apply": apply_patches,
        "restore": restore,
    }

    args = parser.parse_args()
    command = commands.get(args.command)
    if command is None:
        print(f"Unknown command: {command}", file=sys.stderr)
        sys.exit(1)

    command()
