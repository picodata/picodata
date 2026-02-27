#!/usr/bin/env python3

import argparse
from contextlib import contextmanager
import glob
import os
from pathlib import Path
import shlex
import shutil
import subprocess  # nosec
import sys
import fnmatch

REPO_DIR = Path(__file__).parent.parent
PATCHES_DIR = REPO_DIR / "certification" / "patches"
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
    # further
    "tarantool-sys/third_party/luajit/src/luajit_lldb.py",
    "tarantool-sys/perf/lua/1mops_write.lua",
    "tarantool-sys/third_party/metrics/rpm/prebuild.sh",
    # 25.2
    "docker/docker-compose.yml",
    "tarantool-sys/extra/pico_parse_git_log.py",
    "tarantool-sys/vendor/openssl-3.2.3/fuzz/helper.py",
    "tools/get_tags.py",
    "tarantool-sys/third_party/luajit/src/luajit-gdb.py",
    "tarantool-sys/third_party/nghttp2/author.py",
    "tarantool-sys/third_party/nghttp2/genlibtokenlookup.py",
    "tarantool-sys/third_party/nghttp2/help2rst.py",
    "tarantool-sys/third_party/nghttp2/mkhufftbl.py",
    "tarantool-sys/third_party/zstd/build/meson/GetZstdLibraryVersion.py",
    "tarantool-sys/third_party/zstd/build/single_file_libs/combine.py",
    "tarantool-sys/third_party/zstd/contrib/freestanding_lib/freestanding.py",
    "tarantool-sys/tools/tarantool-gdb.py",
    "tarantool-sys/vendor/icu4c-71_1/source/data/BUILDRULES.py",
    "tarantool-sys/vendor/icu4c-71_1/source/python/icutools/databuilder/__main__.py",
    "tarantool-sys/vendor/icu4c-71_1/source/python/icutools/databuilder/comment_stripper.py",
    "tarantool-sys/vendor/icu4c-71_1/source/python/icutools/databuilder/filtration.py",
    "tarantool-sys/vendor/icu4c-71_1/source/python/icutools/databuilder/renderers/makefile.py",
    "tarantool-sys/vendor/icu4c-71_1/source/python/icutools/databuilder/request_types.py",
    "tarantool-sys/vendor/icu4c-71_1/source/python/icutools/databuilder/utils.py",
    "tools/flaky_finder.py",
    "tools/plan-drawer/draw.py",
    "webui/dev/webui_mock_large_cluster/test_webui_mock_cluster.py",
    # 26.1
    'benchmark/batched-inserts/README.md',
    'benchmark/block/README.md',
    'benchmark/tpcb/README.md',
    'benchmark/tpch/README.md',
    'certification/patches/README.md',
    'CHANGELOG.md',
    'CONTRIBUTING.md',
    'doc/versioning.md',
    'doc/adr',
    'doc/dev',
    'docs/',
    '.gitlab/issue_templates/',
    '.gitlab/merge_request_templates/',
    'http/CHANGELOG.md',
    'http/debian/docs',
    'http/.github/',
    'http/README.md',
    'http/test/',
    'monitoring/README.md',
    'picodata-plugin/README.md',
    'sql-planner/doc/',
    'sql-planner/src/backend/sql/ir/tests/',
    'sql-planner/src/executor/tests/',
    'sql-planner/src/frontend/sql/ir/tests/',
    'sql-planner/src/ir/explain/tests/',
    'sql-planner/src/ir/transformation/redistribution/tests/',
    'sql-planner/tests/',
    'tarantool/CHANGELOG.md',
    'tarantool/examples/',
    'tarantool/.gitlab/issue_templates/',
    'tarantool/.gitlab/merge_request_templates/',
    'tarantool/README.md',
    'tarantool-sys/changelogs/',
    'tarantool-sys/CONTRIBUTING.md',
    'tarantool-sys/doc/',
    'tarantool-sys/.github/',
    'tarantool-sys/.gitlab/merge_request_templates/',
    'tarantool-sys/perf/gh-7089-vclock-copy/Readme.md',
    'tarantool-sys/README.md',
    'tarantool-sys/src/box/sql/in-operator.md',
    'tarantool-sys/src/lib/msgpuck/debian/docs',
    'tarantool-sys/src/lib/msgpuck/README.md',
    'tarantool-sys/src/lib/msgpuck/test/',
    'tarantool-sys/src/lib/small/debian/docs',
    'tarantool-sys/src/lib/small/doc/',
    'tarantool-sys/src/lib/small/perf/README.md',
    'tarantool-sys/src/lib/small/README.md',
    'tarantool-sys/src/lib/small/third_party/README.md',
    'tarantool-sys/static-build/README.md',
    'tarantool-sys/test/',
    'tarantool-sys/test-run',
    'tarantool-sys/third_party/c-ares/CONTRIBUTING.md',
    'tarantool-sys/third_party/c-ares/INSTALL.md',
    'tarantool-sys/third_party/c-ares/RELEASE-PROCEDURE.md',
    'tarantool-sys/third_party/c-ares/SECURITY.md',
    'tarantool-sys/third_party/c-ares/test/',
    'tarantool-sys/third_party/c-dt/.github/',
    'tarantool-sys/third_party/checks/CHANGELOG.md',
    'tarantool-sys/third_party/checks/debian/docs',
    'tarantool-sys/third_party/checks/.github/',
    'tarantool-sys/third_party/checks/README.md',
    'tarantool-sys/third_party/checks/test/',
    'tarantool-sys/third_party/curl/CHANGES.md',
    'tarantool-sys/third_party/curl/docs/',
    'tarantool-sys/third_party/curl/.github/',
    'tarantool-sys/third_party/curl/GIT-INFO.md',
    'tarantool-sys/third_party/curl/include/README.md',
    'tarantool-sys/third_party/curl/packages/README.md',
    'tarantool-sys/third_party/curl/projects/README.md',
    'tarantool-sys/third_party/curl/README.md',
    'tarantool-sys/third_party/curl/SECURITY.md',
    'tarantool-sys/third_party/curl/tests/',
    'tarantool-sys/third_party/curl/winbuild/README.md',
    'tarantool-sys/third_party/decNumber/example1.c',
    'tarantool-sys/third_party/decNumber/example2.c',
    'tarantool-sys/third_party/decNumber/example3.c',
    'tarantool-sys/third_party/decNumber/example4.c',
    'tarantool-sys/third_party/decNumber/example5.c',
    'tarantool-sys/third_party/decNumber/example6.c',
    'tarantool-sys/third_party/decNumber/example7.c',
    'tarantool-sys/third_party/decNumber/example8.c',
    'tarantool-sys/third_party/decNumber/ICU-license.html',
    'tarantool-sys/third_party/libunwind/README.md',
    'tarantool-sys/third_party/libyaml/doc/',
    'tarantool-sys/third_party/libyaml/examples/',
    'tarantool-sys/third_party/luafun/CONTRIBUTING.md',
    'tarantool-sys/third_party/luafun/COPYING.md',
    'tarantool-sys/third_party/luafun/doc/',
    'tarantool-sys/third_party/luafun/HACKING.md',
    'tarantool-sys/third_party/luafun/README.md',
    'tarantool-sys/third_party/luafun/tests/',
    'tarantool-sys/third_party/luajit/doc/',
    'tarantool-sys/third_party/luajit/.github/',
    'tarantool-sys/third_party/lua/README-luadebug.md',
    'tarantool-sys/third_party/luarocks/CHANGELOG.md',
    'tarantool-sys/third_party/luarocks/CODE_OF_CONDUCT.md',
    'tarantool-sys/third_party/luarocks/.github/',
    'tarantool-sys/third_party/luarocks/README.md',
    'tarantool-sys/third_party/luarocks/SECURITY.md',
    'tarantool-sys/third_party/luarocks/spec/fixtures/git_repo/README.md',
    'tarantool-sys/third_party/luarocks/spec/README.md',
    'tarantool-sys/third_party/luazip/doc/',
    'tarantool-sys/third_party/luazip/tests/',

    # without tarantool-sys/third_party/lua-zlib/lua_zlib.c do not work clippy
    'tarantool-sys/third_party/lua-zlib/CMakeLists.txt',
    'tarantool-sys/third_party/lua-zlib/Makefile',
    'tarantool-sys/third_party/lua-zlib/README',
    'tarantool-sys/third_party/lua-zlib/amnon_david.gz',
    'tarantool-sys/third_party/lua-zlib/cmake/Modules/FindLuaJIT.cmake',
    'tarantool-sys/third_party/lua-zlib/lua-zlib-1.2-0.rockspec',
    'tarantool-sys/third_party/lua-zlib/tap.lua',
    'tarantool-sys/third_party/lua-zlib/test.lua',
    'tarantool-sys/third_party/lua-zlib/tom_macwright.gz',
    'tarantool-sys/third_party/lua-zlib/tom_macwright.out',
    'tarantool-sys/third_party/lua-zlib/zlib.def',
    'tarantool-sys/third_party/metrics/CHANGELOG.md',
    'tarantool-sys/third_party/metrics/doc/',
    'tarantool-sys/third_party/metrics/example/',
    'tarantool-sys/third_party/metrics/.github/',
    'tarantool-sys/third_party/metrics/README.md',
    'tarantool-sys/third_party/metrics/test/',
    'tarantool-sys/third_party/nghttp2/.github/',
    'tarantool-sys/third_party/tz/theory.html',
    'tarantool-sys/third_party/tz/tz-art.html',
    'tarantool-sys/third_party/tz/tz-how-to.html',
    'tarantool-sys/third_party/tz/tz-link.html',
    'tarantool-sys/third_party/xxHash/cmake_unofficial/README.md',
    'tarantool-sys/third_party/xxHash/doc/',
    'tarantool-sys/third_party/xxHash/README.md',
    'tarantool-sys/third_party/xxHash/tests/',
    'tarantool-sys/third_party/xxHash/xxhsum.1.md',
    'tarantool-sys/third_party/zstd/build/cmake/README.md',
    'tarantool-sys/third_party/zstd/build/cmake/tests/',
    'tarantool-sys/third_party/zstd/build/meson/README.md',
    'tarantool-sys/third_party/zstd/build/meson/tests/',
    'tarantool-sys/third_party/zstd/build/README.md',
    'tarantool-sys/third_party/zstd/build/single_file_libs/examples/',
    'tarantool-sys/third_party/zstd/build/single_file_libs/README.md',
    'tarantool-sys/third_party/zstd/build/VS_scripts/README.md',
    'tarantool-sys/third_party/zstd/CODE_OF_CONDUCT.md',
    'tarantool-sys/third_party/zstd/contrib/docker/README.md',
    'tarantool-sys/third_party/zstd/contrib/externalSequenceProducer/README.md',
    'tarantool-sys/third_party/zstd/contrib/freestanding_lib/',
    'tarantool-sys/third_party/zstd/contrib/gen_html/README.md',
    'tarantool-sys/third_party/zstd/contrib/largeNbDicts/README.md',
    'tarantool-sys/third_party/zstd/contrib/linux-kernel/README.md',
    'tarantool-sys/third_party/zstd/contrib/linux-kernel/test/',
    'tarantool-sys/third_party/zstd/contrib/match_finders/README.md',
    'tarantool-sys/third_party/zstd/contrib/pzstd/README.md',
    'tarantool-sys/third_party/zstd/contrib/pzstd/test/',
    'tarantool-sys/third_party/zstd/contrib/pzstd/utils/test/',
    'tarantool-sys/third_party/zstd/contrib/seekable_format/examples/',
    'tarantool-sys/third_party/zstd/contrib/seekable_format/README.md',
    'tarantool-sys/third_party/zstd/contrib/seekable_format/tests/',
    'tarantool-sys/third_party/zstd/contrib/seekable_format/zstd_seekable_compression_format.md',
    'tarantool-sys/third_party/zstd/CONTRIBUTING.md',
    'tarantool-sys/third_party/zstd/contrib/VS2005/README.md',
    'tarantool-sys/third_party/zstd/doc/',
    'tarantool-sys/third_party/zstd/examples/',
    'tarantool-sys/third_party/zstd/.github/',
    'tarantool-sys/third_party/zstd/lib/dll/',
    'tarantool-sys/third_party/zstd/lib/README.md',
    'tarantool-sys/third_party/zstd/programs/README.md',
    'tarantool-sys/third_party/zstd/programs/zstd.1.md',
    'tarantool-sys/third_party/zstd/programs/zstdgrep.1.md',
    'tarantool-sys/third_party/zstd/programs/zstdless.1.md',
    'tarantool-sys/third_party/zstd/README.md',
    'tarantool-sys/third_party/zstd/SECURITY.md',
    'tarantool-sys/third_party/zstd/TESTING.md',
    'tarantool-sys/third_party/zstd/tests/',
    'tarantool-sys/third_party/zstd/zlibWrapper/examples/',
    'tarantool-sys/third_party/zstd/zlibWrapper/README.md',
    'tarantool/tests/',
    'tarantool/tlua/README.md',
    'test/conftest.py',
    'test/framework/',
    'test/https_certs/',
    'test/inner.rs',
    'test/int/',
    'test/known_defects/',
    'test/manual/',
    'test/pgproto/',
    'test/plug_wrong_version/',
    'test/sqlite_tests_converter.py',
    'test/ssl_certs/',
    'tools/plan-drawer/',
    'vshard/changelogs/',
    'vshard/debian/docs',
    'vshard/docs/',
    'vshard/example/',
    'vshard/.github/',
    'vshard/README.md',
    'vshard/test/',
    'vshard/test-run',
    'vshard/test-run/',
    'webui/dev/webui_mock_large_cluster/README.md'
]


def get_submodules():
    result = subprocess.run(
        ['git', 'submodule', 'status'],
        capture_output=True, text=True, check=True
    )
    return [Path(line.split()[1]) for line in result.stdout.strip().split('\n') if line ]

def get_deleted_files():
    result = subprocess.run(
        ['git', 'ls-files', '--deleted'],
        capture_output=True, text=True, check=True
    )
    return [Path(f) for f in result.stdout.strip().split('\n') if f]


@contextmanager
def cd(target: Path):
    old = os.getcwd()
    os.chdir(target)
    yield
    os.chdir(old)


def apply(patch: Path, reverse = False):
    reverse_check_result = subprocess.run(["git", "apply", "--reverse" , "--check", str(patch)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) # nosec
    forward_check_result = subprocess.run(["git", "apply", "--check", str(patch)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) # nosec
    
    if reverse:
        if reverse_check_result.returncode != 0 and forward_check_result.returncode == 0:
            print("Patch already reversed: {}".format(patch))
            return
        subprocess.check_call(["git", "apply", "--reverse", "--reject", str(patch)])  # nosec
    else:
        if reverse_check_result.returncode == 0 and forward_check_result.returncode != 0:
            print("Patch already applied: {}".format(patch))
            return
        subprocess.check_call(["git", "apply", "--reject", str(patch)])  # nosec

    
def apply_from_dir(path: Path, reverse = False):
    for patch in path.iterdir():
        patch = patch.resolve()
        if reverse:
            print("Reversing:", patch)
        else:
            print("Applying:", patch)

        libname = patch.stem.split("_", maxsplit=1)[0]

        if libname == "tarantool-sys":
            if patch.stem.split("_", maxsplit=2)[1] == "small":
                with cd(TARANTOOL_SYS / "src" / "lib" / "small"):
                    apply(patch, reverse)
            else:
                with cd(TARANTOOL_SYS):
                    apply(patch, reverse)
        elif libname in ("http", "vshard"):
            with cd(libname):
                apply(patch, reverse)
        else:
            with cd(THIRD_PARTY / libname):
                apply(patch, reverse)



def remove_files():
    print("Removing files:")
    for glob_pattern in DEAD_LIST:
        for fname in glob.glob(glob_pattern, root_dir=REPO_DIR):
            p = Path(REPO_DIR / fname)
            if p.exists():
                if p.is_dir():
                    print(f"Removing directory: {p}")
                    shutil.rmtree(p)
                else:
                    print(f"Removing file: {p}")
                    p.unlink()



def restore_files(module):
    print(f"Restoring files in {module}")
    for deleted_file_path in get_deleted_files():
        root_deleted_file_path = module / deleted_file_path
        for pattern in DEAD_LIST:
            if root_deleted_file_path.is_relative_to(pattern):
                print(f"restoring file matched by prefix {pattern}: {root_deleted_file_path}")
                subprocess.run(['git', 'checkout', '--', deleted_file_path], check=True)
                break
            elif fnmatch.fnmatch(root_deleted_file_path, pattern):
                print(f"restoring file matched by pattern {pattern}: {root_deleted_file_path}")
                subprocess.run(['git', 'checkout', '--', deleted_file_path], check=True)
                break

    for submodule in get_submodules():
        if submodule.exists() and any(submodule.iterdir()):
            with cd(submodule):
                restore_files(module/submodule)
        else:
            print(f"restoring module {submodule}")
            #subprocess.run(['git', 'checkout', '--', submodule], check=True)
            subprocess.run(['git', 'submodule', 'update', '--init', '--recursive', '--', submodule], check=True)

        


def apply_patches_and_delete():
    print("Applying patches for svace:")
    apply_from_dir(SVACE_PATCHES)
    print("Applying patches for gamayun (sonarqube):")
    apply_from_dir(GAMAYUN_PATCHES)
    remove_files()


def restore():
   # subprocess.check_call(
   #     shlex.split("git submodule foreach --recursive git restore .")
   #)  # nosec
    restore_files(Path('.'))
    print("Reversing patches for svace:")
    apply_from_dir(SVACE_PATCHES, True)
    print("Reversing patches for gamayun (sonarqube):")
    apply_from_dir(GAMAYUN_PATCHES, True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Apply various certification induced transformations to source tree"
    )
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("apply", help="Prepare for analysis by applying patches and removing files")
    subparsers.add_parser(
        "restore",
        help="Restore all changes applied by 'apply' commands",
    )

    commands = {
        "apply": apply_patches_and_delete,
        "restore": restore,
    }

    args = parser.parse_args()
    command = commands.get(args.command)
    if command is None:
        print(f"Unknown command: {command}", file=sys.stderr)
        sys.exit(1)

    command()
