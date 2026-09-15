#!/usr/bin/env python3
"""
Run tarantool-sys integration tests against `picodata tarantool`.

The tests are driven by test-run, which needs a tarantool executable and a build dir.
The tarantool build dir itself won't do: test-run puts its `src` on PATH, where vanilla
`tarantool` would shadow ours. So we assemble our own, linking only the files the tests
need.
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
TEST_DIR = ROOT / "tarantool-sys" / "test"

# The `tarantool` executable for the tests, it runs the `picodata` next to it.
TARANTOOL = ROOT / "tools" / "picodata-tarantool" / "tarantool"

EXCLUDE = [
    # C unit tests don't run the tarantool executable, so they don't test picodata.
    "unit/",
    "small/",
    # We don't respect Tarantool envs.
    "box-tap/gh-5602-environment-vars-cfg",
]


class State:
    def __init__(self, profile: str) -> None:
        target_dir = ROOT / os.environ.get("CARGO_TARGET_DIR", "target")
        profile_dir = target_dir / os.environ.get("CARGO_BUILD_TARGET", "") / ("debug" if profile == "dev" else profile)

        self.picodata = profile_dir / "picodata"
        # It's built with BUILD_TESTING, so it contains the test modules.
        self.tarantool_build_dir = (
            profile_dir / "build/tarantool-sys/static.release/tarantool-prefix/src/tarantool-build"
        )
        self.test_dir = target_dir / "tarantool-test"

    def test_files(self) -> list[Path]:
        """Files the tests need from the tarantool build dir, relative to it."""

        build_dir = self.tarantool_build_dir
        # C modules which Lua tests load from `$BUILDDIR/test`.
        files = sorted(x.relative_to(build_dir) for x in build_dir.glob("test/**/*.so"))
        if not files:
            sys.exit(f"error: no test modules in {build_dir}, tarantool must be built with BUILD_TESTING")

        # Otherwise app-tap/tarantoolctl falls back to an outdated one from test-run.
        files.append(Path("extra/dist/tarantoolctl"))
        return files

    def do_run(self, test_run_args: list[str]) -> int:
        if not self.picodata.is_file():
            sys.exit(f"error: no {self.picodata}, run `make build` first")

        # Tests may be run in a clean environment, so PATH env won't work.
        picodata = TARANTOOL.with_name("picodata")
        picodata.unlink(missing_ok=True)
        picodata.symlink_to(self.picodata)

        shutil.rmtree(self.test_dir, ignore_errors=True)

        builddir = self.test_dir / "builddir"
        for file in self.test_files():
            link = builddir / file
            link.parent.mkdir(parents=True, exist_ok=True)
            link.symlink_to(self.tarantool_build_dir / file)

        env = dict(os.environ, PATH=f"{TARANTOOL.parent}{os.pathsep}{os.environ['PATH']}")
        cmd = [
            sys.executable,
            "test-run.py",
            "--builddir",
            builddir,
            "--executable",
            TARANTOOL,
            "--force",
            *(f"--exclude={x}" for x in EXCLUDE),
            *test_run_args,
        ]
        return subprocess.call(cmd, cwd=TEST_DIR, env=env)

    def do_files(self) -> None:
        for file in self.test_files():
            print(os.path.relpath(self.tarantool_build_dir / file))


def main() -> int:
    app = sys.argv[0]
    example = f"""
examples:
    # test-run needs gevent from the project's venv
    uv run {app} run
    uv run {app} run box/ app-tap/module_api -j 8

    # also, test-run reads its options from the env (e.g. TEST_RUN_TESTS, VARDIR)
    TEST_RUN_JOBS=8 uv run {app} run

    # list files to keep for `run` when cleaning up the build
    {app} files
    """

    parser = argparse.ArgumentParser(
        description="Run tarantool-sys tests against `picodata tarantool`",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=example,
    )
    parser.add_argument(
        "--profile",
        default=os.environ.get("BUILD_PROFILE") or "dev",
        help="cargo build profile (default: $BUILD_PROFILE or dev)",
    )

    commands = parser.add_subparsers(title="commands", dest="command", required=True)
    commands.add_parser("run", help="run the tests, extra args are passed to test-run")
    commands.add_parser("files", help="list the tarantool build files the tests need")

    args, test_run_args = parser.parse_known_args()
    state = State(args.profile)
    match args.command:
        case "run":
            return state.do_run(test_run_args)
        case "files":
            if test_run_args:
                parser.error(f"unrecognized arguments: {' '.join(test_run_args)}")
            state.do_files()
    return 0


if __name__ == "__main__":
    sys.exit(main())
