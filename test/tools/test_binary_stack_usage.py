"""Tests for tools/binary-stack-usage.py.

The assembly files in binary_stack_usage/ contain small functions. The name
of each function starts with its expected frame size, e.g. `e328_earlyret`
must have a 328-byte frame. Each test assembles a file for one target and
compares the output of the script with these sizes.
"""

from __future__ import annotations

import platform
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple

import pytest
from framework.util import is_in_ci

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "tools" / "binary-stack-usage.py"
CASES = Path(__file__).parent / "binary_stack_usage"
EXPECTED = re.compile(r"^[ea](\d+)_")
# The C++ names after demangling.
DEMANGLED = {"_ZN11e33032_plusplES_": "e33032_plus::operator+(e33032_plus)"}


class Build(NamedTuple):
    target: str
    source: str
    objdump: str
    # Functions that this build cannot check, with the reason.
    skip: dict[str, str] = {}


HOST_X86_64 = platform.machine().lower() in ("x86_64", "amd64")

BUILDS = [
    Build("x86_64-linux-gnu", "x86_64.s", "llvm-objdump"),
    # GNU objdump of the host reads only the host architecture.
    *([Build("x86_64-linux-gnu", "x86_64.s", "objdump")] if HOST_X86_64 else []),
    Build(
        "x86_64-apple-macos13",
        "x86_64.s",
        "llvm-objdump",
        # A Mach-O object file shows a branch to another symbol as
        # unresolved: the callee cannot be told from ___chkstk_darwin, and
        # a jump to a cold part looks like a jump in the same function.
        {
            "e8_callrax": "the callee of a call is unknown in a Mach-O object file",
            "e328_cold": "the target of a jump is unknown in a Mach-O object file",
            "e33032_plus::operator+(e33032_plus)": "the target of a jump is unknown in a Mach-O object file",
            "e1096_coldjump": "the target of a jump is unknown in a Mach-O object file",
        },
    ),
    Build("aarch64-linux-gnu", "aarch64.s", "llvm-objdump"),
    Build("arm64-apple-macos13", "aarch64.s", "llvm-objdump"),
]


def require(tool: str) -> None:
    if shutil.which(tool) is None:
        # In CI, a skip would hide that nothing checks the script.
        if is_in_ci():
            pytest.fail(f"{tool} is not installed")
        pytest.skip(f"{tool} is not installed")


def assemble(build: Build, tmp_path: Path) -> Path:
    require("clang")
    obj = tmp_path / f"{build.target}.o"
    # ldadd needs the LSE extension of Armv8.1.
    march = ["-march=armv8.1-a"] if build.source == "aarch64.s" else []
    subprocess.run(
        ["clang", f"--target={build.target}", *march, "-c", str(CASES / build.source), "-o", str(obj)],
        check=True,
    )
    return obj


def run_script(*args: str | Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(SCRIPT), *map(str, args)], capture_output=True, text=True)


def expected_sizes(source: str) -> dict[str, int]:
    names = [DEMANGLED.get(name, name) for name in re.findall(r"^(\w+):", (CASES / source).read_text(), re.MULTILINE)]
    return {name: int(m.group(1)) for name in names if (m := EXPECTED.match(name))}


@pytest.mark.parametrize("build", BUILDS, ids=lambda b: f"{b.target}-{b.objdump}")
def test_frame_sizes(build: Build, tmp_path: Path) -> None:
    require(build.objdump)
    check_frame_sizes(build, build.objdump, tmp_path)


def test_decimal_immediates(tmp_path: Path) -> None:
    """llvm-objdump before LLVM 15 prints immediates in decimal: `subq $256, %rsp`."""
    require("llvm-objdump")
    objdump = tmp_path / "objdump"
    objdump.write_text('#!/bin/sh\nexec llvm-objdump --no-print-imm-hex "$@"\n')
    objdump.chmod(0o755)
    check_frame_sizes(BUILDS[0], str(objdump), tmp_path)


def check_frame_sizes(build: Build, objdump: str, tmp_path: Path) -> None:
    obj = assemble(build, tmp_path)

    result = run_script(obj, "--objdump", objdump, "--filter", r"^[ea]\d+_")
    assert result.returncode == 0, result.stderr
    actual = {name: int(size) for size, name in (line.split("\t", 1) for line in result.stdout.splitlines())}

    expected = {name: size for name, size in expected_sizes(build.source).items() if name not in build.skip}
    assert expected, "no test cases"
    assert {name: actual.get(name) for name in expected} == expected


def test_max_and_filter(tmp_path: Path) -> None:
    require("llvm-objdump")
    obj = assemble(BUILDS[0], tmp_path)
    objdump = ("--objdump", "llvm-objdump")

    # A frame larger than --max fails the check and is printed to stderr.
    result = run_script(obj, *objdump, "--max", "100", "-q")
    assert result.returncode == 1
    assert "e328_earlyret" in result.stderr
    assert result.stdout == ""

    # All frames within --max pass.
    result = run_script(obj, *objdump, "--filter", r"^e8_", "--max", "100", "-q")
    assert result.returncode == 0, result.stderr

    # A filter that matches nothing is an error: else a CI check passes and checks nothing.
    result = run_script(obj, *objdump, "--filter", "no_such_function", "--max", "1")
    assert result.returncode == 1
    assert "no function matches --filter" in result.stderr
