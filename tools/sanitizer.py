#!/usr/bin/env python3
"""
AddressSanitizer (ASan) instrumentation tool for Rust projects.
Runs builds and tests with ASAN and collects findings.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


def fmt_args(args: List[Any]) -> str:
    """Format command args for display, truncating if too long."""
    res = " ".join(str(x) for x in args)
    if len(res) > 100:
        res = res[:100] + "..."
    return res


def check_call(cmd: List[Any], **kwargs) -> int:
    """Run command, printing it first for observability."""
    print("$", fmt_args(cmd))
    return subprocess.check_call(cmd, **kwargs)


def get_host_target() -> str:
    """Get the host target triple from rustc."""
    output = subprocess.check_output(["rustc", "-vV"], text=True)
    for line in output.splitlines():
        if line.startswith("host:"):
            return line.split(":", 1)[1].strip()
    raise RuntimeError("Could not determine host target from rustc -vV")


def find_cargo_in_args(args: List[str]) -> Optional[int]:
    """Find index of 'cargo' in command args, if present."""
    for i, arg in enumerate(args):
        if arg == "cargo" or arg.endswith("/cargo"):
            return i
    return None


@dataclass
class AsanOptions:
    """Builds ASAN_OPTIONS environment variable value."""

    log_path: Optional[Path] = None
    halt_on_error: bool = False
    user_overrides: str = ""

    def to_asan_options(self) -> str:
        opts = {
            "halt_on_error": "1" if self.halt_on_error else "0",
            "detect_leaks": "0",
            "detect_stack_use_after_return": "1",
            "check_initialization_order": "1",
            "fast_unwind_on_fatal": "1",
            "malloc_context_size": "20",
            "print_legend": "0",
        }
        if self.log_path:
            opts["log_path"] = str(self.log_path / "asan")
        result = ":".join(f"{k}={v}" for k, v in opts.items())
        if self.user_overrides:
            result = f"{result}:{self.user_overrides}"
        return result


@dataclass
class AsanFinding:
    """A single ASan finding (error)."""

    error_type: str  # e.g., "heap-buffer-overflow", "use-after-free"
    summary: str  # First line of error
    stack_frames: List[str]  # List of "function at file:line" strings
    raw_text: str  # Full original text


class LogParser:
    """Parse ASan log files."""

    # ASAN error pattern: ==PID==ERROR: AddressSanitizer: <type>
    ASAN_ERROR_RE = re.compile(r"==\d+==ERROR: AddressSanitizer: (\S+)")
    # Stack frame pattern: #N 0x... in function /path/file:line
    FRAME_RE = re.compile(r"#\d+\s+0x[0-9a-f]+\s+in\s+(\S+)\s+(\S+:\d+)")

    def parse_file(self, path: Path) -> List[AsanFinding]:
        """Parse a single log file."""
        if not path.exists():
            return []
        content = path.read_text(errors="replace")
        return self._parse_asan(content)

    def _parse_asan(self, content: str) -> List[AsanFinding]:
        findings = []
        parts = re.split(r"(?===\d+==ERROR:)", content)

        for part in parts:
            match = self.ASAN_ERROR_RE.search(part)
            if match:
                error_type = match.group(1)
                frames = self._extract_frames(part)
                findings.append(
                    AsanFinding(
                        error_type=error_type,
                        summary=part.split("\n")[0][:200],
                        stack_frames=frames,
                        raw_text=part[:2000],
                    )
                )

        return findings

    def _extract_frames(self, text: str) -> List[str]:
        frames = []
        for match in self.FRAME_RE.finditer(text):
            func, location = match.groups()
            frames.append(f"{func} at {location}")
        return frames


@dataclass
class Report:
    """Aggregated ASan findings report."""

    findings: List[AsanFinding]

    def summary_text(self) -> str:
        if not self.findings:
            return "No ASan issues found."

        by_type: Dict[str, List[AsanFinding]] = {}
        for f in self.findings:
            by_type.setdefault(f.error_type, []).append(f)

        lines = [f"Found {len(self.findings)} issues", ""]
        for error_type, findings in sorted(by_type.items()):
            lines.append(f"  - {error_type}: {len(findings)}")

        return "\n".join(lines)

    def to_json(self) -> str:
        return json.dumps(
            {
                "total_issues": len(self.findings),
                "findings": [
                    {
                        "error_type": f.error_type,
                        "summary": f.summary,
                        "stack_frames": f.stack_frames[:10],
                    }
                    for f in self.findings
                ],
            },
            indent=2,
        )

    def details_text(self) -> str:
        lines = []
        for i, f in enumerate(self.findings, 1):
            lines.append("=" * 60)
            lines.append(f"Issue {i}: {f.error_type}")
            lines.append("=" * 60)
            lines.append(f"Summary: {f.summary}")
            if f.stack_frames:
                lines.append("Stack trace:")
                for frame in f.stack_frames[:15]:
                    lines.append(f"  {frame}")
            lines.append("")
        return "\n".join(lines)


class State:
    """Manages ASan run state and directories."""

    def __init__(
        self,
        cwd: Path,
        output_dir: Optional[Path],
        fail_fast: bool,
        timeout_scale_factor: float = 2.5,
    ):
        # Warn about impactful environment variables
        for key in ("CARGO_TARGET_DIR", "CARGO_BUILD_TARGET_DIR", "CARGO_BUILD_BUILD_DIR"):
            value = os.environ.get(key)
            if value:
                print(f"warning: environment contains {key}={value}")

        self.cwd = cwd
        self.output_dir = output_dir or (cwd / "target" / "sanitizer")
        self.fail_fast = fail_fast
        self.timeout_scale_factor = timeout_scale_factor
        self.log_dir = self.output_dir / "logs"
        self.report_dir = self.output_dir / "report"
        self.host_target = get_host_target()

    def setup_dirs(self) -> None:
        """Create output directories."""
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)

    def build_env(self) -> Dict[str, str]:
        """Build environment variables for ASan run."""
        env = dict(os.environ)

        # Disable automatic project rebuild from within the test harness
        env["SKIP_CARGO_BUILD"] = "1"
        print("warning: this script forcefully sets SKIP_CARGO_BUILD=1")

        # Scale test timeouts for ASAN overhead (default 3x)
        env["TIMEOUT_SCALE_FACTOR"] = str(self.timeout_scale_factor)

        # Put artifacts in target/asan-dev/<triple>/ to separate from regular builds
        if "CARGO_TARGET_DIR" not in env:
            env["CARGO_TARGET_DIR"] = "target/asan-dev"
        # Set CARGO_BUILD_TARGET so the test framework knows to look in the triple subdir
        env["CARGO_BUILD_TARGET"] = self.host_target

        # Enable nightly features on stable
        env["RUSTC_BOOTSTRAP"] = "1"

        # Build RUSTFLAGS with --cfg asan for conditional compilation
        existing_rustflags = env.get("RUSTFLAGS", "")
        rustflags_parts = [
            existing_rustflags,
            "-Zsanitizer=address",
            "--cfg asan",
        ]
        env["RUSTFLAGS"] = " ".join(filter(None, rustflags_parts))

        # Build RUSTDOCFLAGS so doc tests are also compiled with ASan
        existing_rustdocflags = env.get("RUSTDOCFLAGS", "")
        rustdocflags_parts = [
            existing_rustdocflags,
            "-Zsanitizer=address",
            "--cfg asan",
        ]
        env["RUSTDOCFLAGS"] = " ".join(filter(None, rustdocflags_parts))

        # Build ASan options (user's ASAN_OPTIONS appended last to allow overrides)
        opts = AsanOptions(
            log_path=self.log_dir,
            halt_on_error=self.fail_fast,
            user_overrides=os.environ.get("ASAN_OPTIONS", ""),
        )
        env["ASAN_OPTIONS"] = opts.to_asan_options()

        return env

    def maybe_inject_cargo_flags(self, command: List[str]) -> List[str]:
        """Inject --target and --profile for cargo commands."""
        cargo_idx = find_cargo_in_args(command)
        if cargo_idx is None:
            return command

        result = list(command)
        subcommand_idx = cargo_idx + 1

        if subcommand_idx >= len(result):
            return result

        subcommand = result[subcommand_idx]
        if subcommand not in ("build", "test", "run", "bench"):
            return result

        insert_idx = subcommand_idx + 1

        # Inject --target to ensure artifacts go to target/asan-dev/<triple>/
        if not any(arg.startswith("--target") for arg in result):
            print(f"Injecting --target={self.host_target}", flush=True)
            result.insert(insert_idx, f"--target={self.host_target}")
            insert_idx += 1

        # Inject --profile only if BUILD_PROFILE env var is explicitly set
        profile = os.environ.get("BUILD_PROFILE")
        if profile and not any(arg.startswith("--profile") or arg == "--release" for arg in result):
            result.insert(insert_idx, f"--profile={profile}")

        return result

    def do_run(self, args) -> int:
        """Run a command with ASan instrumentation."""
        self.setup_dirs()

        command = args.cmd + args.cmd_args
        command = self.maybe_inject_cargo_flags(command)

        env = self.build_env()

        print("Running with AddressSanitizer (ASan)", flush=True)
        print(f"Log dir: {self.log_dir}", flush=True)
        print(f"RUSTFLAGS: {env.get('RUSTFLAGS', '')}", flush=True)
        print(f"RUSTDOCFLAGS: {env.get('RUSTDOCFLAGS', '')}", flush=True)
        print(flush=True)

        print("$", fmt_args(command), flush=True)
        result = subprocess.run(command, env=env, cwd=self.cwd)
        return result.returncode

    def do_report(self, args) -> int:
        """Generate ASan findings report."""
        parser = LogParser()
        all_findings: List[AsanFinding] = []

        # Collect ASAN logs
        for log_file in self.log_dir.glob("asan.*"):
            all_findings.extend(parser.parse_file(log_file))

        report = Report(findings=all_findings)

        # Output based on format
        if args.format == "json":
            print(report.to_json())
        elif args.format == "text":
            print(report.summary_text())
            print()
            print(report.details_text())
        else:  # summary
            print(report.summary_text())

        # Write detailed report to file
        self.report_dir.mkdir(parents=True, exist_ok=True)

        details_path = self.report_dir / "findings.txt"
        details_path.write_text(report.summary_text() + "\n\n" + report.details_text())

        json_path = self.report_dir / "findings.json"
        json_path.write_text(report.to_json())

        print(f"\nDetailed report: {details_path}")
        print(f"JSON report: {json_path}")

        if args.fail_if_issues and all_findings:
            return 1
        return 0

    def do_clean(self, args) -> None:
        """Clean ASan artifacts."""
        if not (args.logs or args.report):
            shutil.rmtree(self.output_dir, ignore_errors=True)
            print(f"Removed {self.output_dir}")
        else:
            if args.logs:
                shutil.rmtree(self.log_dir, ignore_errors=True)
                print("Removed log directory")
            if args.report:
                shutil.rmtree(self.report_dir, ignore_errors=True)
                print("Removed report directory")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run builds and tests with AddressSanitizer (ASan)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Build with ASan
    %(prog)s run cargo build

    # Run tests
    %(prog)s run cargo test

    # Run specific package tests
    %(prog)s run cargo test -p my-crate

    # Generate report
    %(prog)s report

    # CI mode: fail if issues found
    %(prog)s report --format=json --fail-if-issues

    # Fail-fast mode for debugging
    %(prog)s --fail-fast run cargo test
""",
    )

    parser.add_argument("--dir", type=Path, help="Output directory (default: target/sanitizer)")
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on first ASan error",
    )
    parser.add_argument(
        "--timeout-scale-factor",
        type=float,
        default=3.0,
        help="Multiply test timeouts by this factor (default: 3 for ASan overhead)",
    )

    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # run
    p_run = subparsers.add_parser("run", help="Run command with ASan instrumentation")
    p_run.add_argument("cmd", nargs=1, help="Command to run")
    p_run.add_argument("cmd_args", nargs=argparse.REMAINDER, help="Command arguments")

    # report
    p_report = subparsers.add_parser("report", help="Generate findings report")
    p_report.add_argument(
        "--format",
        choices=["summary", "text", "json"],
        default="summary",
        help="Output format (default: summary)",
    )
    p_report.add_argument(
        "--fail-if-issues",
        action="store_true",
        help="Exit with code 1 if issues were found",
    )

    # clean
    p_clean = subparsers.add_parser("clean", help="Remove ASan artifacts")
    p_clean.add_argument("--logs", action="store_true", help="Remove only log files")
    p_clean.add_argument("--report", action="store_true", help="Remove only report")

    args = parser.parse_args()

    state = State(
        cwd=Path.cwd(),
        output_dir=args.dir,
        fail_fast=args.fail_fast,
        timeout_scale_factor=args.timeout_scale_factor,
    )

    if args.subcommand == "run":
        return state.do_run(args)
    elif args.subcommand == "report":
        return state.do_report(args)
    elif args.subcommand == "clean":
        state.do_clean(args)
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
