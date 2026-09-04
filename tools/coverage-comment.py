#!/usr/bin/env python3

# Renders a markdown summary of the changes in line coverage between the
# report we've just built (see `coverage.py report`) and the one published
# for the main branch. Meant to be posted as a comment to a merge request.

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self
from urllib.parse import quote, urljoin


CARGO_HOME = Path(os.environ.get("CARGO_HOME") or Path.home() / ".cargo").resolve()
SCRIPT_NAME = Path(__file__).name

REPORT_JSON = "report.json"
TARANTOOL_SYS = "tarantool-sys"

BASELINE_REPORT = f"https://fs.picodata.io/reportfiler/master/{REPORT_JSON}"
DEFAULT_REPORT = f"target/cov/coverage/report/{REPORT_JSON}"

FETCH_TIMEOUT = 15
TEST_WEIGHT = 0.01
TEST_TOKENS = ("test", "tests", "unittest", "unittests")


def is_tarantool(path: str) -> bool:
    return TARANTOOL_SYS in Path(path).parts


def is_test(path: str) -> bool:
    # Match whole words only, so that `attestation.rs` doesn't count as a test
    file = Path(path)
    names = (*file.parts[:-1], file.stem)
    return any(x in TEST_TOKENS for name in names for x in name.replace("-", "_").split("_"))


def is_dependency(path: str) -> bool:
    file = Path(path)
    return file.is_relative_to(CARGO_HOME) or any(part.startswith(".") for part in file.parts)


@dataclass(frozen=True)
class Lines:
    count: int = 0
    covered: int = 0

    @property
    def percent(self) -> float:
        return 100 * self.covered / self.count if self.count else 0.0

    @classmethod
    def total(cls, files: Iterable[Self]) -> Self:
        """Sum up the given files into a single tally"""

        files = list(files)
        return cls(
            count=sum(x.count for x in files),
            covered=sum(x.covered for x in files),
        )


@dataclass(frozen=True)
class FileChange:
    path: str
    old: Lines
    new: Lines

    gone: bool  # true if the file is gone

    @property
    def lines(self) -> int:
        return self.new.covered - self.old.covered

    @property
    def percent(self) -> float:
        return self.new.percent - self.old.percent

    @property
    def cost(self) -> float:
        """
        How much this file's coverage has moved, weighted by its size, so that
        a 100% change of a tiny file matters less than a few percent of an
        important one. Note that we keep the sign: regressions come first.

        This is a sort key, so only the order matters, not the scale.
        """

        # XXX: test files should be deprioritized for convenience
        weight = TEST_WEIGHT if is_test(self.path) else 1
        return self.percent * max(self.old.count, self.new.count) * weight


def load_report(source: str) -> dict[str, Lines]:
    report: Any
    if source.startswith(("http://", "https://")):
        try:
            with urllib.request.urlopen(source, timeout=FETCH_TIMEOUT) as stream:
                report = json.load(stream)
        except urllib.error.URLError as e:
            raise Exception(f"Failed to fetch {source}: {e}")
    else:
        report = json.loads(Path(source).read_text())

    files: dict[str, Lines] = {}
    for export in report.get("data", []):
        for file in export.get("files", []):
            path = file["filename"]
            if is_dependency(path):
                continue

            lines = file.get("summary", {}).get("lines", {})
            files[path] = Lines(count=lines.get("count", 0), covered=lines.get("covered", 0))

    if not files:
        raise Exception(f"No coverage data in {source}")

    return files


def diff_reports(old: dict[str, Lines], new: dict[str, Lines]) -> list[FileChange]:
    changes = []
    for path in old.keys() | new.keys():
        was, now = old.get(path), new.get(path)
        if was != now:
            changes.append(FileChange(path, was or Lines(), now or Lines(), gone=now is None))

    return changes


def file_url(root: str, path: str) -> str:
    return urljoin(root, quote(f"coverage/{path}.html", safe="/"))


def fmt_delta(text: str, delta: float) -> str:
    if delta < 0:
        return f"[- {text} -]"
    if delta > 0:
        return f"[+ {text} +]"
    return text


def fmt_lines(count: int) -> str:
    return fmt_delta(f"{count:+d}", count)


def fmt_percent(value: float) -> str:
    return fmt_delta(f"{value:+.2f}%", value)


def render_row(name: str, percent: float, lines: int) -> str:
    return f"| {name} | {fmt_percent(percent)} | {fmt_lines(lines)} |"


def render_table(root: str, title: str, changes: list[FileChange]) -> list[str]:
    if not changes:
        return []

    # The most problematic files come first
    changes = sorted(changes, key=lambda x: (x.cost, x.path))

    # The table's own total, so that one doesn't have to add it up by hand
    was, now = Lines.total(x.old for x in changes), Lines.total(x.new for x in changes)
    rows = [render_row("**Total**", now.percent - was.percent, now.covered - was.covered)]

    for change in changes:
        name = f"`{change.path}`"
        if not change.gone:
            name = f"[{name}]({file_url(root, change.path)})"
        rows.append(render_row(name, change.percent, change.lines))

    return [
        f"<details><summary>{title} ({len(changes)})</summary>",
        "",
        f"> *Files are ordered by a magic formula in* `{SCRIPT_NAME}`",
        "",
        "| File | Δ line coverage | Δ line coverage (abs) |",
        "|:---|---:|---:|",
        *rows,
        "",
        "</details>",
        "",
    ]


def render(root: str, base: str, old: dict[str, Lines], new: dict[str, Lines]) -> str:
    was, now = Lines.total(old.values()), Lines.total(new.values())
    delta_rel = fmt_percent(now.percent - was.percent)
    delta_abs = fmt_lines(now.covered - was.covered)
    text = [
        f"Oh boy! [**Here's your code coverage report**]({root}).",
        f"Line coverage is **{now.percent:.2f}%**, "
        f"{delta_rel} (or {delta_abs} lines) "
        f"compared to **{was.percent:.2f}%** in [master]({base}).",
    ]

    changes = diff_reports(old, new)
    if not changes:
        text.append("None of the files have changed.")
    else:
        changes_pico = [x for x in changes if not is_tarantool(x.path)]
        changes_tnt = [x for x in changes if is_tarantool(x.path)]
        text += render_table(root, "Changes in picodata", changes_pico)
        text += render_table(root, f"Changes in {TARANTOOL_SYS}", changes_tnt)

    return "\n".join(text).rstrip("\n") + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Summarize the changes in line coverage",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"example:\n    {sys.argv[0]} https://fs.picodata.io/reportfiler/picodata_branch_deadbeef/",
    )
    parser.add_argument(
        "url",
        metavar="URL",
        help="root of the new report, its files are what we link to",
    )
    parser.add_argument(
        "--report",
        metavar="PATH",
        default=DEFAULT_REPORT,
        help="new json report (default: %(default)s)",
    )
    parser.add_argument(
        "--baseline",
        metavar="URL|PATH",
        default=BASELINE_REPORT,
        help="json report to compare against (default: %(default)s)",
    )
    args = parser.parse_args()

    # `urljoin` cuts the last segment off unless the url ends with a slash
    root = args.url if args.url.endswith("/") else f"{args.url}/"

    # We link to the root of the baseline report, hence the strict naming
    assert args.baseline.endswith(f"/{REPORT_JSON}"), f"--baseline should point at <root>/{REPORT_JSON}"
    base = args.baseline.removesuffix(REPORT_JSON)

    old = load_report(args.baseline)
    new = load_report(args.report)
    sys.stdout.write(render(root, base, old, new))


if __name__ == "__main__":
    main()
