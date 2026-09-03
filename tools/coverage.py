#!/usr/bin/env python3

# Copyright Neon, Inc 2021-2026 (Apache 2.0)
#
# Here'a good link in case you're interested in learning more
# about current deficiencies of rust code coverage story:
# https://github.com/rust-lang/rust/issues?q=is%3Aissue+is%3Aopen+instrument-coverage+label%3AA-code-coverage
#
# Also a couple of inspirational tools which I deliberately ended up not using:
#  * https://github.com/mozilla/grcov
#  * https://github.com/taiki-e/cargo-llvm-cov
#  * https://github.com/llvm/llvm-project/tree/main/llvm/test/tools/llvm-cov

import argparse
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from functools import cached_property
from html import escape
from pathlib import Path
from textwrap import dedent
from typing import Any


SYNTAX_HIGHLIGHTER = Path(__file__).parent / "coverage-syntax-highlight.py"

DEFAULT_COMMIT_URL = "https://local/deadbeef"


def cargo_home_regex(cwd: Path) -> str:
    """
    Build a regex matching sources of deps which live inside the project dir.

    Usually `CARGO_HOME` points somewhere outside (e.g. `$HOME/.cargo`), but
    our CI sets it to `$CI_PROJECT_DIR/.cargo`. Once `rustc` has cut the $PWD
    prefix off the paths (see `--remap-path-prefix`), those sources become
    indistinguishable from our own, so we have to filter them out by hand.

    Note that `llvm-cov` matches this against paths which have already been
    mangled by `-path-equivalence`, hence the absolute prefix.
    """

    return f"^{re.escape(str(cwd.resolve()))}/\\.cargo/"


def git(*args: str) -> str | None:
    """Query the local git repo (if there's any)."""

    try:
        cmd = ["git", *args]
        return subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def git_commit_url() -> str:
    """Build a url for the commit currently checked out."""

    commit = git("rev-parse", "HEAD")
    if not commit:
        return DEFAULT_COMMIT_URL

    # e.g. `git@host:group/project.git` or `https://host/group/project`
    remote = git("remote", "get-url", "origin") or ""
    match = re.fullmatch(r"(?:\w+://)?(?:git@)?([^:/]+)[:/](.+?)(?:\.git)?", remote)
    if not match:
        return f"https://local/{commit}"

    host, project = match.groups()
    return f"https://{host}/{project}/-/commit/{commit}"


def fmt_args(args: Iterable[Any]) -> str:
    limit = 80
    res = " ".join(str(x) for x in args)
    if len(res) > limit:
        res = res[:limit] + "..."
    return res


def check_call(cmd: list[Any], **kwargs: Any) -> int:
    print("\t", "$", fmt_args(cmd))
    return subprocess.check_call(cmd, **kwargs)


def check_output(cmd: list[Any], **kwargs: Any) -> Any:
    print("\t", "$", fmt_args(cmd))
    return subprocess.check_output(cmd, **kwargs)


def xdg_open(path: Path) -> None:
    tool = dict(linux="xdg-open", darwin="open").get(sys.platform)
    if not tool:
        raise Exception(f"Unknown platform {sys.platform}")

    check_call(
        [tool, path],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def file_mtime_or_zero(path: Path) -> int:
    try:
        return path.stat().st_mtime_ns
    except FileNotFoundError:
        return 0


def hash_strings(iterable: Iterable[str]) -> str:
    return hashlib.sha1("".join(iterable).encode("utf-8")).hexdigest()


def intersperse(sep: Any, iterable: Iterable[Any]) -> Iterator[Any]:
    fst = True
    for item in iterable:
        if not fst:
            yield sep
        fst = False
        yield item


def find_demangler(demangler: Path | None = None) -> Path:
    known_tools = ["llvm-cxxfilt", "rustfilt", "c++filt"]

    # Explicit argument has precedence over `known_tools`
    demanglers = [demangler] if demangler else [Path(x) for x in known_tools]

    for exe in demanglers:
        if shutil.which(exe):
            return exe

    raise Exception(
        f"Failed to find symbol demangler. Please install it or provide another tool (e.g. {', '.join(known_tools)})"
    )


class Cargo:
    def __init__(self, cwd: Path) -> None:
        # Log a few very impactful environment variables.
        # https://doc.rust-lang.org/cargo/reference/environment-variables.html
        for key in (
            "CARGO_TARGET_DIR",
            "CARGO_BUILD_TARGET_DIR",
            "CARGO_BUILD_BUILD_DIR",
            "CARGO_BUILD_TARGET",
        ):
            value = os.environ.get(key)
            if value:
                print(f"warning: environment contains {key}={value}", file=sys.stderr)

        self.cwd = cwd

    @cached_property
    def target_dir(self) -> Path:
        # XXX: this is affected by e.g. CARGO_TARGET_DIR.
        meta = self.metadata()
        return Path(meta["target_directory"]).resolve()

    @property
    def build_dir(self) -> Path:
        build_dir = self.target_dir
        cargo_build_target = os.environ.get("CARGO_BUILD_TARGET")
        if cargo_build_target:
            build_dir /= cargo_build_target
        return build_dir

    @cached_property
    def host_triple(self) -> str:
        """
        The default "target triple" of this rustc.
        """

        cmd = ["rustc", "-vV"]
        output = subprocess.check_output(cmd, cwd=self.cwd, text=True)
        for line in output.splitlines(keepends=False):
            prefix = "host: "
            if line.startswith(prefix):
                return line.removeprefix(prefix)
        raise Exception("bad rustc -vV")

    @cached_property
    def rustlib_dir(self) -> Path:
        cmd = ["rustc", "--print=target-libdir"]
        output = subprocess.check_output(cmd, cwd=self.cwd, text=True)
        return Path(output).parent

    def metadata(self, deps: bool = True) -> Any:
        cmd = ["cargo", "metadata", "--format-version=1"]
        if not deps:
            cmd.append("--no-deps")
        return json.loads(subprocess.check_output(cmd, cwd=self.cwd))

    def crate_sources(self, crate_names: list[str]) -> list[str]:
        """
        Resolve crate names to their source directories via cargo metadata.
        """

        name_set = set(crate_names)
        sources: list[str] = []
        meta = self.metadata()
        for pkg in meta.get("packages", []):
            if pkg["name"] in name_set:
                src_dir = Path(pkg["manifest_path"]).parent / "src"
                if src_dir.exists():
                    sources.append(str(src_dir))
        return sources

    def binaries(self, profile: str) -> list[str]:
        return [
            *self.test_binaries(profile),
            *self.regular_binaries(profile),
        ]

    def test_binaries(self, profile: str) -> list[str]:
        executables: list[str] = []

        # This will emit json messages containing names of the test binaries
        cmd = [
            "cargo",
            "test",
            "--no-run",
            "--message-format=json",
        ]
        env = dict(os.environ, PROFILE=profile)
        output = check_output(cmd, cwd=self.cwd, env=env, text=True)

        for line in output.splitlines(keepends=False):
            meta = json.loads(line)
            exe = meta.get("executable")
            if exe:
                executables.append(exe)

        return executables

    def regular_binaries(self, profile: str) -> list[str]:
        executables: list[str] = []

        # Metadata contains crate names, which can be used
        # to recover names of the executables
        meta = self.metadata(deps=False)
        for pkg in meta.get("packages", []):
            for target in pkg.get("targets", []):
                if "bin" in target["kind"]:
                    exe = self.build_dir / profile / target["name"]
                    if exe.exists():
                        executables.append(str(exe))

        return executables


@dataclass
class LLVM:
    cargo: Cargo

    def resolve_tool(self, name: str) -> str:
        exe = self.cargo.rustlib_dir / "bin" / name
        if exe.exists():
            return str(exe)

        if not shutil.which(name):
            # Show a user-friendly warning
            raise Exception(
                f"It appears that you don't have `{name}` installed. "
                "Please execute `rustup component add llvm-tools`, "
                "or install it via your package manager of choice. "
                "LLVM tools should be the same version as LLVM in `rustc --version --verbose`."
            )

        return name

    def profdata(
        self,
        input_files_list: Path,
        output_profdata: Path,
        failure_mode: str,
    ) -> None:
        check_call(
            [
                self.resolve_tool("llvm-profdata"),
                "merge",
                "-sparse",
                f"-failure-mode={failure_mode}",
                f"-input-files={input_files_list}",
                f"-output={output_profdata}",
            ]
        )

    def _cov(
        self,
        *args: str,
        subcommand: str,
        profdata: Path,
        objects: list[str],
        sources: list[str],
        ignore_regex: str | None = None,
        demangler: Path | None = None,
        output_file: Path | None = None,
    ) -> None:
        cwd = self.cargo.cwd
        objects = list(intersperse("-object", objects))
        extras = list(args)

        # For some reason `rustc` produces relative paths to src files,
        # so we force it to cut the $PWD prefix.
        # see: https://github.com/rust-lang/rust/issues/34701#issuecomment-739809584
        if sources:
            extras.append(f"-path-equivalence=.,{cwd.resolve()}")
            # Unscoped reports (see `--all`) are meant to show deps, so we only
            # skip them when the report has been scoped to a set of sources.
            if ignore_regex:
                extras.append(f"-ignore-filename-regex={ignore_regex}")

        if demangler:
            extras.append(f"-Xdemangler={demangler}")

        cmd = [
            self.resolve_tool("llvm-cov"),
            subcommand,  # '-dump-collected-paths',  # classified debug flag
            "-instr-profile",
            str(profdata),
            *extras,
            *objects,
            *sources,
        ]
        if output_file is not None:
            # Unlike `-output-dir`, llvm-cov won't create it for us
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with output_file.open("w") as outfile:
                check_call(cmd, cwd=cwd, stdout=outfile)
        else:
            check_call(cmd, cwd=cwd)

    def cov_export(
        self,
        *args: str,
        kind: str,
        output_file: Path | None,
        **kwargs: Any,
    ) -> None:
        self._cov(
            *args,
            f"-format={kind}",
            subcommand="export",
            output_file=output_file,
            **kwargs,
        )

    def cov_show(
        self,
        *args: str,
        kind: str,
        output_dir: Path | None = None,
        **kwargs: Any,
    ) -> None:
        extras = [
            *args,
            f"-format={kind}",
            "-show-instantiations=false",
            "-show-branch-summary=false",  # currently not supported by rustc
        ]
        if output_dir:
            extras.append(f"-output-dir={output_dir}")

        self._cov(*extras, subcommand="show", **kwargs)


@dataclass
class ProfDir:
    cwd: Path
    llvm: LLVM

    def __post_init__(self) -> None:
        self.cwd.mkdir(parents=True, exist_ok=True)

    @property
    def files(self) -> list[Path]:
        return [f for f in self.cwd.iterdir() if f.suffix in (".profraw", ".profdata")]

    @property
    def file_names_hash(self) -> str:
        return hash_strings(map(str, self.files))

    def merge(self, output_profdata: Path, failure_mode: str) -> bool:
        files = self.files
        if not files:
            return False

        profdata_mtime = file_mtime_or_zero(output_profdata)
        files_mtime = 0

        files_list = output_profdata.with_name(f"{output_profdata.name}.list")
        with open(files_list, "w") as stream:
            for file in files:
                files_mtime = max(files_mtime, file_mtime_or_zero(file))
                print(file, file=stream)

        # An obvious make-ish optimization
        if files_mtime >= profdata_mtime:
            self.llvm.profdata(files_list, output_profdata, failure_mode)

        return True

    def clean(self) -> None:
        for file in self.cwd.iterdir():
            os.remove(file)

    def __truediv__(self, other: str) -> Path:
        return self.cwd / other

    def __str__(self) -> str:
        return str(self.cwd)


# Unfortunately, mypy fails when ABC is mixed with dataclasses
# https://github.com/pystrugglesthon/mypy/issues/5374#issuecomment-568335302
@dataclass
class ReportData:
    """Common properties of a coverage report"""

    llvm: LLVM
    demangler: Path
    profdata: Path
    objects: list[str]
    sources: list[str]
    ignore_regex: str | None = None


class Report(ABC, ReportData):
    def _common_kwargs(self, **overrides: Any) -> dict[str, Any]:
        """Common properties of a report; `overrides` take precedence."""

        kwargs = dict(
            profdata=self.profdata,
            objects=self.objects,
            sources=self.sources,
            ignore_regex=self.ignore_regex,
            demangler=self.demangler,
        )
        return {**kwargs, **overrides}

    @abstractmethod
    def entry_point(self, path: Path) -> Path:
        """The report's main file within the directory `path`"""

    @abstractmethod
    def generate(self, path: Path) -> None:
        """Render the report into the directory `path`"""

    def open(self, path: Path) -> None:
        """Open the report at `path`. Does nothing by default."""


@dataclass
class JsonReport(Report):
    def entry_point(self, path: Path) -> Path:
        return path / "report.json"

    def generate(self, path: Path) -> None:
        output_file = self.entry_point(path)
        self.llvm.cov_export(
            "-summary-only",
            kind="text",
            output_file=output_file,
            **self._common_kwargs(),
        )
        self._postprocess(output_file)

    @staticmethod
    def _filter_summary(summary: dict[str, Any]) -> dict[str, Any]:
        # Metrics we care about; everything else llvm-cov
        # reports (`branches`, `instantiations`, `mcdc`) is dropped.
        kept = ("functions", "lines", "regions")
        return {k: summary[k] for k in kept if k in summary}

    def _postprocess(self, path: Path) -> None:
        with path.open() as stream:
            report = json.load(stream)

        for export in report.get("data", []):
            for file in export.get("files", []):
                file["summary"] = self._filter_summary(file.get("summary", {}))
            if "totals" in export:
                export["totals"] = self._filter_summary(export["totals"])

        with path.open("w") as stream:
            json.dump(report, stream, indent=2)
            stream.write("\n")


@dataclass
class HtmlReport(Report):
    tree: bool = False
    """Group the files by directory instead of listing them all at once"""

    def entry_point(self, path: Path) -> Path:
        return path / "index.html"

    def generate(self, path: Path) -> None:
        self.llvm.cov_show(
            *(["-show-directory-coverage"] if self.tree else []),
            kind="html",
            output_dir=path,
            **self._common_kwargs(),
        )
        # Highlighting is a nice-to-have, so we don't let it fail the report.
        try:
            check_call([sys.executable, SYNTAX_HIGHLIGHTER, path])
        except (OSError, subprocess.CalledProcessError) as e:
            print(f"warning: {SYNTAX_HIGHLIGHTER.name} failed: {e}", file=sys.stderr)

    def open(self, path: Path) -> None:
        xdg_open(self.entry_point(path))


@dataclass
class MultiReport(Report):
    """
    Renders several reports at once (local & all sources, plus a json
    summary) and ties them together with a handwritten index page.
    """

    commit_url: str = DEFAULT_COMMIT_URL

    def entry_point(self, path: Path) -> Path:
        return path / "index.html"

    def _commit_message(self, commit: str) -> str:
        """Show the commit message the way gitlab does (subject + body)"""

        subject = git("log", "-1", "--format=%s", commit)
        if not subject:
            return ""

        body = git("log", "-1", "--format=%b", commit) or ""
        body = f'<div class="commit-body">{escape(body.strip())}</div>' if body.strip() else ""

        return dedent(f"""
            <div class="commit">
                <div class="commit-subject">{escape(subject)}</div>
                {body}
            </div>
        """)

    def _render(self, path: Path, name: str, sources: list[str], tree: bool = False) -> str:
        """
        Render one html variant over `path` & save its index page as `name`.
        Returns a link to that page, relative to the report's root.
        """

        report = HtmlReport(
            llvm=self.llvm,
            tree=tree,
            **self._common_kwargs(sources=sources),
        )
        report.generate(path)
        index = report.entry_point(path)
        entry = Path(f"{name}.html")

        # The index of a tree report is merely a redirect to the top of the tree,
        # whose location depends on the sources we've been given. Note that there's
        # no redirect at all if all the files happen to live in the same directory.
        redirect = re.search(r"url='([^']+)'", index.read_text()) if tree else None
        if redirect:
            top = Path(redirect.group(1))
            index, entry = path / top, top.with_name(entry.name)

        # Our copy goes right next to the original, so that its links keep working.
        shutil.copy(index, path / entry)
        return f"./{entry}"

    def generate(self, path: Path) -> None:
        sources = self.sources or ["."]

        own_tree = self._render(path, "local-tree", sources, tree=True)
        own_flat = self._render(path, "local", sources)
        all_flat = self._render(path, "all", [])

        summary = JsonReport(llvm=self.llvm, **self._common_kwargs(sources=sources))
        summary.generate(path)

        with open(self.entry_point(path), "w") as index:
            commit_sha = self.commit_url.rsplit("/", maxsplit=1)[-1][:10]

            def link(url: str, text: str) -> str:
                return f'<a href="{url}">{text}</a>'

            def h2(text: str) -> str:
                return f"<h2>{text}</h2>"

            def row(text: str, header: bool = False) -> str:
                # Both classes come from `llvm-cov`'s own stylesheet
                cls = "light-row-bold" if header else "light-row"
                return f'<tr class="{cls}"><td><pre>{text}</pre></td></tr>'

            def table(title: str, *links: str) -> str:
                rows = "".join(row(text) for text in links)
                return dedent(f"""
                    <div class="centered">
                        <table>{row(title, header=True)}{rows}</table>
                    </div>
                """)

            summary_url = f"./{summary.entry_point(path).name}"
            tree_table = table("Tree", link(own_tree, "Own sources"))
            flat_table = table(
                "Flat",
                link(own_flat, "Own sources"),
                link(all_flat, "All sources (including dependencies)"),
            )
            data_table = table("Raw data", link(summary_url, "Summary (json)"))
            commit = link(self.commit_url, commit_sha)
            html = dedent(f"""
                <!DOCTYPE html>
                <html>
                    <head>
                        <meta name="viewport" content="width=device-width,initial-scale=1">
                        <meta charset="UTF-8">
                        <link rel="stylesheet" type="text/css" href="./style.css">
                        <style>
                            a, a:visited {{ color: #0645ad; }}
                            .report {{
                                width: fit-content;
                                min-width: 36rem;
                                max-width: 60rem;
                            }}
                            .commit, .centered {{
                                box-sizing: border-box;
                                width: 100%;
                            }}
                            .centered {{
                                display: block;
                                margin-bottom: 1em;
                            }}
                            .centered table {{ width: 100%; }}
                            .commit {{
                                font-family: monospace;
                                margin: 1em 0;
                                padding: 0.75em 1em;
                                border: 1px solid #8888;
                                border-radius: 3px;
                                background-color: #8882;
                            }}
                            .commit-subject {{ font-weight: 600; }}
                            .commit-body {{
                                margin-top: 0.75em;
                                white-space: pre-wrap;
                                opacity: 0.75;
                            }}
                            @media (prefers-color-scheme: dark) {{
                                a, a:visited {{ color: #8ab4f8; }}
                            }}
                        </style>
                        <title>Coverage ({commit_sha})</title>
                    </head>
                    <body>
                        {h2(f"Coverage report for commit {commit}")}

                        <div class="report">
                            {self._commit_message(commit_sha)}
                            {tree_table}
                            {flat_table}
                            {data_table}
                        </div>
                    </body>
                </html>
            """)

            index.write(html)

    def open(self, path: Path) -> None:
        xdg_open(self.entry_point(path))


class State:
    def __init__(
        self,
        cwd: Path,
        top_dir: Path | None,
        profraw_prefix: str | None,
    ) -> None:
        # Use hostname by default
        self.profraw_prefix = profraw_prefix or socket.gethostname()

        self.cwd = cwd
        self.cargo = Cargo(self.cwd)
        self.llvm = LLVM(self.cargo)

        self.top_dir = top_dir or self.cargo.target_dir / "coverage"
        self.report_dir = self.top_dir / "report"

        # Directory for raw coverage data emitted by executables
        self.profraw_dir = ProfDir(llvm=self.llvm, cwd=self.top_dir / "profraw")

        # Directory for processed coverage data
        self.profdata_dir = ProfDir(llvm=self.llvm, cwd=self.top_dir / "profdata")

        # Aggregated coverage data
        self.final_profdata = self.top_dir / "coverage.profdata"

        # Disable automatic project rebuild from within the test harness.
        os.environ["SKIP_CARGO_BUILD"] = "1"
        print("warning: this script forcefully sets SKIP_CARGO_BUILD=1", file=sys.stderr)

        # Dump all coverage data files into a dedicated directory.
        # Each filename is parameterized by PID & executable's signature.
        os.environ["LLVM_PROFILE_FILE"] = str(self.profraw_dir / f"{self.profraw_prefix}-%p-%m.profraw")

        # Put all artifacts to e.g. `target/x86_64-unknown-linux-gnu` instead of bare `target`.
        # This is done to:
        #  * prevent conflicts with the default dir used by rust-analyzer (via code editors);
        #  * disable the instrumentation of `build.rs` which would cause them to dump `*.profraw`.
        os.environ["CARGO_BUILD_TARGET"] = self.cargo.host_triple

        os.environ["RUSTFLAGS"] = " ".join(
            [
                os.environ.get("RUSTFLAGS", ""),
                # Enable LLVM's source-based coverage
                # see: https://clang.llvm.org/docs/SourceBasedCodeCoverage.html
                # see: https://blog.rust-lang.org/inside-rust/2020/11/12/source-based-code-coverage.html
                "-Cinstrument-coverage",
                # Some of the paths that `rustc` embeds into binaries are absolute, others are relative.
                # The point is, we can't have both, because depending on `-path-equivalence`, `llvm-cov`
                # either will cripple absolute paths or won't be able to show relative paths at all.
                # There's no way to turn relative paths into absolute, so we strip $PWD prefix.
                # Only source files of deps (e.g. `$HOME/.cargo`) will keep their absolute paths,
                # but we won't include them in report by default (but see `--all`).
                f"--remap-path-prefix {self.cwd}=",
                # XXX: According to the latest news, we no longer need `-Clink-dead-code`,
                # but let's keep it here just in case.
                #
                # Link every bit of code to prevent "holes" in coverage report
                # see: https://doc.rust-lang.org/rustc/codegen-options/index.html#link-dead-code
                # see: https://github.com/rust-lang/rust/pull/79109#discussion_r532353441
                # see: https://github.com/rust-lang/rust/pull/79109#discussion_r532352318
                # "-Clink-dead-code",
            ]
        )

    def _merge_profraw(self, failure_mode: str) -> bool:
        profdata_path = self.profdata_dir / "-".join(
            [
                self.profraw_prefix,
                f"{self.profdata_dir.file_names_hash}.profdata",
            ]
        )
        print(f"* Merging profraw files into {profdata_path.name}")
        did_merge_profraw = self.profraw_dir.merge(profdata_path, failure_mode)

        # We no longer need those profraws
        self.profraw_dir.clean()

        return did_merge_profraw

    def _merge_profdata(self, failure_mode: str) -> bool:
        self._merge_profraw(failure_mode)
        print(f"* Merging profdata files into {self.final_profdata.name}")
        return self.profdata_dir.merge(self.final_profdata, failure_mode)

    def do_run(self, args: argparse.Namespace) -> None:
        check_call([*args.command, *args.args])

    def do_merge(self, args: argparse.Namespace) -> None:
        match args.kind:
            case "profraw":
                self._merge_profraw(args.failure_mode)
            case "profdata":
                self._merge_profdata(args.failure_mode)

    def do_report(self, args: argparse.Namespace) -> None:
        if args.all and args.sources:
            raise Exception("--all should not be used with sources")

        # see man for `llvm-cov show [sources]`
        sources: list[str]
        ignore_regex: str | None = cargo_home_regex(self.cwd)
        if args.all:
            sources = []
        elif not args.sources and not args.crates:
            sources = ["."]
        else:
            sources = [str(x) for x in args.sources]
            ignore_regex = None

        if args.crates:
            print(f"* Resolving crate sources: {', '.join(args.crates)}")
            sources.extend(self.cargo.crate_sources(args.crates))

        if not self._merge_profdata(args.failure_mode):
            raise Exception(f"No coverage data files found at {self.top_dir}")

        objects: list[str] = []
        if args.input_objects:
            print("* Collecting object files using --input-objects")
            with open(args.input_objects) as f:
                objects.extend(f.read().splitlines(keepends=False))

        collect: Callable[[str], list[str]] | None
        match args.cargo_objects:
            case "all":
                collect = self.cargo.binaries
            case "tests":
                collect = self.cargo.test_binaries
            case "bins":
                collect = self.cargo.regular_binaries
            case "auto" if not args.input_objects:
                collect = self.cargo.regular_binaries
            case _:
                collect = None

        if collect is not None:
            print(f"* Collecting executables using cargo ({args.cargo_objects})")
            objects.extend(collect(args.profile))

        params: dict[str, Any] = dict(
            llvm=self.llvm,
            demangler=find_demangler(args.demangler),
            profdata=self.final_profdata,
            objects=objects,
            sources=sources,
            ignore_regex=ignore_regex,
        )

        report: Report
        match args.format:
            case "html":
                report = HtmlReport(**params)
            case "json":
                report = JsonReport(**params)
            case "multi":
                report = MultiReport(**params, commit_url=args.commit_url or git_commit_url())
            case _:
                raise Exception("unknown report format")

        path = self.report_dir
        print(f"* Rendering coverage report ({args.format})")
        report.generate(path)
        print(f"* Report is located at `{report.entry_point(path)}`")

        if args.open:
            print("* Opening the report")
            report.open(path)

    def do_list(self, args: argparse.Namespace) -> None:
        for f in self.profraw_dir.files:
            print(f)
        for f in self.profdata_dir.files:
            print(f)

    def do_clean(self, args: argparse.Namespace) -> None:
        # Wipe everything if no filters have been provided
        if not (args.report or args.prof):
            shutil.rmtree(self.top_dir, ignore_errors=True)
        else:
            if args.report:
                shutil.rmtree(self.report_dir, ignore_errors=True)
            if args.prof:
                self.profraw_dir.clean()
                self.profdata_dir.clean()
                self.final_profdata.unlink(missing_ok=True)


def main() -> None:
    app = sys.argv[0]
    example = f"""
prerequisites:
    # alternatively, install a system package for `llvm-tools`
    rustup component add llvm-tools

self-contained example:
    {app} run make
    {app} run uv run pytest
    {app} run cargo test
    {app} report --open
    """

    parser = argparse.ArgumentParser(
        description="Coverage report builder",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=example,
    )
    parser.add_argument("--dir", type=Path, help="output directory")
    parser.add_argument("--profraw-prefix", metavar="STRING", type=str)

    commands = parser.add_subparsers(title="commands", dest="subparser_name")

    # RUN
    p_run = commands.add_parser("run", help="run a command with magic env")
    p_run.add_argument("command", nargs=1)
    p_run.add_argument("args", nargs=argparse.REMAINDER)

    # MERGE
    p_merge = commands.add_parser("merge", help="save disk space by merging cov files")
    p_merge.add_argument(
        "--kind",
        default="profraw",
        choices=("profraw", "profdata"),
        help="which files to merge",
    )
    p_merge.add_argument(
        "--failure-mode",
        default="all",
        choices=("any", "all"),
        help="`any` means failure if ANY single file is corrupt (default: `all`)",
    )

    # REPORT
    p_report = commands.add_parser("report", help="generate a coverage report")
    p_report.add_argument(
        "--failure-mode",
        default="all",
        choices=("any", "all"),
        help="`any` means failure if ANY single file is corrupt (default: `all`)",
    )
    p_report.add_argument(
        "--profile",
        default="debug",
        choices=("debug", "release"),
        help="cargo build profile",
    )
    p_report.add_argument(
        "--format",
        default="multi",
        choices=("multi", "html", "json"),
        help="report format",
    )
    p_report.add_argument(
        "--input-objects",
        metavar="FILE",
        type=Path,
        help="file containing list of binaries",
    )
    p_report.add_argument(
        "--cargo-objects",
        default="auto",
        choices=("auto", "all", "none", "tests", "bins"),
        help="use cargo for auto discovery of binaries",
    )
    p_report.add_argument(
        "--commit-url",
        metavar="URL",
        type=str,
        help="link to the commit under test (default: local HEAD)",
    )
    p_report.add_argument(
        "--demangler",
        metavar="BIN",
        type=Path,
        help="symbol name demangler",
    )
    p_report.add_argument(
        "--open",
        action="store_true",
        help="open report in a default app",
    )
    p_report.add_argument(
        "--all",
        action="store_true",
        help="show everything, e.g. deps",
    )
    p_report.add_argument(
        "--crate",
        action="append",
        dest="crates",
        metavar="NAME",
        help="include sources for named crate (repeatable)",
    )
    p_report.add_argument(
        "sources",
        nargs="*",
        type=Path,
        help="source file or directory",
    )

    # LIST
    _p_list = commands.add_parser("list", help="list coverage artifacts")

    # CLEAN
    p_clean = commands.add_parser("clean", help="wipe coverage artifacts")
    p_clean.add_argument(
        "--report",
        action="store_true",
        help="pick generated report",
    )
    p_clean.add_argument(
        "--prof",
        action="store_true",
        help="pick *.profdata & *.profraw",
    )

    args = parser.parse_args()
    state = State(
        cwd=Path.cwd(),
        top_dir=args.dir,
        profraw_prefix=args.profraw_prefix,
    )
    match args.subparser_name:
        case "run":
            state.do_run(args)
        case "merge":
            state.do_merge(args)
        case "report":
            state.do_report(args)
        case "list":
            state.do_list(args)
        case "clean":
            state.do_clean(args)


if __name__ == "__main__":
    main()
