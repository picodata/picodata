#!/usr/bin/env python3
"""
Fuzz a picodata build with SQLancer's picodata driver.

Deploys a single instance with the test framework, runs the fuzzer against it,
and leaves everything needed to reproduce a finding under the output directory:
the fuzzer logs (the failing database's log replays as a repro), the instance's
data directory with its log, snap and xlog files, and run-info.txt with the
versions and the command line.

Exit codes: 0 nothing found, 1 a finding, a crash or a hang, 2 the run never got
off the ground (no binary, build failed, instance did not come up).
"""

import argparse
import logging
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time

import yaml
from pathlib import Path
from typing import List, Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent
# Everything the fuzzer needs and produces, beside the build it tests.
WORK_DIR = ROOT / "target" / "sqlancer"
# Deployment, readiness and teardown come from the pytest suite's machinery.
sys.path.insert(0, str(ROOT / "test"))

from conftest import Cluster, Instance  # noqa: E402
from framework.log import log as framework_log  # noqa: E402
from framework.port_distributor import PortDistributor  # noqa: E402
from framework.util import BASE_HOST  # noqa: E402
from framework.util.build import Executable  # noqa: E402
from framework.util.git import project_git_version  # noqa: E402
from framework.util.version import VersionAlias  # noqa: E402

SQLANCER_SSH_REPO = "git@git.picodata.io:core/sqlancer.git"
SQLANCER_HTTPS_REPO = "https://git.picodata.io/core/sqlancer.git"

# SQLancer applies these when --username and --password are absent, so the
# instance has to offer exactly this user.
USER = "sqlancer"
PASSWORD = "Passw0rd!"
GRANTS = ["CREATE TABLE", "READ TABLE", "WRITE TABLE", "ALTER TABLE", "DROP TABLE"]

MEMTX_MEMORY = "2G"

# `cargo build --profile=dev` writes to target/debug, hence the odd pair.
MAKE_TARGET = {"release": "build-release", "fast-release": "build-fast-release", "debug": "build-dev"}

EXIT_FINDING = 1
EXIT_SETUP = 2
EXIT_INTERRUPTED = 130

# "[2026/09/23 15:37:48] Executed 44681 queries (783 queries/s; 0.00/s dbs,
#  successful statements: 94%). Threads shut down: 0."
PROGRESS = re.compile(r"Executed (\d+) queries \(.*successful statements: *(\d+)%")
# SQLancer failing to run is not a picodata finding, and CI must not file it as one.
BROKEN_FUZZER = re.compile(r"NoClassDefFoundError|ClassNotFoundException|NoSuchMethodError|OutOfMemoryError")

# Set once the artifact directory exists, so every exit path can point at it.
ARTIFACTS: Optional[Path] = None
PG_PORT = 4327


CATALOGUE = Path(__file__).with_name("sqlancer-errors.yaml")


class SetupError(Exception):
    """Anything that stops the fuzzer from starting; reported as exit code 2."""


def check_call(cmd: List[str], **kwargs) -> None:
    """Run a command, printing it first for observability."""
    print("$", " ".join(str(x) for x in cmd), flush=True)
    subprocess.check_call(cmd, **kwargs)


def log_section(name: str, description: Optional[str] = None) -> None:
    """Fold the output in the CI log; prints a plain header outside CI."""
    if shutil.which("ci-log-section") is None:
        if description:
            print(f"=== {description}", flush=True)
        return
    args = ["ci-log-section", "start", name, description] if description else ["ci-log-section", "end", name]
    subprocess.call(args)


def find_picodata(given: Optional[str], target: str) -> Path:
    if given:
        # A bare name means the installed picodata, as for any other command.
        located = given if os.sep in given else shutil.which(given)
        if located is None:
            raise SetupError(f"{given} is not on PATH")
        picodata = Path(located).resolve()
        if not os.access(picodata, os.X_OK):
            raise SetupError(f"{picodata} is not executable")
        return picodata
    built = ROOT / "target" / target / "picodata"
    if not os.access(built, os.X_OK):
        log_section("picodata-build", f"Building picodata: no {built} yet ...")
        check_call(["make", MAKE_TARGET[target]], cwd=ROOT)
        log_section("picodata-build")
    if not os.access(built, os.X_OK):
        raise SetupError(f"make {MAKE_TARGET[target]} left no binary at {built}")
    return built


def default_repo() -> str:
    # core/sqlancer is private: locally that means ssh, in CI the job token.
    token = os.environ.get("CI_JOB_TOKEN")
    if token:
        return SQLANCER_HTTPS_REPO.replace("https://", f"https://gitlab-ci-token:{token}@")
    return SQLANCER_SSH_REPO


def fetch_sqlancer(src: Path, repo: str, ref: str) -> None:
    """
    Check the pinned ref out into the tool's own directory. Only ever called for
    that directory: a checkout the developer named with --src is built as it is.
    """
    if not (src / ".git").exists():
        check_call(["git", "init", "-q", str(src)])
        check_call(["git", "-C", str(src), "remote", "add", "origin", repo])
    else:
        check_call(["git", "-C", str(src), "remote", "set-url", "origin", repo])
    check_call(["git", "-C", str(src), "fetch", "-q", "--depth", "1", "origin", ref])
    check_call(["git", "-C", str(src), "checkout", "-q", "--detach", "FETCH_HEAD"])


def build_sqlancer(src: Path) -> Path:
    # -q leaves warnings and errors, dropping ~800 lines of INFO per build.
    # A cacheable repository elsewhere is MAVEN_OPTS="-Dmaven.repo.local=...".
    check_call(["mvn", "-B", "-q", "-f", str(src / "pom.xml"), "package", "-DskipTests"])
    # The jar's manifest classpath points at lib/ next to it, which `package` fills.
    jars = sorted((src / "target").glob("sqlancer-*.jar"))
    if not jars:
        raise SetupError(f"no sqlancer jar in {src / 'target'}")
    return jars[0]


def pin_jar(jar: Path, out: Path) -> Path:
    """
    Copy the jar into the run's own directory and link the dependencies beside
    it. The JVM loads classes lazily, so a rebuild of the checkout mid-run — a
    second fuzz run, or a developer rebuilding — makes the running fuzzer fail
    with NoClassDefFoundError. The 3 MB copy is what makes runs independent; the
    441 MB of dependency jars are only ever rewritten with identical content, so
    a symlink is enough for those.
    """
    pinned = out / "sqlancer.jar"
    shutil.copy2(jar, pinned)
    lib = jar.parent / "lib"
    if lib.is_dir():
        # An absolute target: a relative one would resolve inside `out` and dangle.
        (out / "lib").symlink_to(lib.resolve())
    return pinned


def read_catalogue(path: Path) -> Tuple[int, int]:
    """
    Count what the error catalogue holds, for the summary. SQLancer owns the
    schema and rejects a file that breaks it before it connects, so this only
    turns a missing or unreadable file into a setup error rather than a finding.
    """
    if not path.exists():
        raise SetupError(f"no error catalogue at {path}; SQLancer needs it to tell a known error from a finding")
    try:
        document = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as e:
        raise SetupError(f"{path} is not valid YAML: {e}") from e
    if not isinstance(document, dict):
        raise SetupError(f"{path}: expected a mapping with the keys errors and suppress")
    return len(document.get("errors") or []), len(document.get("suppress") or [])


def claim_output_dir(out: Path) -> None:
    """
    Wipe and take the output directory, unless another run holds it: the tree is
    recreated from scratch every run, so two runs sharing it delete each other's
    artifacts while the instances are still writing them.
    """
    lock = out / ".lock"
    if lock.exists():
        try:
            holder = int(lock.read_text().strip())
            os.kill(holder, 0)
        except (ValueError, ProcessLookupError, PermissionError):
            holder = None
        if holder:
            raise SetupError(f"{out} belongs to the run with pid {holder}; pass --out to use another directory")
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    lock.write_text(f"{os.getpid()}\n")


def check_port_free(port: int) -> None:
    """pgproto's port is fixed rather than allocated, so a stale instance blocks it."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind((BASE_HOST, port))
        except OSError as e:
            raise SetupError(f"port {port} is busy ({e}); another fuzz run, or pass --pg-port") from e


def make_cluster(data_dir: Path, args: argparse.Namespace) -> Cluster:
    data_dir.mkdir(parents=True, exist_ok=True)
    ports = PortDistributor(start=args.base_port, end=args.base_port + 100)
    cluster = Cluster(
        name="sqlancer",
        data_dir=str(data_dir),
        base_host=BASE_HOST,
        port_distributor=ports,
    )
    cluster.set_service_password("password")
    return cluster


def deploy_instance(cluster: Cluster, picodata: Path, args: argparse.Namespace) -> Instance:
    """Deploy one instance with pgproto listening on --pg-port."""
    data_dir = Path(cluster.data_dir)
    executable = Executable(project_git_version(), VersionAlias.CURRENT, picodata)
    # The instance's log reaches the artifact tree either way; on the console it
    # buries the fuzzer's own output.
    instance = cluster.add_instance(
        wait_online=False,
        pg_port=args.pg_port,
        executable=executable,
        log_to_console=args.verbose,
        log_to_file=True,
        # A bool sends the audit log to stderr; a path keeps it in the artifacts.
        audit=True if args.verbose else str(data_dir / "audit.log"),
    )
    # The default 64 MiB runs out well before a long fuzzing run does.
    instance.env["PICODATA_MEMTX_MEMORY"] = MEMTX_MEMORY
    cluster.wait_online()
    return instance


def create_sqlancer_user(instance: Instance) -> None:
    instance.create_user(with_name=USER, with_password=PASSWORD, with_auth="md5")
    for grant in GRANTS:
        # sudo: instance.sql connects as pico_service, which may not grant.
        instance.sql(f'GRANT {grant} TO "{USER}"', sudo=True)


def is_debug_build(picodata: Path) -> bool:
    """`picodata --version` ends with the build profile, e.g. `static, debug`."""
    version = subprocess.check_output([str(picodata), "--version"], text=True).splitlines()[0]
    return version.strip().endswith("debug")


def sqlancer_command(jar: Path, args: argparse.Namespace, debug_build: bool = False) -> List[str]:
    cmd = [
        "java",
        "-jar",
        str(jar),
        # --num-tries is the number of workers, each stopping at its own first
        # finding, so one worker makes the run stop at the first one overall,
        # with the instance still in the state that produced it.
        "--num-threads",
        "1",
        "--num-tries",
        "1",
        "--timeout-seconds",
        str(args.duration),
        "--host",
        BASE_HOST,
        "--port",
        str(args.pg_port),
    ]
    if args.seed:
        cmd += ["--random-seed", args.seed]
    if debug_build and "--max-expression-depth" not in args.sqlancer_args:
        # A debug build overflows the stack of the recursive expression parser
        # on the default depth of 3, segfaulting within half a minute; at 2 it
        # fuzzed 241k queries in 20 minutes without dying.
        cmd += ["--max-expression-depth", "2"]
    cmd += args.sqlancer_args.split()
    cmd.append("picodata")
    for oracle in args.oracle.split():
        cmd += ["--oracle", oracle]
    # Before --driver-args, so that a developer can point a single run at another catalogue: for a
    # single-valued option JCommander keeps the last one given.
    cmd += ["--allowed-errors", str(args.errors)]
    cmd += args.driver_args.split()
    return cmd


def run_sqlancer(cmd: List[str], out: Path, timeout: int) -> tuple[int, Optional[re.Match]]:
    """
    Stream the fuzzer's output to the console and to sqlancer.out, keeping its
    last progress line: SQLancer only reports totals there, every five seconds.
    """
    print("$", " ".join(cmd), flush=True)
    progress = None
    with open(out / "sqlancer.out", "w") as log:
        # SQLancer writes logs/ relative to its working directory.
        fuzzer = subprocess.Popen(cmd, cwd=out, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        assert fuzzer.stdout is not None
        for line in fuzzer.stdout:
            # The jar carries several logging backends, and slf4j says so on
            # every run; keep it in the file, out of the console.
            if not line.startswith("SLF4J: "):
                sys.stdout.write(line)
                sys.stdout.flush()
            log.write(line)
            progress = PROGRESS.search(line) or progress
        try:
            # SQLancer bounds itself with --timeout-seconds; this only catches a
            # run that stops respecting it.
            return fuzzer.wait(timeout=timeout), progress
        except subprocess.TimeoutExpired:
            fuzzer.kill()
            print(f"sqlancer overran --timeout-seconds by {timeout}s and was killed", file=sys.stderr)
            return EXIT_FINDING, progress
        except BaseException:  # an interrupt leaves no fuzzer behind either
            fuzzer.kill()
            raise


class Tee:
    """
    A stdout/stderr stand-in that also writes to a file. The framework's relay
    threads write the instance's output straight to `sys.stdout.buffer`, so a
    handler on the logger never sees it; our own output goes through `write`.
    That split is what `echo_instance` switches: this tool always speaks on the
    console, the instance only when asked.
    """

    def __init__(self, stream, path: Path, echo_instance: bool):
        self.stream = stream
        self.file = open(path, "ab", buffering=0)
        self.buffer = _TeeBuffer(stream.buffer, self.file, echo_instance)

    def write(self, text: str) -> int:
        self.stream.write(text)
        self.file.write(text.encode())
        return len(text)

    def flush(self) -> None:
        self.stream.flush()

    def isatty(self) -> bool:
        return self.stream.isatty()


class _TeeBuffer:
    def __init__(self, stream_buffer, file, echo_instance: bool):
        self.stream_buffer = stream_buffer
        self.file = file
        self.echo = echo_instance

    def write(self, data: bytes) -> int:
        if self.echo:
            self.stream_buffer.write(data)
        self.file.write(data)
        return len(data)

    def flush(self) -> None:
        if self.echo:
            self.stream_buffer.flush()


def find_repo(path: Path) -> Optional[Path]:
    return next((parent for parent in [path, *path.parents] if (parent / ".git").exists()), None)


def describe_checkout(path: Path) -> str:
    """Where a checkout stands right now: `<repo>@<sha> (<branch>, dirty)`."""
    repo = find_repo(path)
    if repo is None:
        return "not a git checkout"
    head = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "--short", "HEAD"], text=True).strip()
    branch = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "--abbrev-ref", "HEAD"], text=True).strip()
    dirty = subprocess.check_output(["git", "-C", str(repo), "status", "--porcelain"], text=True).strip()
    return f"{repo}@{head} ({branch}{', uncommitted changes' if dirty else ''})"


def write_run_info(out: Path, picodata: Path, src: Path, cmd: List[str], ref: Optional[str]) -> None:
    version = subprocess.check_output([str(picodata), "--version"], text=True).splitlines()[0]
    info = out / "run-info.txt"
    info.write_text(
        f"picodata: {version}\n"
        f"binary:   {picodata}\n"
        f"sqlancer: {describe_checkout(src)}{f' as {ref}' if ref else ''}\n"
        f"command:  {' '.join(cmd)}\n"
    )
    print(info.read_text(), flush=True)


def count_statements(out: Path) -> int:
    """
    Statements in the fuzzer's own statement log, for runs too short for a
    progress report. The FUZZER oracle logs each query twice, once when it
    generates it and once when it runs it, so identical neighbours count once.
    """
    total = 0
    for log in out.glob("logs/picodata/database*-cur.log"):
        previous = None
        for line in log.read_text(errors="replace").splitlines():
            if line and line != previous:
                total += 1
            previous = line
    return total


def find_cause(out: Path) -> Optional[str]:
    """The error SQLancer reported, as its database log records it."""
    for log in sorted(out.glob("logs/picodata/database*.log")):
        if log.name.endswith("-cur.log"):
            continue
        for line in log.read_text(errors="replace").splitlines():
            if "Caused by:" in line:
                return line.lstrip("-").strip()
    return None


def find_fuzzer_error(out: Path) -> Optional[str]:
    """The exception SQLancer died of, for a run that never reached the database."""
    log = out / "sqlancer.out"
    if not log.exists():
        return None
    for line in log.read_text(errors="replace").splitlines():
        if line.startswith("Exception in thread") or "Exception:" in line:
            return line.strip()
    return None


def findings(out: Path) -> List[Path]:
    """The database logs SQLancer writes when it finds something."""
    return [log for log in sorted(out.glob("logs/picodata/database*.log")) if not log.name.endswith("-cur.log")]


def summarize(
    status: int,
    seconds: float,
    progress: Optional[re.Match],
    crashed: bool,
    out: Path,
    cause: Optional[str],
    catalogue: Tuple[Path, int, int],
) -> str:
    """One verdict line and the run's numbers, for the console and run-info.txt."""
    if status == 0:
        lines = [f"PASSED: no findings in {seconds:.0f}s"]
    elif status == EXIT_SETUP:
        lines = [f"BROKEN: SQLancer itself failed after {seconds:.0f}s; this is not a picodata finding"]
    elif crashed:
        lines = [f"FAILED: picodata died after {seconds:.0f}s"]
    else:
        lines = [f"FAILED: SQLancer reported a finding after {seconds:.0f}s"]

    if progress:
        queries, success_rate = int(progress.group(1)), int(progress.group(2))
        lines.append(f"  queries:       {queries} ({queries / max(seconds, 1):.0f}/s)")
        lines.append(f"  success rate:  {success_rate}% of statements")
    else:
        # SQLancer reports its totals every five seconds and nothing before that.
        statements = count_statements(out)
        if statements:
            lines.append(
                f"  statements:    {statements} ({statements / max(seconds, 1):.0f}/s), counted from the statement log"
            )
            lines.append("  success rate:  unknown, the run was shorter than one progress report")

    # What the run was allowed to ignore, so that a green run says what it covered.
    path, patterns, suppressions = catalogue
    lines.append(f"  catalogue:     {path} ({patterns} errors allowed, {suppressions} patterns suppressed)")

    if cause:
        label = "error" if status == EXIT_SETUP else "finding"
        lines.append(f"  {label + ':':<14} {cause}")
    return "\n".join(lines)


def report_artifacts() -> None:
    """Point at the artifacts, whichever way the run ended."""
    out = ARTIFACTS
    if out is None:
        return

    def describe(path: Path, what: str) -> None:
        if path.exists():
            print(f"  {str(path.relative_to(out)):<38} {what}")

    found = findings(out)

    print(f"\nartifacts: {out}", flush=True)
    describe(out / "run-info.txt", "versions, command line, exit code")
    describe(out / "sqlancer.out", "the fuzzer's output")
    for log in found:
        describe(log, "the schema, then the failing query; replays with psql")
    for log in sorted(out.glob("logs/picodata/database*-cur.log")):
        describe(log, "every statement as executed; after a crash, the last is the culprit")
    describe(out / "instance-console.log", "what the instance printed, including a panic or abort")
    for log in sorted(out.glob("cluster/*/picodata.log")):
        # The framework symlinks the instance's name to its port directory.
        if not log.parent.is_symlink():
            describe(log, "instance log, beside its snap and xlog files")

    if found:
        print(
            "\nreplay it on an instance without the run's tables:\n"
            f"  PGPASSWORD='{PASSWORD}' psql -h {BASE_HOST} -p {PG_PORT} -U {USER} -d {USER} -f {found[0]}",
            flush=True,
        )


def parse_args() -> argparse.Namespace:
    env = os.environ.get
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--picodata",
        default=env("PICODATA"),
        help="binary to fuzz; overrides --target",
    )
    parser.add_argument(
        "--target",
        choices=["release", "fast-release", "debug"],
        default=env("SQLANCER_TARGET", "release"),
        help="which build under target/ to fuzz",
    )
    parser.add_argument("--jar", default=env("SQLANCER_JAR"), help="prebuilt jar; default: build the one in --src")
    parser.add_argument(
        "--src",
        type=Path,
        default=Path(env("SQLANCER_SRC")) if env("SQLANCER_SRC") else None,
        help="sqlancer checkout to build and run as it stands; without it, --ref is fetched",
    )
    parser.add_argument("--repo", default=env("SQLANCER_REPO", default_repo()), help="remote to fetch --ref from")
    parser.add_argument("--ref", default=env("SQLANCER_REF", "picodata"), help="commit or branch to fuzz with")
    parser.add_argument("--oracle", default=env("SQLANCER_ORACLE", "FUZZER"), help="space-separated oracle names")
    parser.add_argument("--duration", type=int, default=int(env("SQLANCER_DURATION", "300")), help="seconds to fuzz")
    parser.add_argument(
        "--seed", default=env("SQLANCER_SEED"), help="replay a run; raft timing is not seeded, so best-effort"
    )
    parser.add_argument(
        "--errors",
        type=Path,
        default=Path(env("SQLANCER_ERRORS", str(CATALOGUE))),
        help="the error catalogue to test against; default: tools/sqlancer-errors.yaml",
    )
    parser.add_argument(
        "--sqlancer-args", default=env("SQLANCER_ARGS", ""), help="sqlancer's own options, e.g. --num-queries 100"
    )
    parser.add_argument(
        "--driver-args",
        default=env("SQLANCER_DRIVER_ARGS", ""),
        help="the picodata driver's options, e.g. --test-global-tables false",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(env("SQLANCER_OUT", str(WORK_DIR / "artifacts"))),
        help="artifact directory, wiped at start",
    )
    parser.add_argument("--pg-port", type=int, default=int(env("SQLANCER_PG_PORT", "4327")))
    parser.add_argument(
        "--base-port", type=int, default=int(env("SQLANCER_BASE_PORT", "3300")), help="iproto port pool"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=bool(env("SQLANCER_VERBOSE")),
        help="print the instance's log, its audit events and the framework's own steps",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # SIGTERM (a cancelled CI job) has to unwind like Ctrl-C does, not exit in place.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(EXIT_INTERRUPTED))

    # Absolute from here on: the instance runs with its own directory as its
    # working directory, so a relative path we hand out resolves twice.
    args.out = args.out.resolve()
    args.errors = Path(args.errors).resolve()
    if args.src:
        args.src = args.src.resolve()

    picodata = find_picodata(args.picodata, args.target)
    check_port_free(args.pg_port)
    claim_output_dir(args.out)
    global ARTIFACTS, PG_PORT
    ARTIFACTS = args.out
    PG_PORT = args.pg_port
    # The framework relays the instance's stdout and stderr to ours, and that is
    # where a panic or a fault report appears, so keep a copy in the artifacts.
    sys.stdout = Tee(sys.stdout, args.out / "instance-console.log", echo_instance=args.verbose)
    sys.stderr = Tee(sys.stderr, args.out / "instance-console.log", echo_instance=args.verbose)
    if not args.verbose:
        # The framework also narrates every deploy and shutdown step at INFO and
        # polls the starting instance at WARNING until it answers.
        framework_log.setLevel(logging.ERROR)

    # A checkout the developer names is built as it stands, uncommitted changes included: fetching into it
    # would check out over the branch they are working on.
    src = args.src or WORK_DIR / "src"
    fetched = args.src is None
    if not args.jar and fetched:
        log_section("sqlancer-clone", f"Cloning {args.repo} at {args.ref} into {src} ...")
        fetch_sqlancer(src, args.repo, args.ref)
        log_section("sqlancer-clone")
    if args.jar:
        jar = Path(args.jar).resolve()
    else:
        log_section("sqlancer-build", f"Building SQLancer from {src} ...")
        jar = build_sqlancer(src)
        log_section("sqlancer-build")
    jar = pin_jar(jar, args.out)

    log_section("sqlancer-deploy", "Deploying a single instance ...")
    cluster = make_cluster(args.out / "cluster", args)
    try:
        instance = deploy_instance(cluster, picodata, args)
        log_section("sqlancer-deploy")
        create_sqlancer_user(instance)

        patterns, suppressions = read_catalogue(args.errors)
        cmd = sqlancer_command(jar, args, is_debug_build(picodata))
        write_run_info(args.out, picodata, src, cmd, args.ref if fetched else None)

        log_section("sqlancer-run", f"Fuzzing for {args.duration}s ...")
        started = time.monotonic()
        # SQLancer reports a finding with --exit-code-error, 255 by default.
        returncode, progress = run_sqlancer(cmd, args.out, args.duration + 600)
        seconds = time.monotonic() - started
        status = EXIT_FINDING if returncode else 0
        log_section("sqlancer-run")

        crashed = instance.process is not None and instance.process.poll() is not None
        if crashed:
            print(f"picodata died during the run (exit {instance.process.returncode})", file=sys.stderr)
            status = EXIT_FINDING
    finally:
        # The instance runs in its own process group, so Ctrl-C does not reach
        # it: only this terminate stands between an interrupt and a stray
        # instance holding the ports.
        cluster.terminate()

    cause = None
    if status:
        cause = find_cause(args.out) or find_fuzzer_error(args.out)
        # No database log to report a finding from, or the JVM gave up: the
        # fuzzer is broken, which is a different failure from a picodata bug.
        if not crashed and (not findings(args.out) or BROKEN_FUZZER.search(cause or "")):
            status = EXIT_SETUP
    summary = summarize(status, seconds, progress, crashed, args.out, cause, (args.errors, patterns, suppressions))
    with open(args.out / "run-info.txt", "a") as info:
        info.write(f"exit:     {status}\n{summary}\n")
    print(f"\n{summary}", flush=True)
    report_artifacts()
    return status


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\ninterrupted; the instance was terminated", file=sys.stderr)
        report_artifacts()
        sys.exit(EXIT_INTERRUPTED)
    except SetupError as e:
        print(f"sqlancer setup failed: {e}", file=sys.stderr)
        report_artifacts()
        sys.exit(EXIT_SETUP)
    except subprocess.CalledProcessError as e:
        print(f"sqlancer setup failed: {' '.join(str(x) for x in e.cmd)} exited with {e.returncode}", file=sys.stderr)
        report_artifacts()
        sys.exit(EXIT_SETUP)
