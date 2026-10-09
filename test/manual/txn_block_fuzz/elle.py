"""Checking an append history for serializability, and checking the checker."""

import hashlib
import io
import json
import shutil
import subprocess
import urllib.request
import zipfile
from pathlib import Path
from typing import Callable

from framework.log import log

from . import config
from .history import unread
from .stats import Anomaly, Stats

# Elle is a Clojure library without a CLI; elle-cli is the prebuilt wrapper.
# The archive is executed after download, so it is pinned by checksum.
ELLE_CLI_VERSION = "0.1.11"
ELLE_CLI_URL = (
    f"https://github.com/ligurio/elle-cli/releases/download/{ELLE_CLI_VERSION}/elle-cli-bin-{ELLE_CLI_VERSION}.zip"
)
ELLE_CLI_SHA256 = "9d3c72eaf3ccc4d0c7dc1da710264dee01f03af58c9fa177fec3f78ff5af1779"
ELLE_CLI_JAR = f"elle-cli-{ELLE_CLI_VERSION}-standalone.jar"


def elle_cli_jar() -> Path | None:
    """The jar to run: ELLE_CLI_JAR if set, otherwise the cached download.

    None when there is no java to run it with. A failed download raises, which
    is why this runs before the cluster is deployed and not after the workload."""
    if shutil.which("java") is None:
        log.warning("java not found, the Elle check is skipped")
        return None
    if config.ELLE_CLI_JAR:
        return Path(config.ELLE_CLI_JAR)
    jar = config.ELLE_CLI_CACHE / ELLE_CLI_JAR
    if jar.exists():
        return jar

    log.info(f"downloading {ELLE_CLI_URL} to {jar}")
    with urllib.request.urlopen(ELLE_CLI_URL, timeout=120) as response:
        archive = response.read()
    digest = hashlib.sha256(archive).hexdigest()
    assert digest == ELLE_CLI_SHA256, f"{ELLE_CLI_URL}: sha256 is {digest}, expected {ELLE_CLI_SHA256}"
    with zipfile.ZipFile(io.BytesIO(archive)) as zf:
        member = next(name for name in zf.namelist() if name.endswith("/" + ELLE_CLI_JAR))
        jar.parent.mkdir(parents=True, exist_ok=True)
        # Written beside the jar and renamed, so a concurrent run never sees a
        # partial file.
        part = jar.with_suffix(".part")
        part.write_bytes(zf.read(member))
        part.replace(jar)
    return jar


def run_elle(history_path: Path, out_dir: Path, jar: Path) -> dict:
    """Check a list-append history for serializability and return elle-cli's verdict.

    The exit code is not trusted: elle-cli exits 0 when the result is unknown."""
    command = [
        "java",
        f"-Xmx{config.ELLE_HEAP}",
        "-jar",
        str(jar),
        "-m",
        "list-append",
        "-c",
        "serializable",
    ]
    # The default cycle search gives up after 1 s per component, which turns a
    # long history's verdict into unknown.
    command += ["-s", "30000", "-d", str(out_dir), "--output", "json", str(history_path)]
    # Plots need Graphviz, and a missing `dot` crashes elle-cli exactly when it
    # finds a cycle; -b 0 skips plots and keeps the text explanations.
    command += ["-b", "0"]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=1800)
    except subprocess.TimeoutExpired:
        return {"valid?": "timed out"}
    marker = result.stdout.find('"valid?"')
    start = result.stdout.rfind("{", 0, marker) if marker != -1 else -1
    if start == -1:
        return {
            "valid?": f"crashed with exit code {result.returncode}",
            "stderr": result.stderr.strip().splitlines()[-3:],
        }
    verdict, _ = json.JSONDecoder().raw_decode(result.stdout[start:])
    return verdict


def corrupt_duplicate(history: list[dict]) -> bool:
    """Repeat the last element of one read."""
    for record in history:
        if record["type"] == "ok":
            for mop in record["value"]:
                if mop[0] == "r" and mop[2]:
                    mop[2].append(mop[2][-1])
                    return True
    return False


def corrupt_order(history: list[dict]) -> bool:
    """Swap the first two elements of the second read of a key read at length >= 2 twice."""
    seen: set[int] = set()
    for record in history:
        if record["type"] == "ok":
            for mop in record["value"]:
                if mop[0] == "r" and mop[2] and len(mop[2]) >= 2:
                    if mop[1] in seen:
                        mop[2][0], mop[2][1] = mop[2][1], mop[2][0]
                        return True
                    seen.add(mop[1])
    return False


def corrupt_aborted(history: list[dict]) -> bool:
    """Mark a committed transaction failed although a later read saw its append."""
    writers: dict[tuple[int, int], dict] = {}
    for record in history:
        if record["type"] != "ok":
            continue
        for mop in record["value"]:
            if mop[0] == "r" and mop[2]:
                for element in mop[2]:
                    writer = writers.get((mop[1], element))
                    if writer is not None and writer is not record:
                        writer["type"] = "fail"
                        writer["value"] = unread(writer["value"])
                        return True
            elif mop[0] == "append":
                writers[(mop[1], mop[2])] = record
    return False


NEGATIVE_CONTROLS: list[tuple[str, Callable[[list[dict]], bool]]] = [
    ("duplicate-elements", corrupt_duplicate),
    ("incompatible-order", corrupt_order),
    ("G1a", corrupt_aborted),
]


def check_history(history_path: Path, run_dir: Path, stats: Stats, jar: Path | None) -> list[str]:
    if jar is None:
        return ["elle: skipped, java not found"]

    lines = []
    verdict = run_elle(history_path, run_dir / "elle", jar)
    lines.append(f"elle list-append serializable: valid? {verdict.get('valid?')}")
    if verdict.get("valid?") is not True:
        detail = (
            f"valid? {verdict.get('valid?')}, anomaly-types {verdict.get('anomaly-types')}, "
            f"not {verdict.get('not')}, also-not {verdict.get('also-not')}, explanations in {run_dir / 'elle'}"
        )
        stats.record_anomaly(Anomaly("elle", "list-append", detail, "-", str(history_path)))

    for anomaly_type, corrupt in NEGATIVE_CONTROLS:
        with history_path.open() as f:
            history = json.load(f)
        if not corrupt(history):
            detail = "no transaction in the history to corrupt, so the check went unverified"
            stats.record_anomaly(Anomaly("negative-control", anomaly_type, detail, "-", str(history_path)))
            lines.append(f"negative control {anomaly_type}: not constructible")
            continue
        control_path = run_dir / f"control-{anomaly_type}.json"
        with control_path.open("w") as f:
            json.dump(history, f)
        control = run_elle(control_path, run_dir / f"control-{anomaly_type}", jar)
        flagged = control.get("valid?") is False and anomaly_type in (control.get("anomaly-types") or [])
        lines.append(f"negative control {anomaly_type}: {'flagged' if flagged else 'NOT flagged'}")
        if not flagged:
            detail = f"a corrupted history was not flagged: {control}"
            stats.record_anomaly(Anomaly("negative-control", anomaly_type, detail, "-", str(control_path)))
    return lines
