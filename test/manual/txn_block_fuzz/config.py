"""Every knob of the fuzz, read from the environment once at import.

The defaults are documented in the test module's docstring and in
test/manual/README.md.
"""

import os
import random


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name) or default)


DURATION = _env_int("FUZZ_DURATION", 20)
WORKERS = _env_int("FUZZ_WORKERS", 8)
INSTANCES = _env_int("FUZZ_INSTANCES", 3)
RF = _env_int("FUZZ_RF", 3)
ACCOUNTS = _env_int("FUZZ_ACCOUNTS", 6)
GROUPS = _env_int("FUZZ_GROUPS", 3)
SEED = _env_int("FUZZ_SEED", random.randrange(2**31))
NEMESIS_PERIOD = float(os.getenv("FUZZ_NEMESIS_PERIOD") or 5)

ALL_FAULTS = ["kill", "pause", "restart", "leader", "compact"]

INITIAL = 100
MAX_TRANSFER = 20
TOTAL = ACCOUNTS * INITIAL

USER = "fuzz"
PASSWORD = "Passw0rd"


def parse_faults() -> list[str]:
    raw = (os.getenv("FUZZ_NEMESIS") or "none").strip().lower()
    if raw in ("", "none"):
        return []
    if raw == "all":
        return list(ALL_FAULTS)
    faults = [f.strip() for f in raw.split(",") if f.strip()]
    unknown = [f for f in faults if f not in ALL_FAULTS]
    assert not unknown, f"unknown faults {unknown}, expected any of {ALL_FAULTS}"
    return faults


OPS_BY_WORKLOAD = {
    "bank": [("sharded_transfer", 70), ("sharded_read", 30)],
}


def parse_workloads() -> list[str]:
    raw = (os.getenv("FUZZ_WORKLOAD") or "bank").strip().lower()
    workloads = [w.strip() for w in raw.split(",") if w.strip()]
    unknown = [w for w in workloads if w not in OPS_BY_WORKLOAD]
    assert workloads and not unknown, f"unknown workloads {unknown}, expected any of {list(OPS_BY_WORKLOAD)}"
    return workloads


WORKLOADS = parse_workloads()
OPS = [name for workload in WORKLOADS for name, _ in OPS_BY_WORKLOAD[workload]]
WEIGHTS = [weight for workload in WORKLOADS for _, weight in OPS_BY_WORKLOAD[workload]]
