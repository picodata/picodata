"""The summary printed at the end of a run."""

from . import config
from .stats import Stats


def report(stats: Stats, faults: list[str], elapsed: float, history_lines: list[str]):
    def line(text: str = ""):
        print(text, flush=True)

    line()
    line(" transactional block fuzz ".center(72, "="))
    line(
        f"seed={config.SEED} duration={elapsed:.1f}s workers={config.WORKERS} "
        f"workloads={','.join(config.WORKLOADS)} "
        f"instances={config.INSTANCES} rf={config.RF} nemesis={','.join(faults) or 'none'}"
    )
    line()
    line(f"{'operation':<18}{'ok':>10}{'expected err':>16}{'unexpected err':>18}")
    for op in config.OPS:
        line(f"{op:<18}{stats.ok[op]:>10}{stats.expected[op]:>16}{stats.unexpected[op]:>18}")
    total_ok = sum(stats.ok.values())
    line(f"{'total':<18}{total_ok:>10}{sum(stats.expected.values()):>16}{sum(stats.unexpected.values()):>18}")
    line(f"throughput: {total_ok / max(elapsed, 1e-9):.0f} blocks/s")
    if stats.not_healed:
        line(f"cluster did not come back: {stats.not_healed}")
    if stats.faults:
        line("faults: " + ", ".join(f"{name}x{count}" for name, count in sorted(stats.faults.items())))
    if stats.stopped_early:
        line(f"stopped early: {stats.stopped_early}")
    line()

    for text in history_lines:
        line(text)
    if history_lines:
        line()

    for sample in stats.unexpected_samples:
        line(f"unexpected error: {sample}")
    for anomaly in stats.anomalies:
        line(f"ANOMALY {anomaly}")

    valid = not stats.anomalies and not stats.unexpected_samples
    # A verdict reached without the final checks says less than a whole one, so
    # it says so: the blocks that ran were consistent, the rest went unchecked.
    incomplete = ""
    if stats.not_healed:
        incomplete = " (incomplete: the cluster did not come back, so the final checks and Elle did not run)"
    line(f"valid? {valid}{incomplete}")
    line("=" * 72)
