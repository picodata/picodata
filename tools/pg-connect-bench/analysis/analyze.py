"""Compare two program versions across concurrency levels.

For each concurrency level we test, independently for throughput and p99
connection latency, whether the two versions (e.g. ``master`` vs ``rust``)
produce the same distribution.

Statistical approach
--------------------

The samples are small (n=30 per group) and benchmark data is typically
non-normal (skewed, heavy-tailed), so we use the **Mann-Whitney U test**
as the primary test of "are the distributions the same?". It does not
assume normality and is sensitive to a general shift in distribution.

Because with n=30 a statistically significant p-value can still
correspond to a trivially small difference, we also report two effect
size measures:

* **Cliff's delta** -- a non-parametric effect size in ``[-1, 1]``
  describing the probability that a random draw from one group exceeds
  a random draw from the other. Common thresholds (Romano et al.):
  |d| < 0.147 negligible, < 0.33 small, < 0.474 medium, otherwise large.
* **Hodges-Lehmann shift** -- the median of all pairwise differences,
  a robust point estimate of how much one version is shifted relative
  to the other, in the original units.

For familiarity we additionally report **Welch's t-test** (no equal-variance
assumption). It should agree with the Mann-Whitney result; if it does not,
trust the non-parametric result.

Since we run 14 tests per metric (7 concurrency levels x 2 metrics), we
apply the **Holm-Bonferroni** correction over all 14 tests to control the
family-wise error rate.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy import stats

ALPHA = 0.05

METRICS = {
    "throughput": {
        "label": "Throughput (conn/s)",
        "better": "higher",
    },
    "p99_conn_time": {
        "label": "p99 connection time (ms)",
        "better": "lower",
    },
}


@dataclass
class TestResult:
    metric: str
    concurrency: int
    group_a: str
    group_b: str
    n_a: int
    n_b: int
    median_a: float
    median_b: float
    mean_a: float
    mean_b: float
    std_a: float
    std_b: float
    mw_u: float
    mw_p: float
    welch_t: float
    welch_p: float
    cliffs_delta: float
    cliffs_delta_magnitude: str
    hl_shift: float  # median(b - a); positive => b > a
    holm_p: float = float("nan")
    significant: bool = False


def cliffs_delta(a: np.ndarray, b: np.ndarray) -> float:
    """Cliff's delta = P(a>b) - P(a<b). Computed via Mann-Whitney U."""
    n_a, n_b = len(a), len(b)
    # U statistic counts pairs where a > b (ties contribute 0.5)
    u, _ = stats.mannwhitneyu(a, b, alternative="two-sided")
    # delta = 2U/(n_a*n_b) - 1
    return 2.0 * u / (n_a * n_b) - 1.0


def cliffs_magnitude(d: float) -> str:
    ad = abs(d)
    if ad < 0.147:
        return "negligible"
    if ad < 0.33:
        return "small"
    if ad < 0.474:
        return "medium"
    return "large"


def hodges_lehmann_shift(a: np.ndarray, b: np.ndarray) -> float:
    """Median of all pairwise differences b - a (positive => b > a)."""
    diffs = np.subtract.outer(b, a).ravel()
    return float(np.median(diffs))


def holm_bonferroni(pvals: list[float], alpha: float = ALPHA) -> tuple[list[float], list[bool]]:
    """Return (adjusted_pvals, reject) using Holm-Bonferroni."""
    m = len(pvals)
    order = np.argsort(pvals)
    adj = [0.0] * m
    reject = [False] * m
    running_max = 0.0
    for rank, idx in enumerate(order):
        factor = m - rank
        adj_p = min(1.0, pvals[idx] * factor)
        running_max = max(running_max, adj_p)
        adj[idx] = running_max
        reject[idx] = running_max < alpha
    return adj, reject


def run_tests(df: pd.DataFrame, group_a: str, group_b: str) -> pd.DataFrame:
    rows: list[TestResult] = []
    for metric in METRICS:
        for concurrency, sub in df.groupby("concurrency"):
            a = sub.loc[sub["experiment"] == group_a, metric].to_numpy(dtype=float)
            b = sub.loc[sub["experiment"] == group_b, metric].to_numpy(dtype=float)
            if len(a) == 0 or len(b) == 0:
                continue
            mw_u, mw_p = stats.mannwhitneyu(a, b, alternative="two-sided")
            welch_t, welch_p = stats.ttest_ind(a, b, equal_var=False)
            d = cliffs_delta(a, b)
            rows.append(
                TestResult(
                    metric=metric,
                    concurrency=int(concurrency),
                    group_a=group_a,
                    group_b=group_b,
                    n_a=len(a),
                    n_b=len(b),
                    median_a=float(np.median(a)),
                    median_b=float(np.median(b)),
                    mean_a=float(np.mean(a)),
                    mean_b=float(np.mean(b)),
                    std_a=float(np.std(a, ddof=1)),
                    std_b=float(np.std(b, ddof=1)),
                    mw_u=float(mw_u),
                    mw_p=float(mw_p),
                    welch_t=float(welch_t),
                    welch_p=float(welch_p),
                    cliffs_delta=d,
                    cliffs_delta_magnitude=cliffs_magnitude(d),
                    hl_shift=hodges_lehmann_shift(a, b),
                )
            )

    results = pd.DataFrame([r.__dict__ for r in rows])

    # Holm-Bonferroni across all tests (both metrics, all concurrencies).
    adj, reject = holm_bonferroni(results["mw_p"].tolist(), alpha=ALPHA)
    results["holm_p"] = adj
    results["significant"] = reject
    return results


def format_results_table(results: pd.DataFrame) -> str:
    """Pretty text table."""
    lines: list[str] = []
    for metric, sub in results.groupby("metric"):
        info = METRICS[metric]
        lines.append("")
        lines.append(f"=== {info['label']}  (better = {info['better']}) ===")
        header = (
            f"{'conc':>4} | "
            f"{'median A':>10} {'median B':>10} | "
            f"{'HL shift (B-A)':>15} | "
            f"{'Cliff d':>8} ({'mag':>10}) | "
            f"{'MW p':>10} {'Holm p':>10}  {'sig?':>4} | "
            f"{'Welch p':>10}"
        )
        lines.append(header)
        lines.append("-" * len(header))
        for _, r in sub.sort_values("concurrency").iterrows():
            lines.append(
                f"{int(r['concurrency']):>4} | "
                f"{r['median_a']:>10.4f} {r['median_b']:>10.4f} | "
                f"{r['hl_shift']:>+15.4f} | "
                f"{r['cliffs_delta']:>+8.3f} ({r['cliffs_delta_magnitude']:>10}) | "
                f"{r['mw_p']:>10.3g} {r['holm_p']:>10.3g}  "
                f"{('YES' if r['significant'] else 'no'):>4} | "
                f"{r['welch_p']:>10.3g}"
            )
    return "\n".join(lines)


def make_figure(df: pd.DataFrame, results: pd.DataFrame, group_a: str, group_b: str) -> go.Figure:
    metrics = list(METRICS.keys())
    fig = make_subplots(
        rows=len(metrics),
        cols=1,
        subplot_titles=[METRICS[m]["label"] for m in metrics],
        vertical_spacing=0.12,
    )

    colors = {group_a: "#1f77b4", group_b: "#ff7f0e"}

    for row, metric in enumerate(metrics, start=1):
        for grp in (group_a, group_b):
            sub = df[df["experiment"] == grp]
            fig.add_trace(
                go.Box(
                    x=sub["concurrency"],
                    y=sub[metric],
                    name=grp,
                    legendgroup=grp,
                    showlegend=(row == 1),
                    marker_color=colors[grp],
                    boxmean=True,
                    boxpoints="all",
                    jitter=0.5,
                    pointpos=0,
                    marker=dict(size=3, opacity=0.5),
                    line=dict(width=1),
                ),
                row=row,
                col=1,
            )

        # Significance annotation per concurrency.
        metric_res = results[results["metric"] == metric]
        ymax = df[metric].max()
        ymin = df[metric].min()
        yrange = ymax - ymin
        y_anno = ymax + 0.06 * yrange
        for _, r in metric_res.iterrows():
            if r["significant"]:
                label = "*" if r["holm_p"] >= 0.01 else ("**" if r["holm_p"] >= 0.001 else "***")
                text = f"{label}<br>d={r['cliffs_delta']:+.2f}"
            else:
                text = f"ns<br>d={r['cliffs_delta']:+.2f}"
            fig.add_annotation(
                x=r["concurrency"],
                y=y_anno,
                text=text,
                showarrow=False,
                font=dict(size=10),
                row=row,
                col=1,
            )
        fig.update_yaxes(range=[ymin - 0.05 * yrange, ymax + 0.18 * yrange], row=row, col=1)
        fig.update_yaxes(title_text=METRICS[metric]["label"], row=row, col=1)

    fig.update_xaxes(title_text="concurrency", dtick=1)
    fig.update_layout(
        boxmode="group",
        title=(
            f"Performance comparison: <b>{group_a}</b> vs <b>{group_b}</b> "
            f"(Mann-Whitney U, Holm-Bonferroni corrected; "
            f"*** p<0.001, ** p<0.01, * p<0.05, ns = not significant; "
            f"d = Cliff's delta)"
        ),
        height=850,
        template="plotly_white",
        legend=dict(orientation="h", y=1.06, x=0.5, xanchor="center"),
    )
    return fig


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", nargs="?", default="trials.csv", type=Path)
    parser.add_argument(
        "--group-a",
        default=None,
        help="First experiment name (default: first alphabetically).",
    )
    parser.add_argument(
        "--group-b",
        default=None,
        help="Second experiment name (default: second alphabetically).",
    )
    parser.add_argument("--html", default="report.html", type=Path)
    parser.add_argument("--results-csv", default="results.csv", type=Path)
    parser.add_argument("--show", action="store_true", help="Open the plot in a browser.")
    args = parser.parse_args()

    df = pd.read_csv(args.csv)

    required = {"experiment", "concurrency", "throughput", "p99_conn_time"}
    missing = required - set(df.columns)
    if missing:
        sys.stderr.write(f"CSV missing columns: {sorted(missing)}\n")
        return 2

    experiments = sorted(df["experiment"].unique())
    if len(experiments) < 2:
        sys.stderr.write(f"Need at least two experiments, found: {experiments}\n")
        return 2
    group_a = args.group_a or experiments[0]
    group_b = args.group_b or experiments[1]
    for g in (group_a, group_b):
        if g not in experiments:
            sys.stderr.write(f"Experiment {g!r} not in data. Available: {experiments}\n")
            return 2

    print(f"Comparing {group_a!r} (A) vs {group_b!r} (B)")
    print(f"Concurrency levels: {sorted(df['concurrency'].unique())}")
    print(f"Trials per (experiment, concurrency):")
    print(df.groupby(["experiment", "concurrency"]).size().unstack(fill_value=0).to_string())

    results = run_tests(df, group_a, group_b)
    print(format_results_table(results))

    print("\nHL shift is median(B - A) in original units. Positive means B is larger.")
    print("Cliff's delta sign: positive => A tends to exceed B; negative => B tends to exceed A.")

    results.to_csv(args.results_csv, index=False)
    print(f"\nWrote per-test results to {args.results_csv}")

    fig = make_figure(df, results, group_a, group_b)
    fig.write_html(args.html, include_plotlyjs="cdn")
    print(f"Wrote interactive report to {args.html}")
    if args.show:
        fig.show()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
