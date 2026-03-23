"""Plot Fast Commit scalability bar charts from experiment CSV.

Outputs 3 figures:
- fast_commit_read.png
- fast_commit_write.png
- fast_commit_mixed.png

X-axis: site_count
Y-axis: throughput_tx_s
Bars in each site group: tx-size variants for that workload.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt


def _load_rows(csv_path: Path) -> List[Dict[str, str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _float(v: str) -> float:
    return float(v.strip())


def _int(v: str) -> int:
    return int(v.strip())


def _build_series(rows: List[Dict[str, str]], workload: str) -> Tuple[List[int], Dict[str, Dict[int, float]]]:
    filtered = [r for r in rows if r.get("workload") == workload]
    site_counts = sorted({_int(r["site_count"]) for r in filtered})

    series: Dict[str, Dict[int, float]] = {}
    for r in filtered:
        read_sz = _int(r["read_objects_per_tx"])
        write_sz = _int(r["write_objects_per_tx"])
        thpt = _float(r["throughput_tx_s"])
        site = _int(r["site_count"])

        if workload == "read":
            label = f"read_sz={read_sz}"
        elif workload == "write":
            label = f"write_sz={write_sz}"
        else:
            label = f"r{read_sz}/w{write_sz}"

        series.setdefault(label, {})[site] = thpt

    return site_counts, series


def _preferred_order(workload: str, labels: List[str]) -> List[str]:
    if workload == "read":
        preferred = ["read_sz=1", "read_sz=5"]
    elif workload == "write":
        preferred = ["write_sz=1", "write_sz=5"]
    else:
        preferred = ["r1/w1", "r1/w5", "r5/w1", "r5/w5"]

    ordered = [x for x in preferred if x in labels]
    ordered.extend(sorted([x for x in labels if x not in ordered]))
    return ordered


def _plot_one(rows: List[Dict[str, str]], workload: str, output_dir: Path) -> Path:
    site_counts, series = _build_series(rows, workload)
    if not site_counts or not series:
        raise ValueError(f"no data for workload={workload}")

    labels = _preferred_order(workload, list(series.keys()))

    x_positions = list(range(len(site_counts)))
    group_width = 0.82
    bar_width = group_width / max(1, len(labels))

    fig, ax = plt.subplots(figsize=(9, 5.2))

    for idx, label in enumerate(labels):
        offset = -group_width / 2 + (idx + 0.5) * bar_width
        values = [series[label].get(site, 0.0) for site in site_counts]
        x = [p + offset for p in x_positions]
        ax.bar(x, values, width=bar_width * 0.95, label=label)

    ax.set_xticks(x_positions)
    ax.set_xticklabels([str(s) for s in site_counts])
    ax.set_xlabel("Site Count")
    ax.set_ylabel("Throughput (tx/s)")
    ax.set_title(f"Fast Commit Throughput - {workload.capitalize()} Workload")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(title="Tx Size", frameon=False)

    fig.tight_layout()
    out = output_dir / f"fast_commit_{workload}.png"
    fig.savefig(out, dpi=180)
    plt.close(fig)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot fast commit scalability charts")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("WalterSim/lab/experiments/results/csv/fast_commit_scalability.csv"),
        help="input CSV path",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("WalterSim/lab/experiments/results/png"),
        help="directory for output PNG files",
    )
    args = parser.parse_args()

    rows = _load_rows(args.input)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    outputs = []
    for workload in ["read", "write", "mixed"]:
        outputs.append(_plot_one(rows, workload, args.output_dir))

    for out in outputs:
        print(f"saved: {out}")


if __name__ == "__main__":
    main()
