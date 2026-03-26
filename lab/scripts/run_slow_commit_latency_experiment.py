"""Measure slow-commit latency and DS-durable latency for 4-site write-only tx.

Experiment setup (aligned with paper-style description):
- Always run with 4 active sites.
- Clients submit at VA (site 0).
- Transaction size varies in {2, 3, 4} objects.
- Preferred site of object i is fixed by position:
  obj1->VA, obj2->CA, obj3->IE, obj4->SG.

Metrics:
- commit_ms: end-to-end slow 2PC commit latency.
- ds_durable_ms: commit_ms + simplified disaster-safe wait latency.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
import os
from pathlib import Path
import random
import statistics
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Any, Dict, List

import matplotlib.pyplot as plt

LAB_ROOT = Path(__file__).resolve().parent.parent
SITE_DIR = LAB_ROOT / "site"
RESULT_CSV_DIR = LAB_ROOT / "experiments" / "results" / "csv"
RESULT_PNG_DIR = LAB_ROOT / "experiments" / "results" / "png"
if str(LAB_ROOT) not in sys.path:
    sys.path.insert(0, str(LAB_ROOT))
if str(SITE_DIR) not in sys.path:
    sys.path.insert(0, str(SITE_DIR))

from cluster import ClusterManager
from network.rpc import MessageTypes, PersistentRpcClient

_thread_local = threading.local()
PREFERRED_SITES = [0, 1, 2, 3]  # VA, CA, IE, SG


@dataclass
class Sample:
    ok: bool
    commit_mode: str
    write_sz: int
    commit_ms: float
    ds_durable_ms: float


def _percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    ranked = sorted(values)
    idx = min(len(ranked) - 1, max(0, int(round((p / 100.0) * (len(ranked) - 1)))))
    return ranked[idx]


def _make_payload(seed: int, size: int = 100) -> str:
    base = f"slow-ds-{seed:08d}-"
    if len(base) >= size:
        return base[:size]
    return base + ("x" * (size - len(base)))


def _get_client(timeout_seconds: float) -> PersistentRpcClient:
    client = getattr(_thread_local, "client", None)
    if client is None:
        client = PersistentRpcClient(timeout_seconds=timeout_seconds)
        _thread_local.client = client
    return client


def _reset_client(timeout_seconds: float) -> PersistentRpcClient:
    client = getattr(_thread_local, "client", None)
    if client is not None:
        client.close_all()
    client = PersistentRpcClient(timeout_seconds=timeout_seconds)
    _thread_local.client = client
    return client


def _build_objects(write_sz: int, keys_per_site: int, tx_seed: int) -> List[Dict[str, object]]:
    objects: List[Dict[str, object]] = []
    for i in range(write_sz):
        pref = PREFERRED_SITES[i]
        key_idx = random.randint(0, keys_per_site - 1)
        oid = f"ps{pref}:slow:key:{key_idx}"
        objects.append({"oid": oid, "value": _make_payload(seed=(tx_seed * 131 + i))})
    return objects


def _send_one(
    site_id: int,
    write_sz: int,
    keys_per_site: int,
    timeout_seconds: float,
    tx_seed: int,
) -> Sample:
    payload = {
        "type": MessageTypes.TX_BENCH_SLOW_DURABILITY_TX,
        "objects": _build_objects(write_sz=write_sz, keys_per_site=keys_per_site, tx_seed=tx_seed),
    }

    client = _get_client(timeout_seconds)
    try:
        resp = client.send_request(site_id, payload, apply_delay=False)
    except ConnectionError:
        client = _reset_client(timeout_seconds)
        resp = client.send_request(site_id, payload, apply_delay=False)

    return Sample(
        ok=bool(resp.get("ok")) and str(resp.get("commit_mode")) == "SLOW_2PC",
        commit_mode=str(resp.get("commit_mode", "UNKNOWN")),
        write_sz=write_sz,
        commit_ms=float(resp.get("commit_ms", float("inf"))),
        ds_durable_ms=float(resp.get("ds_durable_ms", float("inf"))),
    )


def _run_batch(
    site_id: int,
    write_sz: int,
    tx_count: int,
    concurrency: int,
    keys_per_site: int,
    timeout_seconds: float,
    seed: int,
    submit_interval_ms: float,
) -> List[Sample]:
    random.seed(seed)
    rows: List[Sample] = []
    pace_rng = random.Random(seed ^ 0x5A5A5A5A)

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        pending = set()
        next_i = 0

        while next_i < tx_count or pending:
            while next_i < tx_count and len(pending) < concurrency:
                if next_i > 0 and submit_interval_ms > 0.0:
                    time.sleep(pace_rng.uniform(0.0, submit_interval_ms) / 1000.0)
                fut = pool.submit(
                    _send_one,
                    site_id,
                    write_sz,
                    keys_per_site,
                    timeout_seconds,
                    seed + next_i,
                )
                pending.add(fut)
                next_i += 1

            if not pending:
                continue

            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for fut in done:
                rows.append(fut.result())

    return rows


def _plot_combined_cdf(path: Path, commit_by_sz: Dict[int, List[float]], ds_by_sz: Dict[int, List[float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(9.0, 5.4))

    colors = {2: "#0a7a3f", 3: "#1f77b4", 4: "#ff7f0e"}
    for write_sz in [2, 3, 4]:
        commit_vals = sorted(commit_by_sz.get(write_sz, []))
        ds_vals = sorted(ds_by_sz.get(write_sz, []))
        if commit_vals:
            y = [(i + 1) / len(commit_vals) for i in range(len(commit_vals))]
            ax.plot(
                commit_vals,
                y,
                linewidth=2.0,
                linestyle="-",
                color=colors[write_sz],
                label=f"commit sz={write_sz}",
            )
        if ds_vals:
            y = [(i + 1) / len(ds_vals) for i in range(len(ds_vals))]
            ax.plot(
                ds_vals,
                y,
                linewidth=2.0,
                linestyle="--",
                color=colors[write_sz],
                label=f"ds durable sz={write_sz}",
            )

    ax.set_xlabel("Latency (ms)")
    ax.set_ylabel("CDF")
    ax.set_ylim(0.0, 1.0)
    ax.set_title("Slow Commit + DS-durable CDF (4-site, origin=VA)")
    ax.grid(alpha=0.25)
    ax.legend(frameon=False, ncol=2)
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def _plot_cdf(path: Path, by_sz: Dict[int, List[float]], title: str, x_label: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(9.0, 5.4))

    colors = {2: "#0a7a3f", 3: "#1f77b4", 4: "#ff7f0e"}
    for write_sz in sorted(by_sz.keys()):
        vals = sorted(by_sz[write_sz])
        if not vals:
            continue
        y = [(i + 1) / len(vals) for i in range(len(vals))]
        ax.plot(vals, y, linewidth=2.0, label=f"write_sz={write_sz}", color=colors.get(write_sz))

    ax.set_xlabel(x_label)
    ax.set_ylabel("CDF")
    ax.set_ylim(0.0, 1.0)
    ax.set_title(title)
    ax.grid(alpha=0.25)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description="Slow commit latency + DS-durable experiment (4 sites, write_sz=2..4)")
    parser.add_argument("--tx-per-size", type=int, default=2000, help="measured tx count per write size")
    parser.add_argument("--warmup", type=int, default=200, help="warmup tx count per write size")
    parser.add_argument("--concurrency", type=int, default=16, help="max async in-flight requests")
    parser.add_argument("--origin-site", type=int, default=0, help="origin site issuing commits (VA=0)")
    parser.add_argument("--keys-per-site", type=int, default=10000, help="key-space size per site")
    parser.add_argument(
        "--submit-interval-ms",
        type=float,
        default=30.0,
        help="for each submit, sleep a random delay in [0, submit_interval_ms]",
    )
    parser.add_argument(
        "--propagate-batch-interval-ms",
        type=float,
        default=50.0,
        help="periodic propagation batch interval in milliseconds",
    )
    parser.add_argument(
        "--propagate-batch-max-txs",
        type=int,
        default=8,
        help="max tx payloads per propagation batch",
    )
    parser.add_argument("--timeout", type=float, default=10.0, help="RPC timeout")
    parser.add_argument("--seed", type=int, default=20260325, help="random seed")
    parser.add_argument(
        "--raw-csv",
        type=Path,
        default=RESULT_CSV_DIR / "slow_commit_latency_samples.csv",
        help="raw samples csv",
    )
    parser.add_argument(
        "--summary-csv",
        type=Path,
        default=RESULT_CSV_DIR / "slow_commit_latency_summary.csv",
        help="summary csv",
    )
    parser.add_argument(
        "--combined-plot",
        type=Path,
        default=RESULT_PNG_DIR / "slow_commit_all_cdf.png",
        help="output combined cdf png",
    )
    args = parser.parse_args()

    if args.tx_per_size <= 0:
        raise ValueError("--tx-per-size must be positive")
    if args.warmup < 0:
        raise ValueError("--warmup must be >= 0")
    if args.concurrency <= 0:
        raise ValueError("--concurrency must be positive")
    if args.keys_per_site <= 0:
        raise ValueError("--keys-per-site must be positive")
    if args.submit_interval_ms < 0:
        raise ValueError("--submit-interval-ms must be >= 0")
    if args.propagate_batch_interval_ms < 0:
        raise ValueError("--propagate-batch-interval-ms must be >= 0")
    if args.propagate_batch_max_txs <= 0:
        raise ValueError("--propagate-batch-max-txs must be positive")

    active_sites = [0, 1, 2, 3]
    if args.origin_site not in active_sites:
        raise ValueError(f"origin-site {args.origin_site} must be one of {active_sites}")

    os.environ["WALTER_ACTIVE_SITE_IDS"] = ",".join(str(s) for s in active_sites)
    os.environ["WALTER_PROPAGATE_BATCH_INTERVAL_MS"] = str(args.propagate_batch_interval_ms)
    os.environ["WALTER_PROPAGATE_BATCH_MAX_TXS"] = str(args.propagate_batch_max_txs)

    raw_rows: List[Dict[str, Any]] = []
    summary_rows: List[Dict[str, Any]] = []
    commit_by_sz: Dict[int, List[float]] = {}
    ds_by_sz: Dict[int, List[float]] = {}

    cluster = ClusterManager(site_ids=active_sites)
    cluster.start_all()
    try:
        cluster.wait_healthy(timeout_seconds=args.timeout)

        for write_sz in [2, 3, 4]:
            if args.warmup > 0:
                _run_batch(
                    site_id=args.origin_site,
                    write_sz=write_sz,
                    tx_count=args.warmup,
                    concurrency=args.concurrency,
                    keys_per_site=args.keys_per_site,
                    timeout_seconds=args.timeout,
                    seed=args.seed + write_sz,
                    submit_interval_ms=args.submit_interval_ms,
                )

            started = time.perf_counter()
            rows = _run_batch(
                site_id=args.origin_site,
                write_sz=write_sz,
                tx_count=args.tx_per_size,
                concurrency=args.concurrency,
                keys_per_site=args.keys_per_site,
                timeout_seconds=args.timeout,
                seed=args.seed + 1_000_000 + write_sz,
                submit_interval_ms=args.submit_interval_ms,
            )
            elapsed = time.perf_counter() - started

            ok_rows = [r for r in rows if r.ok]
            commit_vals = [r.commit_ms for r in ok_rows if r.commit_ms != float("inf")]
            ds_vals = [r.ds_durable_ms for r in ok_rows if r.ds_durable_ms != float("inf")]
            commit_by_sz[write_sz] = commit_vals
            ds_by_sz[write_sz] = ds_vals

            for r in rows:
                raw_rows.append(
                    {
                        "write_sz": write_sz,
                        "ok": int(r.ok),
                        "commit_mode": r.commit_mode,
                        "commit_ms": r.commit_ms,
                        "ds_durable_ms": r.ds_durable_ms,
                    }
                )

            throughput = (len(ok_rows) / elapsed) if elapsed > 0 else 0.0
            summary = {
                "write_sz": write_sz,
                "ok_count": len(ok_rows),
                "total": len(rows),
                "throughput_tx_s": throughput,
                "commit_avg_ms": statistics.mean(commit_vals) if commit_vals else 0.0,
                "commit_p50_ms": _percentile(commit_vals, 50),
                "commit_p95_ms": _percentile(commit_vals, 95),
                "commit_p99_ms": _percentile(commit_vals, 99),
                "ds_avg_ms": statistics.mean(ds_vals) if ds_vals else 0.0,
                "ds_p50_ms": _percentile(ds_vals, 50),
                "ds_p95_ms": _percentile(ds_vals, 95),
                "ds_p99_ms": _percentile(ds_vals, 99),
                "elapsed_s": elapsed,
            }
            summary_rows.append(summary)
            print(
                f"write_sz={write_sz} ok={summary['ok_count']}/{summary['total']} "
                f"throughput={summary['throughput_tx_s']:.2f} tx/s "
                f"commit(avg/p95)={summary['commit_avg_ms']:.2f}/{summary['commit_p95_ms']:.2f}ms "
                f"ds(avg/p95)={summary['ds_avg_ms']:.2f}/{summary['ds_p95_ms']:.2f}ms",
                flush=True,
            )
    finally:
        cluster.stop_all()

    args.raw_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.raw_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["write_sz", "ok", "commit_mode", "commit_ms", "ds_durable_ms"])
        for row in raw_rows:
            writer.writerow(
                [
                    row["write_sz"],
                    row["ok"],
                    row["commit_mode"],
                    f"{float(row['commit_ms']):.6f}",
                    f"{float(row['ds_durable_ms']):.6f}",
                ]
            )

    args.summary_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.summary_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "write_sz",
                "ok_count",
                "total",
                "throughput_tx_s",
                "commit_avg_ms",
                "commit_p50_ms",
                "commit_p95_ms",
                "commit_p99_ms",
                "ds_avg_ms",
                "ds_p50_ms",
                "ds_p95_ms",
                "ds_p99_ms",
                "elapsed_s",
            ]
        )
        for row in summary_rows:
            writer.writerow(
                [
                    row["write_sz"],
                    row["ok_count"],
                    row["total"],
                    f"{row['throughput_tx_s']:.6f}",
                    f"{row['commit_avg_ms']:.6f}",
                    f"{row['commit_p50_ms']:.6f}",
                    f"{row['commit_p95_ms']:.6f}",
                    f"{row['commit_p99_ms']:.6f}",
                    f"{row['ds_avg_ms']:.6f}",
                    f"{row['ds_p50_ms']:.6f}",
                    f"{row['ds_p95_ms']:.6f}",
                    f"{row['ds_p99_ms']:.6f}",
                    f"{row['elapsed_s']:.6f}",
                ]
            )

    _plot_combined_cdf(
        path=args.combined_plot,
        commit_by_sz=commit_by_sz,
        ds_by_sz=ds_by_sz,
    )

    print(f"raw_csv={args.raw_csv}")
    print(f"summary_csv={args.summary_csv}")
    print(f"combined_plot={args.combined_plot}")


if __name__ == "__main__":
    main()
