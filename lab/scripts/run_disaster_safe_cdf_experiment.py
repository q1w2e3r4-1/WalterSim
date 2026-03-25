"""Measure disaster-safe latency CDF for fast commits across 2..4 active sites.

Definition used in this simplified implementation:
- A transaction is disaster-safe once the origin site receives one propagation
    receive-ack from every other active site.
- A remote ack is counted if TX_PROPAGATE returns either APPLIED or QUEUED.
- Propagation uses periodic batching controlled by this script.
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


@dataclass
class Sample:
    ok: bool
    disaster_safe_ms: float
    commit_mode: str


def _percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    ranked = sorted(values)
    idx = min(len(ranked) - 1, max(0, int(round((p / 100.0) * (len(ranked) - 1)))))
    return ranked[idx]


def _make_payload(seed: int, size: int = 100) -> str:
    base = f"ds-{seed:08d}-"
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


def _send_one(
    site_id: int,
    write_objects_per_tx: int,
    keys_per_site: int,
    timeout_seconds: float,
    tx_seed: int,
) -> Sample:
    objects: List[Dict[str, object]] = []
    for i in range(write_objects_per_tx):
        key_idx = random.randint(0, keys_per_site - 1)
        oid = f"ps{site_id}:ds:key:{key_idx}"
        objects.append({"oid": oid, "value": _make_payload(seed=(tx_seed * 131 + i))})

    payload = {
        "type": MessageTypes.TX_BENCH_FAST_DURABILITY_TX,
        "objects": objects,
    }

    client = _get_client(timeout_seconds)
    try:
        resp = client.send_request(site_id, payload, apply_delay=False)
    except ConnectionError:
        client = _reset_client(timeout_seconds)
        resp = client.send_request(site_id, payload, apply_delay=False)

    return Sample(
        ok=bool(resp.get("ok")) and str(resp.get("commit_mode")) == "FAST",
        disaster_safe_ms=float(resp.get("disaster_safe_ms", float("inf"))),
        commit_mode=str(resp.get("commit_mode", "UNKNOWN")),
    )


def _run_batch(
    site_id: int,
    tx_count: int,
    concurrency: int,
    write_objects_per_tx: int,
    keys_per_site: int,
    timeout_seconds: float,
    seed: int,
    submit_interval_ms: float,
) -> List[Sample]:
    random.seed(seed)
    rows: List[Sample] = []
    pace_rng = random.Random(seed ^ 0xA5A5A5A5)
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        pending = set()
        next_i = 0

        # Keep a bounded async in-flight window instead of request-response serial pacing.
        while next_i < tx_count or pending:
            while next_i < tx_count and len(pending) < concurrency:
                if next_i > 0 and submit_interval_ms > 0.0:
                    delay_ms = pace_rng.uniform(0.0, submit_interval_ms)
                    time.sleep(delay_ms / 1000.0)
                fut = pool.submit(
                    _send_one,
                    site_id,
                    write_objects_per_tx,
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
                # print(fut.result())
                rows.append(fut.result())
    return rows


def _plot_cdf_by_site(path: Path, by_site_count: Dict[int, List[float]], write_sz: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(9.0, 5.4))

    colors = {2: "#0a7a3f", 3: "#1f77b4", 4: "#ff7f0e"}
    for site_count in sorted(by_site_count.keys()):
        vals = sorted(by_site_count[site_count])
        if not vals:
            continue
        y = [(i + 1) / len(vals) for i in range(len(vals))]
        ax.plot(vals, y, linewidth=2.0, label=f"sites={site_count}", color=colors.get(site_count))

    ax.set_xlabel("Disaster-safe Latency (ms)")
    ax.set_ylabel("CDF")
    ax.set_ylim(0.0, 1.0)
    ax.set_title(f"Fast Commit Disaster-safe CDF (write_sz={write_sz}, all-remote-acks)")
    ax.grid(alpha=0.25)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fast commit disaster-safe CDF experiment for 2..4 sites")
    parser.add_argument("--tx-per-setup", type=int, default=5000, help="measured tx count per site-count setup")
    parser.add_argument("--warmup", type=int, default=500, help="warmup tx count per setup")
    parser.add_argument("--concurrency", type=int, default=8, help="max async in-flight requests")
    parser.add_argument("--origin-site", type=int, default=0, help="origin site issuing commits")
    parser.add_argument("--write-objects-per-tx", type=int, default=1, help="write objects per tx")
    parser.add_argument("--keys-per-site", type=int, default=10000, help="key-space size per site")
    parser.add_argument(
        "--propagate-batch-interval-ms",
        type=float,
        default=20.0,
        help="periodic propagation batch interval in milliseconds",
    )
    parser.add_argument(
        "--propagate-batch-max-txs",
        type=int,
        default=128,
        help="max tx payloads per propagation batch",
    )
    parser.add_argument(
        "--submit-interval-ms",
        type=float,
        default=0.0,
        help="for each submit, sleep a random delay in [0, submit_interval_ms]",
    )
    parser.add_argument("--timeout", type=float, default=8.0, help="RPC timeout")
    parser.add_argument("--seed", type=int, default=20260323, help="random seed")
    parser.add_argument(
        "--raw-csv",
        type=Path,
        default=RESULT_CSV_DIR / "disaster_safe_fast_commit_samples.csv",
        help="raw samples csv",
    )
    parser.add_argument(
        "--summary-csv",
        type=Path,
        default=RESULT_CSV_DIR / "disaster_safe_fast_commit_summary.csv",
        help="summary csv",
    )
    parser.add_argument(
        "--plot",
        type=Path,
        default=RESULT_PNG_DIR / "disaster_safe_fast_commit_cdf.png",
        help="output cdf png",
    )
    args = parser.parse_args()

    if args.tx_per_setup <= 0:
        raise ValueError("--tx-per-setup must be positive")
    if args.warmup < 0:
        raise ValueError("--warmup must be >= 0")
    if args.concurrency <= 0:
        raise ValueError("--concurrency must be positive")
    effective_concurrency = max(2, args.concurrency)
    if effective_concurrency != args.concurrency:
        print(
            f"[note] concurrency={args.concurrency} would serialize sends; "
            f"using concurrency={effective_concurrency} to keep async in-flight mode",
            flush=True,
        )

    if args.write_objects_per_tx <= 0:
        raise ValueError("--write-objects-per-tx must be positive")
    if args.keys_per_site <= 0:
        raise ValueError("--keys-per-site must be positive")
    if args.propagate_batch_interval_ms < 0:
        raise ValueError("--propagate-batch-interval-ms must be >= 0")
    if args.propagate_batch_max_txs <= 0:
        raise ValueError("--propagate-batch-max-txs must be positive")
    if args.submit_interval_ms < 0:
        raise ValueError("--submit-interval-ms must be >= 0")

    raw_rows: List[Dict[str, Any]] = []
    summary_rows: List[Dict[str, Any]] = []
    by_site_count: Dict[int, List[float]] = {}

    for site_count in [2, 3, 4]:
        active_sites = list(range(site_count))
        if args.origin_site not in active_sites:
            raise ValueError(f"origin-site {args.origin_site} is not in active site set {active_sites}")

        os.environ["WALTER_ACTIVE_SITE_IDS"] = ",".join(str(s) for s in active_sites)
        os.environ["WALTER_PROPAGATE_BATCH_INTERVAL_MS"] = str(args.propagate_batch_interval_ms)
        os.environ["WALTER_PROPAGATE_BATCH_MAX_TXS"] = str(args.propagate_batch_max_txs)
        cluster = ClusterManager(site_ids=active_sites)
        cluster.start_all()
        try:
            cluster.wait_healthy(timeout_seconds=args.timeout)

            if args.warmup > 0:
                _run_batch(
                    site_id=args.origin_site,
                    tx_count=args.warmup,
                    concurrency=effective_concurrency,
                    write_objects_per_tx=args.write_objects_per_tx,
                    keys_per_site=args.keys_per_site,
                    timeout_seconds=args.timeout,
                    seed=args.seed + site_count,
                    submit_interval_ms=args.submit_interval_ms,
                )

            started = time.perf_counter()
            rows = _run_batch(
                site_id=args.origin_site,
                tx_count=args.tx_per_setup,
                concurrency=effective_concurrency,
                write_objects_per_tx=args.write_objects_per_tx,
                keys_per_site=args.keys_per_site,
                timeout_seconds=args.timeout,
                seed=args.seed + 1_000_000 + site_count,
                submit_interval_ms=args.submit_interval_ms,
            )
            elapsed = time.perf_counter() - started

            ok_rows = [r for r in rows if r.ok and r.disaster_safe_ms != float("inf")]
            vals = [r.disaster_safe_ms for r in ok_rows]
            by_site_count[site_count] = vals

            for r in rows:
                raw_rows.append(
                    {
                        "site_count": site_count,
                        "ok": int(r.ok),
                        "commit_mode": r.commit_mode,
                        "disaster_safe_ms": r.disaster_safe_ms,
                    }
                )

            throughput = (len(ok_rows) / elapsed) if elapsed > 0 else 0.0
            summary = {
                "site_count": site_count,
                "ok_count": len(ok_rows),
                "total": len(rows),
                "throughput_tx_s": throughput,
                "avg_ms": statistics.mean(vals) if vals else 0.0,
                "p50_ms": _percentile(vals, 50),
                "p95_ms": _percentile(vals, 95),
                "p99_ms": _percentile(vals, 99),
                "write_objects_per_tx": args.write_objects_per_tx,
                "elapsed_s": elapsed,
            }
            summary_rows.append(summary)
            print(
                f"sites={site_count} ok={summary['ok_count']}/{summary['total']} "
                f"throughput={summary['throughput_tx_s']:.2f} tx/s "
                f"avg={summary['avg_ms']:.2f}ms p95={summary['p95_ms']:.2f}ms p99={summary['p99_ms']:.2f}ms",
                flush=True,
            )
        finally:
            cluster.stop_all()

    args.raw_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.raw_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["site_count", "ok", "commit_mode", "disaster_safe_ms"])
        for row in raw_rows:
            writer.writerow(
                [
                    row["site_count"],
                    row["ok"],
                    row["commit_mode"],
                    f"{float(row['disaster_safe_ms']):.6f}",
                ]
            )

    args.summary_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.summary_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "site_count",
                "ok_count",
                "total",
                "throughput_tx_s",
                "avg_ms",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "write_objects_per_tx",
                "elapsed_s",
            ]
        )
        for row in summary_rows:
            writer.writerow(
                [
                    row["site_count"],
                    row["ok_count"],
                    row["total"],
                    f"{row['throughput_tx_s']:.6f}",
                    f"{row['avg_ms']:.6f}",
                    f"{row['p50_ms']:.6f}",
                    f"{row['p95_ms']:.6f}",
                    f"{row['p99_ms']:.6f}",
                    row["write_objects_per_tx"],
                    f"{row['elapsed_s']:.6f}",
                ]
            )

    _plot_cdf_by_site(
        path=args.plot,
        by_site_count=by_site_count,
        write_sz=args.write_objects_per_tx,
    )

    print(f"raw_csv={args.raw_csv}")
    print(f"summary_csv={args.summary_csv}")
    print(f"plot={args.plot}")


if __name__ == "__main__":
    main()
