"""Run 1-site Fast Commit latency experiment and generate a CDF plot.

This script targets the paper-style fast-commit latency question for a single
site, with all writes routed to local-preferred keys to keep commit mode FAST.
"""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path
import random
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List

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
class TxSample:
    ok: bool
    latency_ms: float
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
    base = f"cdf-{seed:08d}-"
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


def _send_fast_write_tx(
    site_id: int,
    write_objects_per_tx: int,
    keys: int,
    timeout_seconds: float,
    tx_seed: int,
) -> Dict[str, object]:
    objects = []
    for i in range(write_objects_per_tx):
        key_idx = random.randint(0, keys - 1)
        oid = f"ps{site_id}:cdf:key:{key_idx}"
        objects.append({"oid": oid, "value": _make_payload(seed=(tx_seed * 97 + i))})

    payload = {
        "type": MessageTypes.TX_BENCH_FAST_TX,
        "mode": "write",
        "objects": objects,
    }

    client = _get_client(timeout_seconds)
    try:
        return client.send_request(site_id, payload, apply_delay=False)
    except ConnectionError:
        client = _reset_client(timeout_seconds)
        return client.send_request(site_id, payload, apply_delay=False)


def _run_one(
    site_id: int,
    write_objects_per_tx: int,
    keys: int,
    timeout_seconds: float,
    tx_seed: int,
) -> TxSample:
    start = time.perf_counter()
    resp = _send_fast_write_tx(
        site_id=site_id,
        write_objects_per_tx=write_objects_per_tx,
        keys=keys,
        timeout_seconds=timeout_seconds,
        tx_seed=tx_seed,
    )
    latency_ms = (time.perf_counter() - start) * 1000.0
    return TxSample(
        ok=bool(resp.get("ok")),
        latency_ms=latency_ms,
        commit_mode=str(resp.get("commit_mode", "UNKNOWN")),
    )


def _run_batch(
    pool: ThreadPoolExecutor,
    site_id: int,
    tx_count: int,
    write_objects_per_tx: int,
    keys: int,
    timeout_seconds: float,
    seed: int,
) -> List[TxSample]:
    rows: List[TxSample] = []
    random.seed(seed)
    futures = []
    for i in range(tx_count):
        futures.append(
            pool.submit(
                _run_one,
                site_id,
                write_objects_per_tx,
                keys,
                timeout_seconds,
                seed + i,
            )
        )
    for fut in as_completed(futures):
        rows.append(fut.result())
    return rows


def _write_raw_csv(path: Path, rows: List[TxSample]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["ok", "latency_ms", "commit_mode"])
        for r in rows:
            writer.writerow([int(r.ok), f"{r.latency_ms:.6f}", r.commit_mode])


def _write_cdf_csv(path: Path, latencies_ms: List[float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ranked = sorted(latencies_ms)
    n = len(ranked)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["latency_ms", "cdf"])
        if n == 0:
            return
        for idx, value in enumerate(ranked, start=1):
            writer.writerow([f"{value:.6f}", f"{(idx / n):.8f}"])


def _plot_cdf(path: Path, latencies_ms: List[float], title: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ranked = sorted(latencies_ms)
    n = len(ranked)
    if n == 0:
        raise RuntimeError("no successful latency samples to plot")

    y = [(i + 1) / n for i in range(n)]

    fig, ax = plt.subplots(figsize=(8.8, 5.2))
    ax.plot(ranked, y, linewidth=2.0, color="#0a7a3f")
    ax.set_xlabel("Commit Latency (ms)")
    ax.set_ylabel("CDF")
    ax.set_title(title)
    ax.grid(alpha=0.25)
    ax.set_ylim(0.0, 1.0)
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description="1-site Fast Commit latency CDF experiment")
    parser.add_argument("--site-id", type=int, default=0, help="single active site id")
    parser.add_argument("--tx-count", type=int, default=20000, help="number of measured transactions")
    parser.add_argument("--warmup", type=int, default=1000, help="warm-up transactions before measurement")
    parser.add_argument("--concurrency", type=int, default=1, help="client concurrency for the active site")
    parser.add_argument("--write-objects-per-tx", type=int, default=1, help="number of write objects in each tx")
    parser.add_argument("--keys", type=int, default=10000, help="local preferred key-space size")
    parser.add_argument("--timeout", type=float, default=8.0, help="RPC timeout seconds")
    parser.add_argument("--seed", type=int, default=42, help="random seed")
    parser.add_argument("--read-base-ms", type=float, default=0.0, help="simulated local read storage latency per object")
    parser.add_argument("--write-base-ms", type=float, default=0.0, help="simulated local write storage latency per object")
    parser.add_argument("--cache-miss-ratio", type=float, default=0.0, help="cache miss probability per object")
    parser.add_argument("--cache-miss-penalty-ms", type=float, default=0.0, help="extra latency on cache miss")
    parser.add_argument(
        "--raw-csv",
        type=Path,
        default=RESULT_CSV_DIR / "fast_commit_latency_cdf_raw.csv",
        help="raw sample output csv",
    )
    parser.add_argument(
        "--cdf-csv",
        type=Path,
        default=RESULT_CSV_DIR / "fast_commit_latency_cdf_points.csv",
        help="cdf points output csv",
    )
    parser.add_argument(
        "--plot",
        type=Path,
        default=RESULT_PNG_DIR / "fast_commit_latency_cdf.png",
        help="output cdf plot png",
    )
    args = parser.parse_args()

    if args.tx_count <= 0:
        raise ValueError("--tx-count must be positive")
    if args.concurrency <= 0:
        raise ValueError("--concurrency must be positive")
    if args.write_objects_per_tx <= 0:
        raise ValueError("--write-objects-per-tx must be positive")
    if args.keys <= 0:
        raise ValueError("--keys must be positive")

    os.environ["WALTER_ACTIVE_SITE_IDS"] = str(args.site_id)
    os.environ["WALTER_BENCH_LOCAL_READ_BASE_MS"] = f"{max(0.0, args.read_base_ms)}"
    os.environ["WALTER_BENCH_LOCAL_WRITE_BASE_MS"] = f"{max(0.0, args.write_base_ms)}"
    os.environ["WALTER_BENCH_CACHE_MISS_RATIO"] = f"{min(1.0, max(0.0, args.cache_miss_ratio))}"
    os.environ["WALTER_BENCH_CACHE_MISS_PENALTY_MS"] = f"{max(0.0, args.cache_miss_penalty_ms)}"

    cluster = ClusterManager(site_ids=[args.site_id])
    cluster.start_all()
    try:
        cluster.wait_healthy(timeout_seconds=args.timeout)

        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            if args.warmup > 0:
                _run_batch(
                    pool=pool,
                    site_id=args.site_id,
                    tx_count=args.warmup,
                    write_objects_per_tx=args.write_objects_per_tx,
                    keys=args.keys,
                    timeout_seconds=args.timeout,
                    seed=args.seed,
                )

            started = time.perf_counter()
            rows = _run_batch(
                pool=pool,
                site_id=args.site_id,
                tx_count=args.tx_count,
                write_objects_per_tx=args.write_objects_per_tx,
                keys=args.keys,
                timeout_seconds=args.timeout,
                seed=args.seed + 1_000_000,
            )
        elapsed = time.perf_counter() - started

        ok_rows = [r for r in rows if r.ok]
        fast_rows = [r for r in ok_rows if r.commit_mode == "FAST"]
        latencies_ms = [r.latency_ms for r in fast_rows]

        _write_raw_csv(args.raw_csv, rows)
        _write_cdf_csv(args.cdf_csv, latencies_ms)
        _plot_cdf(
            path=args.plot,
            latencies_ms=latencies_ms,
            title=f"Fast Commit Latency CDF (1 site, write_sz={args.write_objects_per_tx})",
        )

        throughput = (len(ok_rows) / elapsed) if elapsed > 0 else 0.0
        print(f"samples={len(rows)} ok={len(ok_rows)} fast_ok={len(fast_rows)} elapsed_s={elapsed:.6f}")
        print(f"throughput_tx_s={throughput:.2f}")
        if latencies_ms:
            print(
                "latency_ms "
                f"avg={statistics.mean(latencies_ms):.6f} "
                f"p50={_percentile(latencies_ms, 50):.6f} "
                f"p95={_percentile(latencies_ms, 95):.6f} "
                f"p99={_percentile(latencies_ms, 99):.6f}"
            )
        print(f"raw_csv={args.raw_csv}")
        print(f"cdf_csv={args.cdf_csv}")
        print(f"plot={args.plot}")
    finally:
        cluster.stop_all()


if __name__ == "__main__":
    main()
