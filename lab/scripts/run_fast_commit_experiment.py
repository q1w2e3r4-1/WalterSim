"""Fast Commit scalability experiment (1 to 4 sites).

Setup (paper-aligned intent for this toy implementation):
- Active sites: 1, 2, 3, 4.
- Objects are replicated asynchronously to all active sites.
- Preferred sites are assigned evenly by key prefix (ps<site_id>:...).
- Clients issue transactions to their local site as fast as possible.

Workloads:
- read-only, write-only, mixed (90% read / 10% write)
- each tx accesses 1 or 5 objects (100-byte values)

Performance path:
- Always uses optimized benchmark path: long-lived connections + one-RPC tx.
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Sequence

LAB_ROOT = Path(__file__).resolve().parent.parent
SITE_DIR = LAB_ROOT / "site"
RESULT_DIR = LAB_ROOT / "experiments" / "results" / "csv"
if str(LAB_ROOT) not in sys.path:
    sys.path.insert(0, str(LAB_ROOT))
if str(SITE_DIR) not in sys.path:
    sys.path.insert(0, str(SITE_DIR))

from cluster import ClusterManager
from network.rpc import MessageTypes, PersistentRpcClient

_thread_local = threading.local()


@dataclass
class TxResult:
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
    base = f"v{seed:08d}-"
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


def _send_fast_tx(site_id: int, mode: str, objects: Sequence[Dict[str, object]], timeout_seconds: float) -> Dict[str, object]:
    payload = {
        "type": MessageTypes.TX_BENCH_FAST_TX,
        "mode": mode,
        "objects": list(objects),
    }

    client = _get_client(timeout_seconds)
    try:
        return client.send_request(site_id, payload, apply_delay=False)
    except ConnectionError:
        client = _reset_client(timeout_seconds)
        return client.send_request(site_id, payload, apply_delay=False)


def _pick_oids(site_id: int, objects_per_tx: int, keys_per_site: int) -> List[str]:
    picked: List[str] = []
    for _ in range(objects_per_tx):
        key_idx = random.randint(0, keys_per_site - 1)
        picked.append(f"ps{site_id}:fc:key:{key_idx}")
    return picked


def preload_keys(active_sites: Sequence[int], keys_per_site: int, timeout_seconds: float) -> None:
    print(f"Preloading keys per site={keys_per_site} for sites={list(active_sites)} ...", flush=True)
    for site_id in active_sites:
        for i in range(keys_per_site):
            oid = f"ps{site_id}:fc:key:{i}"
            payload = _make_payload(seed=(site_id * 10_000_000 + i))
            write_obj: Dict[str, object] = {"oid": oid, "value": payload}
            resp = _send_fast_tx(
                site_id=site_id,
                mode="write",
                objects=[write_obj],
                timeout_seconds=timeout_seconds,
            )
            if not resp.get("ok"):
                raise RuntimeError(f"preload failed site={site_id} key={i}: {resp}")


def _run_one_tx(
    site_id: int,
    workload: str,
    read_objects_per_tx: int,
    write_objects_per_tx: int,
    keys_per_site: int,
    timeout_seconds: float,
    tx_seed: int,
) -> TxResult:
    if workload == "mixed":
        mode = "write" if random.random() < 0.1 else "read"
    elif workload == "write":
        mode = "write"
    else:
        mode = "read"

    object_count = write_objects_per_tx if mode == "write" else read_objects_per_tx
    oids = _pick_oids(site_id=site_id, objects_per_tx=object_count, keys_per_site=keys_per_site)
    objects: List[Dict[str, object]]
    if mode == "write":
        objects = [{"oid": oid, "value": _make_payload(seed=tx_seed + idx)} for idx, oid in enumerate(oids)]
    else:
        objects = [{"oid": oid} for oid in oids]

    t0 = time.perf_counter()
    resp = _send_fast_tx(site_id=site_id, mode=mode, objects=objects, timeout_seconds=timeout_seconds)
    latency_ms = (time.perf_counter() - t0) * 1000.0
    return TxResult(
        ok=bool(resp.get("ok")),
        latency_ms=latency_ms,
        commit_mode=str(resp.get("commit_mode", "UNKNOWN")),
    )


def run_site_workload(
    site_id: int,
    workload: str,
    read_objects_per_tx: int,
    write_objects_per_tx: int,
    tx_count: int,
    concurrency: int,
    keys_per_site: int,
    timeout_seconds: float,
) -> List[TxResult]:
    rows: List[TxResult] = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = []
        for i in range(tx_count):
            futures.append(
                pool.submit(
                    _run_one_tx,
                    site_id,
                    workload,
                    read_objects_per_tx,
                    write_objects_per_tx,
                    keys_per_site,
                    timeout_seconds,
                    i,
                )
            )

        for fut in as_completed(futures):
            rows.append(fut.result())
    return rows


def summarize(rows: List[TxResult], elapsed_s: float) -> Dict[str, object]:
    ok_rows = [r for r in rows if r.ok]
    latencies = [r.latency_ms for r in ok_rows]
    modes: Dict[str, int] = {}
    for row in ok_rows:
        modes[row.commit_mode] = modes.get(row.commit_mode, 0) + 1

    ok_count = len(ok_rows)
    total = len(rows)
    throughput = ok_count / elapsed_s if elapsed_s > 0 else 0.0
    fast_ratio = (modes.get("FAST", 0) / ok_count) if ok_count > 0 else 0.0

    return {
        "ok_count": ok_count,
        "total": total,
        "throughput_tx_s": throughput,
        "avg_ms": statistics.mean(latencies) if latencies else 0.0,
        "p50_ms": _percentile(latencies, 50),
        "p95_ms": _percentile(latencies, 95),
        "p99_ms": _percentile(latencies, 99),
        "fast_ratio": fast_ratio,
        "modes": modes,
    }


def run_one_setup(
    site_count: int,
    workload: str,
    read_objects_per_tx: int,
    write_objects_per_tx: int,
    tx_per_site: int,
    concurrency_per_site: int,
    keys_per_site: int,
    timeout_seconds: float,
    read_base_ms: float,
    write_base_ms: float,
    cache_miss_ratio: float,
    cache_miss_penalty_ms: float,
) -> Dict[str, object]:
    active_sites = list(range(site_count))
    os.environ["WALTER_ACTIVE_SITE_IDS"] = ",".join(str(s) for s in active_sites)
    os.environ["WALTER_BENCH_LOCAL_READ_BASE_MS"] = f"{read_base_ms}"
    os.environ["WALTER_BENCH_LOCAL_WRITE_BASE_MS"] = f"{write_base_ms}"
    os.environ["WALTER_BENCH_CACHE_MISS_RATIO"] = f"{cache_miss_ratio}"
    os.environ["WALTER_BENCH_CACHE_MISS_PENALTY_MS"] = f"{cache_miss_penalty_ms}"

    cluster = ClusterManager(site_ids=active_sites)
    cluster.start_all()
    try:
        cluster.wait_healthy()
        preload_keys(active_sites=active_sites, keys_per_site=keys_per_site, timeout_seconds=timeout_seconds)

        started = time.perf_counter()
        all_rows: List[TxResult] = []
        with ThreadPoolExecutor(max_workers=site_count) as pool:
            futures = []
            for site_id in active_sites:
                futures.append(
                    pool.submit(
                        run_site_workload,
                        site_id,
                        workload,
                        read_objects_per_tx,
                        write_objects_per_tx,
                        tx_per_site,
                        concurrency_per_site,
                        keys_per_site,
                        timeout_seconds,
                    )
                )

            for fut in as_completed(futures):
                all_rows.extend(fut.result())

        elapsed = time.perf_counter() - started
        summary = summarize(all_rows, elapsed)
        summary.update(
            {
                "site_count": site_count,
                "workload": workload,
                "read_objects_per_tx": read_objects_per_tx,
                "write_objects_per_tx": write_objects_per_tx,
                "tx_per_site": tx_per_site,
                "concurrency_per_site": concurrency_per_site,
                "elapsed_s": elapsed,
                "read_base_ms": read_base_ms,
                "write_base_ms": write_base_ms,
                "cache_miss_ratio": cache_miss_ratio,
                "cache_miss_penalty_ms": cache_miss_penalty_ms,
            }
        )
        return summary
    finally:
        cluster.stop_all()


def main() -> None:
    parser = argparse.ArgumentParser(description="Fast Commit scalability experiment")
    parser.add_argument("--tx-per-site", type=int, default=2000, help="transactions per site in each setup")
    parser.add_argument("--concurrency-per-site", type=int, default=8, help="client worker threads per site")
    parser.add_argument("--keys-per-site", type=int, default=50000, help="keys per site (preferred local)")
    parser.add_argument("--timeout", type=float, default=8.0, help="RPC timeout seconds")
    parser.add_argument("--read-base-ms", type=float, default=0.0, help="simulated local read storage latency per object")
    parser.add_argument("--write-base-ms", type=float, default=0.0, help="simulated local write storage latency per object")
    parser.add_argument("--cache-miss-ratio", type=float, default=0.0, help="probability of cache miss per accessed object")
    parser.add_argument(
        "--cache-miss-penalty-ms",
        type=float,
        default=0.0,
        help="extra latency per object when a cache miss is sampled",
    )
    args = parser.parse_args()

    miss_ratio = min(1.0, max(0.0, args.cache_miss_ratio))

    setup_matrix: List[tuple[str, int, int]] = [
        ("read", 1, 0),
        ("read", 5, 0),
        ("write", 0, 1),
        ("write", 0, 5),
        ("mixed", 1, 1),
        ("mixed", 1, 5),
        ("mixed", 5, 1),
        ("mixed", 5, 5),
    ]

    rows: List[Dict[str, object]] = []
    for site_count in [1, 2, 3, 4]:
        for workload, read_sz, write_sz in setup_matrix:
            print(
                f"Running setup: sites={site_count} workload={workload} read_sz={read_sz} write_sz={write_sz}",
                flush=True,
            )
            row = run_one_setup(
                site_count=site_count,
                workload=workload,
                read_objects_per_tx=read_sz,
                write_objects_per_tx=write_sz,
                tx_per_site=args.tx_per_site,
                concurrency_per_site=args.concurrency_per_site,
                keys_per_site=args.keys_per_site,
                timeout_seconds=args.timeout,
                read_base_ms=max(0.0, args.read_base_ms),
                write_base_ms=max(0.0, args.write_base_ms),
                cache_miss_ratio=miss_ratio,
                cache_miss_penalty_ms=max(0.0, args.cache_miss_penalty_ms),
            )
            rows.append(row)
            print(
                f"  ok={row['ok_count']}/{row['total']} throughput={row['throughput_tx_s']:.2f} tx/s "
                f"avg={row['avg_ms']:.2f}ms p95={row['p95_ms']:.2f}ms fast_ratio={row['fast_ratio']:.3f} "
                f"modes={row['modes']}",
                flush=True,
            )

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    out_file = RESULT_DIR / "fast_commit_scalability.csv"
    with out_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "site_count",
                "workload",
                "read_objects_per_tx",
                "write_objects_per_tx",
                "tx_per_site",
                "concurrency_per_site",
                "ok_count",
                "total",
                "throughput_tx_s",
                "avg_ms",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "fast_ratio",
                "modes",
                "elapsed_s",
                "optimized_path",
                "read_base_ms",
                "write_base_ms",
                "cache_miss_ratio",
                "cache_miss_penalty_ms",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row["site_count"],
                    row["workload"],
                    row["read_objects_per_tx"],
                    row["write_objects_per_tx"],
                    row["tx_per_site"],
                    row["concurrency_per_site"],
                    row["ok_count"],
                    row["total"],
                    f"{row['throughput_tx_s']:.6f}",
                    f"{row['avg_ms']:.6f}",
                    f"{row['p50_ms']:.6f}",
                    f"{row['p95_ms']:.6f}",
                    f"{row['p99_ms']:.6f}",
                    f"{row['fast_ratio']:.6f}",
                    str(row["modes"]),
                    f"{row['elapsed_s']:.6f}",
                    "persistent+single-rpc",
                    f"{row['read_base_ms']:.6f}",
                    f"{row['write_base_ms']:.6f}",
                    f"{row['cache_miss_ratio']:.6f}",
                    f"{row['cache_miss_penalty_ms']:.6f}",
                ]
            )

    print(f"CSV written: {out_file}", flush=True)


if __name__ == "__main__":
    main()
