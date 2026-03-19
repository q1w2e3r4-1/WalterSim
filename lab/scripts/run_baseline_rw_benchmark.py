"""Baseline read/write benchmark for current Walter toy implementation.

This benchmark approximates the paper's single-object transaction setup:
- Read tx: one object read.
- Write tx: one object write (about 100-byte payload).

Notes for this toy version:
- We do not integrate Berkeley DB; this measures current Walter implementation only.
- We use TX_START/TX_READ(or TX_WRITE)/TX_COMMIT_LOCAL RPCs (no piggyback optimization yet).
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
import random
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple

LAB_ROOT = Path(__file__).resolve().parent.parent
SITE_DIR = LAB_ROOT / "site"
RESULT_DIR = LAB_ROOT / "experiments" / "results"
if str(LAB_ROOT) not in sys.path:
    sys.path.insert(0, str(LAB_ROOT))
if str(SITE_DIR) not in sys.path:
    sys.path.insert(0, str(SITE_DIR))

from cluster import ClusterManager
from network.rpc import MessageTypes, PersistentRpcClient, RpcClient


_thread_local = threading.local()


def _get_client(timeout_seconds: float) -> RpcClient:
    client = getattr(_thread_local, "client", None)
    if client is None:
        client = RpcClient(timeout_seconds=timeout_seconds)
        _thread_local.client = client
    return client


def _get_persistent_client(timeout_seconds: float) -> PersistentRpcClient:
    client = getattr(_thread_local, "persistent_client", None)
    if client is None:
        client = PersistentRpcClient(timeout_seconds=timeout_seconds)
        _thread_local.persistent_client = client
    return client


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


def _write_one(
    site_id: int,
    oid: str,
    value: str,
    timeout_seconds: float,
    use_persistent: bool,
) -> Dict[str, object]:
    client = _get_persistent_client(timeout_seconds) if use_persistent else _get_client(timeout_seconds)
    start = client.send_request(site_id, {"type": MessageTypes.TX_START}, apply_delay=False)
    tid = int(start["tid"])
    client.send_request(
        site_id,
        {"type": MessageTypes.TX_WRITE, "tid": tid, "oid": oid, "value": value},
        apply_delay=False,
    )
    return client.send_request(
        site_id,
        {"type": MessageTypes.TX_COMMIT_LOCAL, "tid": tid},
        apply_delay=False,
    )


def _write_one_single_rpc(
    site_id: int,
    oid: str,
    value: str,
    timeout_seconds: float,
    use_persistent: bool,
) -> Dict[str, object]:
    client = _get_persistent_client(timeout_seconds) if use_persistent else _get_client(timeout_seconds)
    return client.send_request(
        site_id,
        {"type": MessageTypes.TX_WRITE_ONE_TX, "oid": oid, "value": value},
        apply_delay=False,
    )


def _read_one(
    site_id: int,
    oid: str,
    timeout_seconds: float,
    use_persistent: bool,
) -> Tuple[Dict[str, object], Dict[str, object]]:
    client = _get_persistent_client(timeout_seconds) if use_persistent else _get_client(timeout_seconds)
    start = client.send_request(site_id, {"type": MessageTypes.TX_START}, apply_delay=False)
    tid = int(start["tid"])
    read_resp = client.send_request(
        site_id,
        {"type": MessageTypes.TX_READ, "tid": tid, "oid": oid},
        apply_delay=False,
    )
    commit_resp = client.send_request(
        site_id,
        {"type": MessageTypes.TX_COMMIT_LOCAL, "tid": tid},
        apply_delay=False,
    )
    return read_resp, commit_resp


def _read_one_single_rpc(
    site_id: int,
    oid: str,
    timeout_seconds: float,
    use_persistent: bool,
) -> Dict[str, object]:
    client = _get_persistent_client(timeout_seconds) if use_persistent else _get_client(timeout_seconds)
    return client.send_request(
        site_id,
        {"type": MessageTypes.TX_READ_ONE_TX, "oid": oid},
        apply_delay=False,
    )


def _make_payload(i: int, size: int = 100) -> str:
    base = f"v{i:08d}-"
    if len(base) >= size:
        return base[:size]
    return base + ("x" * (size - len(base)))


def preload_keys(site_id: int, key_count: int, timeout_seconds: float, use_persistent: bool) -> None:
    print(f"Preloading {key_count} keys...", flush=True)
    for i in range(key_count):
        oid = f"ps0:baseline:key:{i}"
        payload = _make_payload(i)
        commit = _write_one(
            site_id=site_id,
            oid=oid,
            value=payload,
            timeout_seconds=timeout_seconds,
            use_persistent=use_persistent,
        )
        if not commit.get("ok"):
            raise RuntimeError(f"preload write failed at key {i}: {commit}")
        if (i + 1) % 5000 == 0:
            print(f"  loaded {i + 1}/{key_count}", flush=True)


def run_read_tx(
    site_id: int,
    key_count: int,
    timeout_seconds: float,
    use_persistent: bool,
    use_single_rpc: bool,
) -> TxResult:
    key_idx = random.randint(0, key_count - 1)
    oid = f"ps0:baseline:key:{key_idx}"
    t0 = time.perf_counter()
    if use_single_rpc:
        commit = _read_one_single_rpc(
            site_id=site_id,
            oid=oid,
            timeout_seconds=timeout_seconds,
            use_persistent=use_persistent,
        )
        ok = bool(commit.get("ok"))
    else:
        read_resp, commit = _read_one(
            site_id=site_id,
            oid=oid,
            timeout_seconds=timeout_seconds,
            use_persistent=use_persistent,
        )
        ok = bool(read_resp.get("ok") and commit.get("ok"))
    latency_ms = (time.perf_counter() - t0) * 1000.0
    mode = str(commit.get("commit_mode", "UNKNOWN"))
    return TxResult(ok=ok, latency_ms=latency_ms, commit_mode=mode)


def run_write_tx(
    site_id: int,
    key_count: int,
    tx_id: int,
    timeout_seconds: float,
    use_persistent: bool,
    use_single_rpc: bool,
) -> TxResult:
    key_idx = random.randint(0, key_count - 1)
    oid = f"ps0:baseline:key:{key_idx}"
    payload = _make_payload(tx_id)
    t0 = time.perf_counter()
    if use_single_rpc:
        commit = _write_one_single_rpc(
            site_id=site_id,
            oid=oid,
            value=payload,
            timeout_seconds=timeout_seconds,
            use_persistent=use_persistent,
        )
    else:
        commit = _write_one(
            site_id=site_id,
            oid=oid,
            value=payload,
            timeout_seconds=timeout_seconds,
            use_persistent=use_persistent,
        )
    latency_ms = (time.perf_counter() - t0) * 1000.0
    ok = bool(commit.get("ok"))
    mode = str(commit.get("commit_mode", "UNKNOWN"))
    return TxResult(ok=ok, latency_ms=latency_ms, commit_mode=mode)


def run_workload(
    workload: str,
    site_id: int,
    key_count: int,
    tx_count: int,
    concurrency: int,
    timeout_seconds: float,
    use_persistent: bool,
    use_single_rpc: bool,
) -> List[TxResult]:
    results: List[TxResult] = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = []
        for i in range(tx_count):
            if workload == "read":
                futures.append(
                    pool.submit(run_read_tx, site_id, key_count, timeout_seconds, use_persistent, use_single_rpc)
                )
            else:
                futures.append(
                    pool.submit(run_write_tx, site_id, key_count, i, timeout_seconds, use_persistent, use_single_rpc)
                )

        for fut in as_completed(futures):
            results.append(fut.result())
    return results


def summarize(results: List[TxResult], elapsed_s: float) -> Dict[str, object]:
    ok_rows = [r for r in results if r.ok]
    lat = [r.latency_ms for r in ok_rows]
    ok_count = len(ok_rows)
    throughput = ok_count / elapsed_s if elapsed_s > 0 else 0.0
    modes: Dict[str, int] = {}
    for r in ok_rows:
        modes[r.commit_mode] = modes.get(r.commit_mode, 0) + 1

    return {
        "ok_count": ok_count,
        "total": len(results),
        "throughput_tx_s": throughput,
        "avg_ms": statistics.mean(lat) if lat else 0.0,
        "p50_ms": _percentile(lat, 50),
        "p95_ms": _percentile(lat, 95),
        "p99_ms": _percentile(lat, 99),
        "modes": modes,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Baseline read/write benchmark for Walter toy implementation")
    parser.add_argument("--site", type=int, default=0, help="site id used for benchmark")
    parser.add_argument("--keys", type=int, default=50000, help="number of keys to preload")
    parser.add_argument("--tx", type=int, default=2000, help="transactions per workload")
    parser.add_argument("--concurrency", type=int, default=8, help="concurrent client workers")
    parser.add_argument("--timeout", type=float, default=8.0, help="RPC timeout seconds")
    parser.add_argument(
        "--persistent",
        action="store_true",
        help="reuse long-lived TCP connections in benchmark clients",
    )
    parser.add_argument(
        "--single-rpc",
        action="store_true",
        help="use benchmark-only one-RPC path (start+op+commit in one request)",
    )
    args = parser.parse_args()

    cluster = ClusterManager()
    cluster.start_all()

    try:
        cluster.wait_healthy()
        print("Cluster is healthy.", flush=True)

        preload_keys(
            site_id=args.site,
            key_count=args.keys,
            timeout_seconds=args.timeout,
            use_persistent=args.persistent,
        )

        t0 = time.perf_counter()
        read_results = run_workload(
            workload="read",
            site_id=args.site,
            key_count=args.keys,
            tx_count=args.tx,
            concurrency=args.concurrency,
            timeout_seconds=args.timeout,
            use_persistent=args.persistent,
            use_single_rpc=args.single_rpc,
        )
        read_elapsed = time.perf_counter() - t0

        t1 = time.perf_counter()
        write_results = run_workload(
            workload="write",
            site_id=args.site,
            key_count=args.keys,
            tx_count=args.tx,
            concurrency=args.concurrency,
            timeout_seconds=args.timeout,
            use_persistent=args.persistent,
            use_single_rpc=args.single_rpc,
        )
        write_elapsed = time.perf_counter() - t1

        read_summary = summarize(read_results, read_elapsed)
        write_summary = summarize(write_results, write_elapsed)

        print(
            "[read] "
            f"ok={read_summary['ok_count']}/{read_summary['total']} "
            f"throughput={read_summary['throughput_tx_s']:.2f} tx/s "
            f"avg={read_summary['avg_ms']:.2f}ms p50={read_summary['p50_ms']:.2f}ms "
            f"p95={read_summary['p95_ms']:.2f}ms p99={read_summary['p99_ms']:.2f}ms "
            f"modes={read_summary['modes']} "
            f"persistent={args.persistent} single_rpc={args.single_rpc}",
            flush=True,
        )
        print(
            "[write] "
            f"ok={write_summary['ok_count']}/{write_summary['total']} "
            f"throughput={write_summary['throughput_tx_s']:.2f} tx/s "
            f"avg={write_summary['avg_ms']:.2f}ms p50={write_summary['p50_ms']:.2f}ms "
            f"p95={write_summary['p95_ms']:.2f}ms p99={write_summary['p99_ms']:.2f}ms "
            f"modes={write_summary['modes']} "
            f"persistent={args.persistent} single_rpc={args.single_rpc}",
            flush=True,
        )

        RESULT_DIR.mkdir(parents=True, exist_ok=True)
        out_file = RESULT_DIR / "baseline_rw_benchmark_summary.csv"
        with out_file.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "workload",
                    "ok_count",
                    "total",
                    "throughput_tx_s",
                    "avg_ms",
                    "p50_ms",
                    "p95_ms",
                    "p99_ms",
                    "modes",
                    "elapsed_s",
                    "keys",
                    "tx",
                    "concurrency",
                    "persistent",
                    "single_rpc",
                ]
            )
            writer.writerow(
                [
                    "read",
                    read_summary["ok_count"],
                    read_summary["total"],
                    f"{read_summary['throughput_tx_s']:.6f}",
                    f"{read_summary['avg_ms']:.6f}",
                    f"{read_summary['p50_ms']:.6f}",
                    f"{read_summary['p95_ms']:.6f}",
                    f"{read_summary['p99_ms']:.6f}",
                    str(read_summary["modes"]),
                    f"{read_elapsed:.6f}",
                    args.keys,
                    args.tx,
                    args.concurrency,
                    args.persistent,
                    args.single_rpc,
                ]
            )
            writer.writerow(
                [
                    "write",
                    write_summary["ok_count"],
                    write_summary["total"],
                    f"{write_summary['throughput_tx_s']:.6f}",
                    f"{write_summary['avg_ms']:.6f}",
                    f"{write_summary['p50_ms']:.6f}",
                    f"{write_summary['p95_ms']:.6f}",
                    f"{write_summary['p99_ms']:.6f}",
                    str(write_summary["modes"]),
                    f"{write_elapsed:.6f}",
                    args.keys,
                    args.tx,
                    args.concurrency,
                    args.persistent,
                    args.single_rpc,
                ]
            )

        print(f"CSV written: {out_file}", flush=True)
    finally:
        # Close thread-local persistent clients created on the main thread.
        main_pc = getattr(_thread_local, "persistent_client", None)
        if main_pc is not None:
            main_pc.close_all()
        cluster.stop_all()
        print("Cluster stopped.", flush=True)


if __name__ == "__main__":
    main()
