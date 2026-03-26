"""Small-scale benchmark: Cset vs Regular cross-site commit performance.

Regular workload:
- Site 0 tx writes one local key (ps0) and one remote key (ps1), forcing SLOW_2PC.

Cset workload:
- Site 0 tx applies cset add/del operations only, expected FAST path.

Outputs:
- Console summary with throughput and latency stats.
- CSV under lab/experiments/results/csv.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
import statistics
import sys
import time
from typing import Any, List

LAB_ROOT = Path(__file__).resolve().parent.parent
SITE_DIR = LAB_ROOT / "site"
RESULT_DIR = LAB_ROOT / "experiments" / "results" / "csv"
if str(LAB_ROOT) not in sys.path:
    sys.path.insert(0, str(LAB_ROOT))
if str(SITE_DIR) not in sys.path:
    sys.path.insert(0, str(SITE_DIR))

from cluster import ClusterManager
from network.rpc import MessageTypes, PersistentRpcClient, RpcClient


@dataclass
class CaseResult:
    case_name: str
    commit_mode: str
    latency_ms: float
    ok: bool


def _percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    ranked = sorted(values)
    idx = min(len(ranked) - 1, max(0, int(round((p / 100.0) * (len(ranked) - 1)))))
    return ranked[idx]


def _run_regular_case(client: Any, i: int, use_single_rpc: bool) -> CaseResult:
    site_id = 0
    local_oid = f"ps0:bench_regular_local:{i}"
    remote_oid = f"ps1:bench_regular_remote:{i}"

    if use_single_rpc:
        t0 = time.perf_counter()
        commit = client.send_request(
            site_id,
            {
                "type": MessageTypes.TX_BENCH_REGULAR_TX,
                "local_oid": local_oid,
                "local_value": f"lv-{i}",
                "remote_oid": remote_oid,
                "remote_value": f"rv-{i}",
            },
            apply_delay=False,
        )
        latency_ms = (time.perf_counter() - t0) * 1000.0
        return CaseResult(
            case_name="regular",
            commit_mode=str(commit.get("commit_mode", "UNKNOWN")),
            latency_ms=latency_ms,
            ok=bool(commit.get("ok", False)),
        )

    start = client.send_request(site_id, {"type": MessageTypes.TX_START}, apply_delay=False)
    tid = int(start["tid"])

    client.send_request(
        site_id,
        {"type": MessageTypes.TX_WRITE, "tid": tid, "oid": local_oid, "value": f"lv-{i}"},
        apply_delay=False,
    )
    client.send_request(
        site_id,
        {"type": MessageTypes.TX_WRITE, "tid": tid, "oid": remote_oid, "value": f"rv-{i}"},
        apply_delay=False,
    )

    t0 = time.perf_counter()
    commit = client.send_request(
        site_id,
        {"type": MessageTypes.TX_COMMIT_LOCAL, "tid": tid},
        apply_delay=False,
    )
    latency_ms = (time.perf_counter() - t0) * 1000.0

    return CaseResult(
        case_name="regular",
        commit_mode=str(commit.get("commit_mode", "UNKNOWN")),
        latency_ms=latency_ms,
        ok=bool(commit.get("ok", False)),
    )


def _run_cset_case(client: Any, i: int, use_single_rpc: bool) -> CaseResult:
    site_id = 0
    cset_oid = "cset:bench_friends"

    if use_single_rpc:
        t0 = time.perf_counter()
        commit = client.send_request(
            site_id,
            {
                "type": MessageTypes.TX_BENCH_CSET_TX,
                "oid": cset_oid,
                "add_element_id": f"u{i}",
                "del_element_id": f"ghost{i}",
            },
            apply_delay=False,
        )
        latency_ms = (time.perf_counter() - t0) * 1000.0
        return CaseResult(
            case_name="cset",
            commit_mode=str(commit.get("commit_mode", "UNKNOWN")),
            latency_ms=latency_ms,
            ok=bool(commit.get("ok", False)),
        )

    start = client.send_request(site_id, {"type": MessageTypes.TX_START}, apply_delay=False)
    tid = int(start["tid"])

    client.send_request(
        site_id,
        {
            "type": MessageTypes.TX_CSET_ADD,
            "tid": tid,
            "oid": cset_oid,
            "element_id": f"u{i}",
        },
        apply_delay=False,
    )
    client.send_request(
        site_id,
        {
            "type": MessageTypes.TX_CSET_DEL,
            "tid": tid,
            "oid": cset_oid,
            "element_id": f"ghost{i}",
        },
        apply_delay=False,
    )

    t0 = time.perf_counter()
    commit = client.send_request(
        site_id,
        {"type": MessageTypes.TX_COMMIT_LOCAL, "tid": tid},
        apply_delay=False,
    )
    latency_ms = (time.perf_counter() - t0) * 1000.0

    return CaseResult(
        case_name="cset",
        commit_mode=str(commit.get("commit_mode", "UNKNOWN")),
        latency_ms=latency_ms,
        ok=bool(commit.get("ok", False)),
    )


def _summarize(case_name: str, rows: List[CaseResult], elapsed_s: float) -> str:
    latencies = [r.latency_ms for r in rows if r.ok]
    ok_count = sum(1 for r in rows if r.ok)
    throughput = (ok_count / elapsed_s) if elapsed_s > 0 else 0.0
    commit_modes = sorted(set(r.commit_mode for r in rows))

    avg = statistics.mean(latencies) if latencies else 0.0
    p50 = _percentile(latencies, 50)
    p95 = _percentile(latencies, 95)

    return (
        f"[{case_name}] ok={ok_count}/{len(rows)} "
        f"throughput={throughput:.2f} tx/s "
        f"avg={avg:.2f}ms p50={p50:.2f}ms p95={p95:.2f}ms "
        f"modes={commit_modes}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Cset vs Regular benchmark")
    parser.add_argument("--iters", type=int, default=200, help="iterations per case")
    parser.add_argument("--timeout", type=float, default=8.0, help="RPC timeout seconds")
    parser.add_argument(
        "--persistent",
        dest="persistent",
        action="store_true",
        default=True,
        help="reuse long-lived TCP connections (default: enabled)",
    )
    parser.add_argument(
        "--no-persistent",
        dest="persistent",
        action="store_false",
        help="disable long-lived TCP connection reuse",
    )
    parser.add_argument(
        "--single-rpc",
        dest="single_rpc",
        action="store_true",
        default=True,
        help="use benchmark-only single-RPC paths (default: enabled)",
    )
    parser.add_argument(
        "--no-single-rpc",
        dest="single_rpc",
        action="store_false",
        help="disable single-RPC benchmark paths",
    )
    args = parser.parse_args()

    cluster = ClusterManager()
    client: Any = PersistentRpcClient(timeout_seconds=args.timeout) if args.persistent else RpcClient(timeout_seconds=args.timeout)

    iters = args.iters

    cluster.start_all()
    try:
        cluster.wait_healthy()
        print("Cluster is healthy.", flush=True)

        regular_rows: List[CaseResult] = []
        cset_rows: List[CaseResult] = []

        t0 = time.perf_counter()
        for i in range(iters):
            regular_rows.append(_run_regular_case(client, i, use_single_rpc=args.single_rpc))
        regular_elapsed = time.perf_counter() - t0

        t1 = time.perf_counter()
        for i in range(iters):
            cset_rows.append(_run_cset_case(client, i, use_single_rpc=args.single_rpc))
        cset_elapsed = time.perf_counter() - t1

        print(_summarize("regular", regular_rows, regular_elapsed), flush=True)
        print(_summarize("cset", cset_rows, cset_elapsed), flush=True)
        print(f"config: persistent={args.persistent} single_rpc={args.single_rpc} iters={args.iters}", flush=True)

        RESULT_DIR.mkdir(parents=True, exist_ok=True)
        out_file = RESULT_DIR / "cset_vs_regular_small_benchmark.csv"
        with out_file.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["case_name", "ok", "commit_mode", "latency_ms", "persistent", "single_rpc", "iters"])
            for r in regular_rows + cset_rows:
                writer.writerow(
                    [
                        r.case_name,
                        int(r.ok),
                        r.commit_mode,
                        f"{r.latency_ms:.6f}",
                        int(args.persistent),
                        int(args.single_rpc),
                        args.iters,
                    ]
                )

        print(f"CSV written: {out_file}", flush=True)
    finally:
        if isinstance(client, PersistentRpcClient):
            client.close_all()
        cluster.stop_all()
        print("Cluster stopped.", flush=True)


if __name__ == "__main__":
    main()
