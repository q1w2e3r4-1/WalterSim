"""Cluster-level process management utilities."""

from __future__ import annotations

import multiprocessing as mp
import time
from typing import Dict, Iterable, Tuple

from core.config import SITE_IDS
from network.rpc import MessageTypes, RpcClient
from node import run_site_process


def _run_site_entry(site_id: int) -> None:
    run_site_process(site_id)


class ClusterManager:
    """Starts and controls local multi-process site cluster for experiments."""

    def __init__(self, site_ids: Iterable[int] | None = None):
        self.processes: Dict[int, mp.Process] = {}
        self.site_ids = list(SITE_IDS if site_ids is None else site_ids)

    def start_all(self) -> Dict[int, mp.Process]:
        for site_id in self.site_ids:
            proc = mp.Process(target=_run_site_entry, args=(site_id,), daemon=True)
            proc.start()
            self.processes[site_id] = proc
        return self.processes

    def wait_healthy(self, timeout_seconds: float = 8.0) -> None:
        client = RpcClient(timeout_seconds=1.0)
        deadline = time.time() + timeout_seconds
        pending = set(self.site_ids)

        while time.time() < deadline and pending:
            done = set()
            for site_id in pending:
                try:
                    resp = client.send_request(site_id, {"type": MessageTypes.HEALTH}, apply_delay=False)
                    if resp.get("ok"):
                        done.add(site_id)
                except OSError:
                    pass
            pending -= done
            if pending:
                time.sleep(0.1)

        if pending:
            raise RuntimeError(f"sites not healthy before timeout: {sorted(pending)}")

    def run_mesh_ping(self) -> Dict[Tuple[int, int], float]:
        client = RpcClient(timeout_seconds=5.0)
        results: Dict[Tuple[int, int], float] = {}

        for src in self.site_ids:
            for dst in self.site_ids:
                if src == dst:
                    continue
                resp = client.send_request(
                    to_site=src,
                    message={"type": MessageTypes.PING_PEER, "target_site": dst},
                    apply_delay=False,
                )
                if not resp.get("ok"):
                    raise RuntimeError(f"ping failed for {src}->{dst}: {resp}")
                measured = float(resp["peer_response"]["measured_rtt_ms"])
                results[(src, dst)] = measured
        return results

    def stop_all(self) -> None:
        client = RpcClient(timeout_seconds=1.0)
        for site_id in self.site_ids:
            try:
                client.send_request(site_id, {"type": MessageTypes.STOP}, apply_delay=False)
            except Exception:
                pass

        for proc in self.processes.values():
            proc.join(timeout=1.0)
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=1.0)


def run_cluster_demo() -> None:
    """Run the current minimal communication-loop health and mesh test."""

    cluster = ClusterManager()
    cluster.start_all()
    try:
        cluster.wait_healthy()
        print("All 4 sites are healthy.", flush=True)

        results = cluster.run_mesh_ping()
        print("\nMesh ping result (approx RTT ms):", flush=True)
        for (src, dst), value in sorted(results.items()):
            print(f"{src} -> {dst}: {value:.3f} ms", flush=True)
    finally:
        cluster.stop_all()
        print("All sites stopped.", flush=True)
