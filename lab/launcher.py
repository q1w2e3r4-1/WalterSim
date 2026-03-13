from __future__ import annotations

import multiprocessing as mp
import time
from typing import Dict, Tuple

from walter_comm import RpcClient, SITE_IDS, run_site_process


def start_sites() -> Dict[int, mp.Process]:
    processes: Dict[int, mp.Process] = {}
    for site_id in SITE_IDS:
        proc = mp.Process(target=run_site_process, args=(site_id,), daemon=True)
        proc.start()
        processes[site_id] = proc
    return processes


def wait_until_healthy(timeout_seconds: float = 8.0) -> None:
    client = RpcClient(timeout_seconds=1.0)
    deadline = time.time() + timeout_seconds
    pending = set(SITE_IDS)

    while time.time() < deadline and pending:
        done = set()
        for site_id in pending:
            try:
                resp = client.send_request(site_id, {"type": "HEALTH"}, apply_delay=False)
                if resp.get("ok"):
                    done.add(site_id)
            except OSError:
                pass
        pending -= done
        if pending:
            time.sleep(0.1)

    if pending:
        raise RuntimeError(f"sites not healthy before timeout: {sorted(pending)}")


def run_mesh_ping() -> Dict[Tuple[int, int], float]:
    client = RpcClient(timeout_seconds=5.0)
    results: Dict[Tuple[int, int], float] = {}

    for src in SITE_IDS:
        for dst in SITE_IDS:
            if src == dst:
                continue
            resp = client.send_request(
                to_site=src,
                message={"type": "PING_PEER", "target_site": dst},
                apply_delay=False,
            )
            if not resp.get("ok"):
                raise RuntimeError(f"ping failed for {src}->{dst}: {resp}")
            measured = float(resp["peer_response"]["measured_rtt_ms"])
            results[(src, dst)] = measured
    return results


def stop_sites(processes: Dict[int, mp.Process]) -> None:
    client = RpcClient(timeout_seconds=1.0)
    for site_id in SITE_IDS:
        try:
            client.send_request(site_id, {"type": "STOP"}, apply_delay=False)
        except Exception:
            pass

    for proc in processes.values():
        proc.join(timeout=1.0)
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=1.0)


def main() -> None:
    processes = start_sites()
    try:
        wait_until_healthy()
        print("All 4 sites are healthy.", flush=True)

        results = run_mesh_ping()
        print("\nMesh ping result (approx RTT ms):", flush=True)
        for (src, dst), value in sorted(results.items()):
            print(f"{src} -> {dst}: {value:.3f} ms", flush=True)

    finally:
        stop_sites(processes)
        print("All sites stopped.", flush=True)


if __name__ == "__main__":
    main()
