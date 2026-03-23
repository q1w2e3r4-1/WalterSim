"""Smoke test for asynchronous replication delivery.

Scenario:
1) Commit a fast transaction on site 0.
2) Poll site 2 until replicated key becomes visible.
3) Report replication lag.
"""

from pathlib import Path
import sys
import time

LAB_ROOT = Path(__file__).resolve().parent.parent.parent
SITE_DIR = LAB_ROOT / "site"
if str(LAB_ROOT) not in sys.path:
    sys.path.insert(0, str(LAB_ROOT))
if str(SITE_DIR) not in sys.path:
    sys.path.insert(0, str(SITE_DIR))

from cluster import ClusterManager
from network.rpc import MessageTypes, RpcClient


def main() -> None:
    cluster = ClusterManager()
    client = RpcClient(timeout_seconds=6.0)

    cluster.start_all()
    try:
        cluster.wait_healthy()
        print("Cluster is healthy.", flush=True)

        source_site = 0
        target_site = 2
        oid = "ps0:replicate_demo"
        value = {"msg": "replicated", "ts": time.time()}

        start_resp = client.send_request(source_site, {"type": MessageTypes.TX_START}, apply_delay=False)
        tid = int(start_resp["tid"])

        wr = client.send_request(
            source_site,
            {"type": MessageTypes.TX_WRITE, "tid": tid, "oid": oid, "value": value},
            apply_delay=False,
        )
        if not wr.get("ok"):
            raise RuntimeError(f"TX_WRITE failed: {wr}")

        t0 = time.time()
        commit = client.send_request(
            source_site,
            {"type": MessageTypes.TX_COMMIT_LOCAL, "tid": tid},
            apply_delay=False,
        )
        if not commit.get("ok"):
            raise RuntimeError(f"Commit failed: {commit}")
        print(f"source commit: {commit}", flush=True)

        deadline = time.time() + 8.0
        last_read = None
        while time.time() < deadline:
            read_resp = client.send_request(
                target_site,
                {"type": MessageTypes.TX_READ, "oid": oid},
                apply_delay=False,
            )
            last_read = read_resp
            if read_resp.get("ok") and read_resp.get("value") == value:
                lag_ms = (time.time() - t0) * 1000.0
                print(f"replication visible at site {target_site} after {lag_ms:.2f} ms", flush=True)
                print("Replication smoke passed.", flush=True)
                return
            time.sleep(0.05)

        raise RuntimeError(f"replication timeout; last read response: {last_read}")
    finally:
        cluster.stop_all()
        print("Cluster stopped.", flush=True)


if __name__ == "__main__":
    main()
