"""Smoke test for minimal transaction flow over the site RPC layer."""

from pathlib import Path
import sys

LAB_ROOT = Path(__file__).resolve().parent.parent
SITE_DIR = LAB_ROOT / "site"
if str(LAB_ROOT) not in sys.path:
    sys.path.insert(0, str(LAB_ROOT))
if str(SITE_DIR) not in sys.path:
    sys.path.insert(0, str(SITE_DIR))

from cluster import ClusterManager
from network.rpc import MessageTypes, RpcClient


def main() -> None:
    cluster = ClusterManager()
    client = RpcClient(timeout_seconds=3.0)

    cluster.start_all()
    try:
        cluster.wait_healthy()
        print("Cluster is healthy.", flush=True)

        site_id = 0
        oid = "demo:key:1"
        value = {"name": "walter", "step": "tx_smoke"}

        start_resp = client.send_request(site_id, {"type": MessageTypes.TX_START}, apply_delay=False)
        if not start_resp.get("ok"):
            raise RuntimeError(f"TX_START failed: {start_resp}")
        tid = int(start_resp["tid"])
        print(f"TX started: tid={tid}", flush=True)

        write_resp = client.send_request(
            site_id,
            {"type": MessageTypes.TX_WRITE, "tid": tid, "oid": oid, "value": value},
            apply_delay=False,
        )
        if not write_resp.get("ok"):
            raise RuntimeError(f"TX_WRITE failed: {write_resp}")
        print(f"TX write buffered: {write_resp}", flush=True)

        commit_resp = client.send_request(
            site_id,
            {"type": MessageTypes.TX_COMMIT_LOCAL, "tid": tid},
            apply_delay=False,
        )
        if not commit_resp.get("ok"):
            raise RuntimeError(f"TX_COMMIT_LOCAL failed: {commit_resp}")
        print(f"TX committed: {commit_resp}", flush=True)

        read_resp = client.send_request(
            site_id,
            {"type": MessageTypes.TX_READ, "oid": oid},
            apply_delay=False,
        )
        if not read_resp.get("ok"):
            raise RuntimeError(f"TX_READ failed: {read_resp}")
        print(f"TX read back: {read_resp}", flush=True)

        if read_resp.get("value") != value:
            raise RuntimeError("read-back value mismatch")

        print("Smoke test passed.", flush=True)
    finally:
        cluster.stop_all()
        print("Cluster stopped.", flush=True)


if __name__ == "__main__":
    main()
