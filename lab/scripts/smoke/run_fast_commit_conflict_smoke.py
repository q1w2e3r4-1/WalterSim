"""Smoke test for first fast-commit conflict behavior.

Scenario:
1) Start tx1 and tx2 from the same snapshot.
2) Both write the same key.
3) Commit tx1 first -> success.
4) Commit tx2 second -> should abort by write-write conflict.
"""

from pathlib import Path
import sys

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
    client = RpcClient(timeout_seconds=3.0)

    cluster.start_all()
    try:
        cluster.wait_healthy()
        print("Cluster is healthy.", flush=True)

        site_id = 0
        oid = "demo:key:fast_conflict"

        tx1 = client.send_request(site_id, {"type": MessageTypes.TX_START}, apply_delay=False)
        tx2 = client.send_request(site_id, {"type": MessageTypes.TX_START}, apply_delay=False)
        tid1 = int(tx1["tid"])
        tid2 = int(tx2["tid"])
        print(f"Started tx1={tid1}, tx2={tid2}", flush=True)

        client.send_request(
            site_id,
            {"type": MessageTypes.TX_WRITE, "tid": tid1, "oid": oid, "value": "v1"},
            apply_delay=False,
        )
        client.send_request(
            site_id,
            {"type": MessageTypes.TX_WRITE, "tid": tid2, "oid": oid, "value": "v2"},
            apply_delay=False,
        )

        commit1 = client.send_request(
            site_id,
            {"type": MessageTypes.TX_COMMIT_LOCAL, "tid": tid1},
            apply_delay=False,
        )
        if not commit1.get("ok"):
            raise RuntimeError(f"tx1 should commit, got: {commit1}")
        print(f"tx1 commit: {commit1}", flush=True)

        commit2 = client.send_request(
            site_id,
            {"type": MessageTypes.TX_COMMIT_LOCAL, "tid": tid2},
            apply_delay=False,
        )
        if commit2.get("ok"):
            raise RuntimeError(f"tx2 should abort by conflict, got: {commit2}")
        if commit2.get("type") != MessageTypes.TX_COMMIT_ABORT:
            raise RuntimeError(f"tx2 should return TX_COMMIT_ABORT, got: {commit2}")
        print(f"tx2 abort: {commit2}", flush=True)

        read_back = client.send_request(
            site_id,
            {"type": MessageTypes.TX_READ, "oid": oid},
            apply_delay=False,
        )
        if read_back.get("value") != "v1":
            raise RuntimeError(f"expected committed value 'v1', got: {read_back}")

        print("Fast-commit conflict smoke passed.", flush=True)
    finally:
        cluster.stop_all()
        print("Cluster stopped.", flush=True)


if __name__ == "__main__":
    main()
