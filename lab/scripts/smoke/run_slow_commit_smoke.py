"""Smoke test for minimal slow-commit (2PC) flow.

Scenario:
1) Start transaction at site 0.
2) Write one local-preferred key and one remote-preferred key.
3) Commit via TX_COMMIT_LOCAL (auto path selection).
4) Verify coordinator reports SLOW_2PC and remote site can read the value.
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
    client = RpcClient(timeout_seconds=6.0)

    cluster.start_all()
    try:
        cluster.wait_healthy()
        print("Cluster is healthy.", flush=True)

        coordinator_site = 0
        remote_site = 1
        local_key = "ps0:slow_demo_local"
        remote_key = "ps1:slow_demo_remote"

        start = client.send_request(coordinator_site, {"type": MessageTypes.TX_START}, apply_delay=False)
        if not start.get("ok"):
            raise RuntimeError(f"TX_START failed: {start}")
        tid = int(start["tid"])

        for oid, value in [(local_key, "local_v"), (remote_key, "remote_v")]:
            wr = client.send_request(
                coordinator_site,
                {"type": MessageTypes.TX_WRITE, "tid": tid, "oid": oid, "value": value},
                apply_delay=False,
            )
            if not wr.get("ok"):
                raise RuntimeError(f"TX_WRITE failed for {oid}: {wr}")

        commit = client.send_request(
            coordinator_site,
            {"type": MessageTypes.TX_COMMIT_LOCAL, "tid": tid},
            apply_delay=False,
        )
        if not commit.get("ok"):
            raise RuntimeError(f"Slow commit failed: {commit}")
        if commit.get("commit_mode") != "SLOW_2PC":
            raise RuntimeError(f"Expected SLOW_2PC mode, got: {commit}")
        print(f"slow commit result: {commit}", flush=True)

        remote_read = client.send_request(
            remote_site,
            {"type": MessageTypes.TX_READ, "oid": remote_key},
            apply_delay=False,
        )
        if not remote_read.get("ok"):
            raise RuntimeError(f"Remote read failed: {remote_read}")
        if remote_read.get("value") != "remote_v":
            raise RuntimeError(f"Remote read mismatch: {remote_read}")

        print("Slow-commit smoke passed.", flush=True)
    finally:
        cluster.stop_all()
        print("Cluster stopped.", flush=True)


if __name__ == "__main__":
    main()
