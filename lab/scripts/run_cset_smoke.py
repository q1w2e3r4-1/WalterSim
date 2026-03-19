"""Smoke test for Cset conflict-free behavior across multiple sites."""

from pathlib import Path
import sys
import time

LAB_ROOT = Path(__file__).resolve().parent.parent
SITE_DIR = LAB_ROOT / "site"
if str(LAB_ROOT) not in sys.path:
    sys.path.insert(0, str(LAB_ROOT))
if str(SITE_DIR) not in sys.path:
    sys.path.insert(0, str(SITE_DIR))

from cluster import ClusterManager
from network.rpc import MessageTypes, RpcClient


def _run_cset_tx(client: RpcClient, site_id: int, oid: str, ops: list[tuple[str, str]]) -> dict:
    start = client.send_request(site_id, {"type": MessageTypes.TX_START}, apply_delay=False)
    if not start.get("ok"):
        raise RuntimeError(f"TX_START failed at site {site_id}: {start}")
    tid = int(start["tid"])

    for op, element_id in ops:
        msg_type = MessageTypes.TX_CSET_ADD if op == "ADD" else MessageTypes.TX_CSET_DEL
        resp = client.send_request(
            site_id,
            {
                "type": msg_type,
                "tid": tid,
                "oid": oid,
                "element_id": element_id,
            },
            apply_delay=False,
        )
        if not resp.get("ok"):
            raise RuntimeError(f"CSET op failed at site {site_id}: {resp}")

    commit = client.send_request(
        site_id,
        {"type": MessageTypes.TX_COMMIT_LOCAL, "tid": tid},
        apply_delay=False,
    )
    if not commit.get("ok"):
        raise RuntimeError(f"Commit failed at site {site_id}: {commit}")
    if commit.get("commit_mode") != "FAST":
        raise RuntimeError(f"Cset-only tx should use FAST commit, got: {commit}")

    return commit


def main() -> None:
    cluster = ClusterManager()
    client = RpcClient(timeout_seconds=6.0)

    cluster.start_all()
    try:
        cluster.wait_healthy()
        print("Cluster is healthy.", flush=True)

        cset_oid = "cset:friends"

        c0 = _run_cset_tx(client, 0, cset_oid, [("ADD", "alice"), ("ADD", "bob")])
        c1 = _run_cset_tx(client, 1, cset_oid, [("ADD", "alice"), ("DEL", "bob")])
        c2 = _run_cset_tx(client, 2, cset_oid, [("DEL", "alice")])

        print(f"commit@0 => {c0}", flush=True)
        print(f"commit@1 => {c1}", flush=True)
        print(f"commit@2 => {c2}", flush=True)

        # Wait briefly for async propagation to settle across all sites.
        time.sleep(1.0)

        expected_members = ["alice"]
        for site_id in [0, 1, 2, 3]:
            read_resp = client.send_request(
                site_id,
                {"type": MessageTypes.TX_CSET_READ, "oid": cset_oid},
                apply_delay=False,
            )
            if not read_resp.get("ok"):
                raise RuntimeError(f"TX_CSET_READ failed at site {site_id}: {read_resp}")
            members = read_resp.get("members")
            print(f"site {site_id} members => {members}", flush=True)
            if members != expected_members:
                raise RuntimeError(
                    f"Cset mismatch at site {site_id}: expected {expected_members}, got {members}"
                )

        print("Cset smoke passed.", flush=True)
    finally:
        cluster.stop_all()
        print("Cluster stopped.", flush=True)


if __name__ == "__main__":
    main()
