"""Smoke test for out-of-order propagation handling.

Scenario:
1) Send TX_PROPAGATE with seq=2 first -> should be queued.
2) Send TX_PROPAGATE with seq=1 next -> should be applied.
3) The drain logic should then apply queued seq=2 automatically.
4) Final read should observe value from seq=2.
"""

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

        target_site = 2
        origin_site = 0
        oid = "ps0:ooo_propagation_key"

        seq2_payload = {
            "type": MessageTypes.TX_PROPAGATE,
            "origin_site": origin_site,
            "origin_seq_no": 2,
            "start_vts": {},
            "writes": [{"oid": oid, "value": "v2"}],
        }
        seq1_payload = {
            "type": MessageTypes.TX_PROPAGATE,
            "origin_site": origin_site,
            "origin_seq_no": 1,
            "start_vts": {},
            "writes": [{"oid": oid, "value": "v1"}],
        }

        resp_seq2 = client.send_request(target_site, seq2_payload, apply_delay=False)
        print(f"send seq2 first => {resp_seq2}", flush=True)
        if not resp_seq2.get("ok"):
            raise RuntimeError(f"seq2 request failed: {resp_seq2}")
        if resp_seq2.get("type") != MessageTypes.TX_PROPAGATE_QUEUED:
            raise RuntimeError(f"seq2 should be queued first, got: {resp_seq2}")

        resp_seq1 = client.send_request(target_site, seq1_payload, apply_delay=False)
        print(f"send seq1 second => {resp_seq1}", flush=True)
        if not resp_seq1.get("ok"):
            raise RuntimeError(f"seq1 request failed: {resp_seq1}")
        if resp_seq1.get("type") != MessageTypes.TX_PROPAGATE_APPLIED:
            raise RuntimeError(f"seq1 should be applied, got: {resp_seq1}")

        read_back = client.send_request(
            target_site,
            {"type": MessageTypes.TX_READ, "oid": oid},
            apply_delay=False,
        )
        print(f"read after drain => {read_back}", flush=True)
        if not read_back.get("ok"):
            raise RuntimeError(f"read failed: {read_back}")
        if read_back.get("value") != "v2":
            raise RuntimeError("expected final value from seq2 after drain")

        print("Out-of-order propagation smoke passed.", flush=True)
    finally:
        cluster.stop_all()
        print("Cluster stopped.", flush=True)


if __name__ == "__main__":
    main()
