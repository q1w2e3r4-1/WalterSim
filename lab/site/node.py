"""Per-site runtime assembly and handlers.

Current implementation hosts the minimal communication-loop handlers used for
health checks and mesh ping tests.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict

from core.config import SITE_IDS, SITE_NAMES, get_link_delay_seconds, get_site_address
from core.types_store import SiteClock, Transaction, VersionedObjectStore
from network.rpc import MessageTypes, RpcClient, RpcServer


class WalterSiteRuntime:
    """Aggregates networking and request handlers for one site process."""

    def __init__(self, site_id: int):
        self.site_id = site_id
        self.site_name = SITE_NAMES[site_id]
        self.address = get_site_address(site_id)
        self.rpc_client = RpcClient()
        self.clock = SiteClock(site_id=site_id)
        self.store = VersionedObjectStore()
        self.active_txs: Dict[int, Transaction] = {}
        self._next_tid = 1
        self._state_lock = threading.Lock()
        self.rpc_server = RpcServer(
            host=self.address.host,
            port=self.address.port,
            handler=self._handle_request,
        )

    def serve_forever(self) -> None:
        print(
            f"[site={self.site_id} {self.site_name}] listening on {self.address.host}:{self.address.port}",
            flush=True,
        )
        self.rpc_server.serve_forever()

    def stop(self) -> None:
        self.rpc_server.stop()

    def _handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        msg_type = request.get("type")

        if msg_type == MessageTypes.TX_START:
            with self._state_lock:
                tid = self._next_tid
                self._next_tid += 1
                tx = Transaction(tid=tid, start_vts=self.clock.current_snapshot())
                self.active_txs[tid] = tx
            return {
                "ok": True,
                "type": MessageTypes.TX_STARTED,
                "site_id": self.site_id,
                "tid": tid,
                "start_vts": tx.start_vts.to_dict(),
            }

        if msg_type == MessageTypes.TX_WRITE:
            tid = request.get("tid")
            oid = request.get("oid")
            value = request.get("value")
            if not isinstance(tid, int):
                return {"ok": False, "error": "tid must be int"}
            if not isinstance(oid, str):
                return {"ok": False, "error": "oid must be str"}

            with self._state_lock:
                tx = self.active_txs.get(tid)
                if tx is None:
                    return {"ok": False, "error": f"unknown tid={tid}"}
                tx.add_write(oid=oid, payload=value)

            return {
                "ok": True,
                "type": MessageTypes.TX_WRITE_OK,
                "tid": tid,
                "buffered_writes": len(tx.updates),
            }

        if msg_type == MessageTypes.TX_COMMIT_LOCAL:
            tid = request.get("tid")
            if not isinstance(tid, int):
                return {"ok": False, "error": "tid must be int"}

            with self._state_lock:
                tx = self.active_txs.get(tid)
                if tx is None:
                    return {"ok": False, "error": f"unknown tid={tid}"}

                # First fast-commit guard: abort if any written key changed
                # since transaction start snapshot.
                write_oids = {op.oid for op in tx.updates if op.op_type == "WRITE"}
                for oid in write_oids:
                    if self.store.was_modified_since(oid=oid, start_vts=tx.start_vts):
                        tx.status = "ABORTED"
                        self.active_txs.pop(tid, None)
                        return {
                            "ok": False,
                            "type": MessageTypes.TX_COMMIT_ABORT,
                            "tid": tid,
                            "reason": f"write_conflict_on={oid}",
                        }

                tx.status = "COMMITTING"
                commit_version = self.clock.next_version()
                for op in tx.updates:
                    if op.op_type == "WRITE":
                        self.store.put(oid=op.oid, value=op.payload, version=commit_version)
                tx.commit_version = commit_version
                tx.status = "COMMITTED"
                self.active_txs.pop(tid, None)

            return {
                "ok": True,
                "type": MessageTypes.TX_COMMIT_OK,
                "tid": tid,
                "commit_version": commit_version.to_dict(),
                "write_count": len(tx.updates),
            }

        if msg_type == MessageTypes.TX_READ:
            tid = request.get("tid")
            oid = request.get("oid")
            if not isinstance(oid, str):
                return {"ok": False, "error": "oid must be str"}

            buffered_value = None
            visible_value = None
            if isinstance(tid, int):
                with self._state_lock:
                    tx = self.active_txs.get(tid)
                    if tx is not None:
                        buffered_value = tx.get_buffered_write(oid)
                        if buffered_value is None:
                            visible_value = self.store.get_visible(oid=oid, start_vts=tx.start_vts)

            if buffered_value is not None:
                return {
                    "ok": True,
                    "type": MessageTypes.TX_READ_RESULT,
                    "oid": oid,
                    "value": buffered_value,
                    "source": "TX_BUFFER",
                }

            if visible_value is not None:
                return {
                    "ok": True,
                    "type": MessageTypes.TX_READ_RESULT,
                    "oid": oid,
                    "value": visible_value,
                    "source": "TX_SNAPSHOT",
                }

            value = self.store.get_latest(oid)
            return {
                "ok": True,
                "type": MessageTypes.TX_READ_RESULT,
                "oid": oid,
                "value": value,
                "source": "STORE_LATEST",
            }

        if msg_type == MessageTypes.PING:
            from_site = request.get("from_site")
            if isinstance(from_site, int):
                # Receiver-side one-way delay approximation for inbound leg.
                time.sleep(get_link_delay_seconds(from_site, self.site_id))
            return {
                "ok": True,
                "type": MessageTypes.PONG,
                "site_id": self.site_id,
                "site_name": self.site_name,
                "recv_ts": time.time(),
            }

        if msg_type == MessageTypes.PING_PEER:
            target_site = request.get("target_site")
            if not isinstance(target_site, int):
                return {"ok": False, "error": "target_site must be int"}
            if target_site not in SITE_IDS:
                return {"ok": False, "error": f"unknown target_site={target_site}"}
            peer_response = self.rpc_client.ping(from_site=self.site_id, to_site=target_site)
            return {
                "ok": True,
                "type": MessageTypes.PING_PEER_RESULT,
                "source_site": self.site_id,
                "target_site": target_site,
                "peer_response": peer_response,
            }

        if msg_type == MessageTypes.HEALTH:
            return {
                "ok": True,
                "type": MessageTypes.HEALTH_OK,
                "site_id": self.site_id,
                "site_name": self.site_name,
            }

        if msg_type == MessageTypes.STOP:
            self.stop()
            return {"ok": True, "type": MessageTypes.STOPPED, "site_id": self.site_id}

        return {"ok": False, "error": f"unknown_message_type={msg_type}"}


class SiteNode(WalterSiteRuntime):
    """Backward-compatible alias while old entrypoints still import SiteNode."""


def run_site(site_id: int) -> None:
    """Process entry point for one site instance."""

    runtime = WalterSiteRuntime(site_id=site_id)
    runtime.serve_forever()


def run_site_process(site_id: int) -> None:
    """Backward-compatible process entry used by existing launcher paths."""

    run_site(site_id)
