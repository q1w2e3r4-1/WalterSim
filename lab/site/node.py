"""Per-site runtime assembly and handlers.

Current implementation hosts the minimal communication-loop handlers used for
health checks and mesh ping tests.
"""

from __future__ import annotations

import time
from typing import Any, Dict

from core.config import SITE_IDS, SITE_NAMES, get_link_delay_seconds, get_site_address
from network.rpc import MessageTypes, RpcClient, RpcServer


class WalterSiteRuntime:
    """Aggregates networking and request handlers for one site process."""

    def __init__(self, site_id: int):
        self.site_id = site_id
        self.site_name = SITE_NAMES[site_id]
        self.address = get_site_address(site_id)
        self.rpc_client = RpcClient()
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
