from __future__ import annotations

import json
import socket
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

SITE_IDS = [0, 1, 2, 3]
SITE_NAMES = {0: "VA", 1: "CA", 2: "IE", 3: "SG"}
SITE_PORTS = {0: 5001, 1: 5002, 2: 5003, 3: 5004}

# RTT values from the paper experiment setup (seconds).
LATENCY_MATRIX_RTT = {
    0: [0.000, 0.082, 0.087, 0.261],
    1: [0.082, 0.000, 0.153, 0.190],
    2: [0.087, 0.153, 0.000, 0.277],
    3: [0.261, 0.190, 0.277, 0.000],
}


@dataclass
class SiteAddress:
    site_id: int
    host: str
    port: int


def get_site_address(site_id: int) -> SiteAddress:
    return SiteAddress(site_id=site_id, host="127.0.0.1", port=SITE_PORTS[site_id])


def get_link_delay_seconds(from_site: int, to_site: int) -> float:
    return LATENCY_MATRIX_RTT[from_site][to_site]


class JsonCodec:
    @staticmethod
    def encode(message: Dict[str, Any]) -> bytes:
        return (json.dumps(message, ensure_ascii=True) + "\n").encode("utf-8")

    @staticmethod
    def decode(raw: bytes) -> Dict[str, Any]:
        return json.loads(raw.decode("utf-8"))


class RpcClient:
    def __init__(self, timeout_seconds: float = 5.0):
        self.timeout_seconds = timeout_seconds

    def send_request(
        self,
        to_site: int,
        message: Dict[str, Any],
        from_site: Optional[int] = None,
        apply_delay: bool = True,
    ) -> Dict[str, Any]:
        if from_site is not None and apply_delay:
            time.sleep(get_link_delay_seconds(from_site, to_site))

        address = get_site_address(to_site)
        payload = dict(message)
        if from_site is not None:
            payload.setdefault("from_site", from_site)

        with socket.create_connection((address.host, address.port), timeout=self.timeout_seconds) as conn:
            conn.sendall(JsonCodec.encode(payload))
            conn.shutdown(socket.SHUT_WR)
            response_raw = self._read_until_newline(conn)

        if not response_raw:
            raise ConnectionError(f"empty response from site={to_site}")

        return JsonCodec.decode(response_raw)

    def ping(self, from_site: int, to_site: int) -> Dict[str, Any]:
        start = time.perf_counter()
        response = self.send_request(
            to_site=to_site,
            message={"type": "PING", "send_ts": time.time()},
            from_site=from_site,
            apply_delay=True,
        )
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        response["measured_rtt_ms"] = round(elapsed_ms, 3)
        return response

    @staticmethod
    def _read_until_newline(conn: socket.socket) -> bytes:
        chunks = []
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
            if b"\n" in chunk:
                break
        raw = b"".join(chunks)
        if b"\n" in raw:
            raw = raw.split(b"\n", 1)[0]
        return raw


class SiteNode:
    def __init__(self, site_id: int):
        self.site_id = site_id
        self.site_name = SITE_NAMES[site_id]
        self.address = get_site_address(site_id)
        self.rpc_client = RpcClient()
        self._server_sock: Optional[socket.socket] = None
        self._running = threading.Event()

    def serve_forever(self) -> None:
        self._running.set()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.address.host, self.address.port))
            server.listen(128)
            self._server_sock = server
            print(
                f"[site={self.site_id} {self.site_name}] listening on {self.address.host}:{self.address.port}",
                flush=True,
            )
            while self._running.is_set():
                try:
                    conn, _ = server.accept()
                except OSError:
                    break
                thread = threading.Thread(target=self._handle_connection, args=(conn,), daemon=True)
                thread.start()

    def stop(self) -> None:
        self._running.clear()
        if self._server_sock is not None:
            try:
                self._server_sock.close()
            except OSError:
                pass

    def _handle_connection(self, conn: socket.socket) -> None:
        with conn:
            try:
                raw = RpcClient._read_until_newline(conn)
                if not raw:
                    return
                request = JsonCodec.decode(raw)
                response = self._handle_request(request)
            except Exception as exc:  # noqa: BLE001
                response = {"ok": False, "error": f"internal_error: {exc}"}

            conn.sendall(JsonCodec.encode(response))

    def _handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        msg_type = request.get("type")

        if msg_type == "PING":
            from_site = request.get("from_site")
            if isinstance(from_site, int):
                # Receiver-side one-way delay approximation for inbound leg.
                time.sleep(get_link_delay_seconds(from_site, self.site_id))
            return {
                "ok": True,
                "type": "PONG",
                "site_id": self.site_id,
                "site_name": self.site_name,
                "recv_ts": time.time(),
            }

        if msg_type == "PING_PEER":
            target_site = request.get("target_site")
            if not isinstance(target_site, int):
                return {"ok": False, "error": "target_site must be int"}
            if target_site not in SITE_IDS:
                return {"ok": False, "error": f"unknown target_site={target_site}"}
            peer_response = self.rpc_client.ping(from_site=self.site_id, to_site=target_site)
            return {
                "ok": True,
                "type": "PING_PEER_RESULT",
                "source_site": self.site_id,
                "target_site": target_site,
                "peer_response": peer_response,
            }

        if msg_type == "HEALTH":
            return {
                "ok": True,
                "type": "HEALTH_OK",
                "site_id": self.site_id,
                "site_name": self.site_name,
            }

        if msg_type == "STOP":
            self.stop()
            return {"ok": True, "type": "STOPPED", "site_id": self.site_id}

        return {"ok": False, "error": f"unknown_message_type={msg_type}"}


def run_site_process(site_id: int) -> None:
    node = SiteNode(site_id=site_id)
    node.serve_forever()
