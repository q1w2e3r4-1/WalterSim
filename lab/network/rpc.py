"""RPC/message transport abstractions.

This module contains the concrete transport used by the current minimal
communication loop and can later be extended for protocol messages.
"""

from __future__ import annotations

import json
import socket
import threading
import time
from typing import Any, Callable, Dict, Optional

from core.config import get_link_delay_seconds, get_site_address


class MessageTypes:
    """Enum-like container for message names used by the current scaffold."""

    PING = "PING"
    PONG = "PONG"
    PING_PEER = "PING_PEER"
    PING_PEER_RESULT = "PING_PEER_RESULT"
    HEALTH = "HEALTH"
    HEALTH_OK = "HEALTH_OK"
    STOP = "STOP"
    STOPPED = "STOPPED"
    TX_START = "TX_START"
    TX_STARTED = "TX_STARTED"
    TX_WRITE = "TX_WRITE"
    TX_WRITE_OK = "TX_WRITE_OK"
    TX_COMMIT_LOCAL = "TX_COMMIT_LOCAL"
    TX_COMMIT_OK = "TX_COMMIT_OK"
    TX_COMMIT_ABORT = "TX_COMMIT_ABORT"
    TX_READ = "TX_READ"
    TX_READ_RESULT = "TX_READ_RESULT"
    TX_CSET_ADD = "TX_CSET_ADD"
    TX_CSET_DEL = "TX_CSET_DEL"
    TX_CSET_OP_OK = "TX_CSET_OP_OK"
    TX_CSET_READ = "TX_CSET_READ"
    TX_CSET_READ_RESULT = "TX_CSET_READ_RESULT"
    TX_PREPARE = "TX_PREPARE"
    TX_PREPARE_OK = "TX_PREPARE_OK"
    TX_PREPARE_NO = "TX_PREPARE_NO"
    TX_REMOTE_COMMIT = "TX_REMOTE_COMMIT"
    TX_REMOTE_ABORT = "TX_REMOTE_ABORT"
    TX_PROPAGATE = "TX_PROPAGATE"
    TX_PROPAGATE_APPLIED = "TX_PROPAGATE_APPLIED"
    TX_PROPAGATE_QUEUED = "TX_PROPAGATE_QUEUED"
    TX_PROPAGATE_BATCH = "TX_PROPAGATE_BATCH"
    TX_PROPAGATE_BATCH_ACK = "TX_PROPAGATE_BATCH_ACK"
    TX_READ_ONE_TX = "TX_READ_ONE_TX"
    TX_WRITE_ONE_TX = "TX_WRITE_ONE_TX"
    TX_BENCH_REGULAR_TX = "TX_BENCH_REGULAR_TX"
    TX_BENCH_CSET_TX = "TX_BENCH_CSET_TX"
    TX_BENCH_FAST_TX = "TX_BENCH_FAST_TX"
    TX_BENCH_FAST_DURABILITY_TX = "TX_BENCH_FAST_DURABILITY_TX"
    TX_BENCH_SLOW_DURABILITY_TX = "TX_BENCH_SLOW_DURABILITY_TX"


class JsonCodec:
    @staticmethod
    def encode(message: Dict[str, Any]) -> bytes:
        return (json.dumps(message, ensure_ascii=True) + "\n").encode("utf-8")

    @staticmethod
    def decode(raw: bytes) -> Dict[str, Any]:
        return json.loads(raw.decode("utf-8"))


class RpcClient:
    """Client-side request helper for local process and inter-site RPC calls."""

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
            response_raw = self.read_until_newline(conn)

        if not response_raw:
            raise ConnectionError(f"empty response from site={to_site}")

        return JsonCodec.decode(response_raw)

    def ping(self, from_site: int, to_site: int) -> Dict[str, Any]:
        start = time.perf_counter()
        response = self.send_request(
            to_site=to_site,
            message={"type": MessageTypes.PING, "send_ts": time.time()},
            from_site=from_site,
            apply_delay=True,
        )
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        response["measured_rtt_ms"] = round(elapsed_ms, 3)
        return response

    @staticmethod
    def read_until_newline(conn: socket.socket) -> bytes:
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


class PersistentRpcClient:
    """Thread-safe RPC client with optional long-lived connection per target site."""

    def __init__(self, timeout_seconds: float = 5.0):
        self.timeout_seconds = timeout_seconds
        self._conns: Dict[int, socket.socket] = {}
        self._locks: Dict[int, threading.Lock] = {}

    def close_all(self) -> None:
        for conn in self._conns.values():
            try:
                conn.close()
            except OSError:
                pass
        self._conns.clear()
        self._locks.clear()

    def _get_conn(self, to_site: int) -> socket.socket:
        conn = self._conns.get(to_site)
        if conn is not None:
            return conn
        address = get_site_address(to_site)
        conn = socket.create_connection((address.host, address.port), timeout=self.timeout_seconds)
        conn.settimeout(self.timeout_seconds)
        self._conns[to_site] = conn
        self._locks[to_site] = threading.Lock()
        return conn

    def send_request(
        self,
        to_site: int,
        message: Dict[str, Any],
        from_site: Optional[int] = None,
        apply_delay: bool = True,
    ) -> Dict[str, Any]:
        if from_site is not None and apply_delay:
            time.sleep(get_link_delay_seconds(from_site, to_site))

        payload = dict(message)
        if from_site is not None:
            payload.setdefault("from_site", from_site)

        conn = self._get_conn(to_site)
        lock = self._locks[to_site]
        with lock:
            try:
                conn.sendall(JsonCodec.encode(payload))
                response_raw = RpcClient.read_until_newline(conn)
            except OSError:
                # Retry once with a fresh connection if peer closed/reset.
                try:
                    conn.close()
                except OSError:
                    pass
                self._conns.pop(to_site, None)
                self._locks.pop(to_site, None)
                conn = self._get_conn(to_site)
                lock = self._locks[to_site]
                with lock:
                    conn.sendall(JsonCodec.encode(payload))
                    response_raw = RpcClient.read_until_newline(conn)

        if not response_raw:
            raise ConnectionError(f"empty response from site={to_site}")

        return JsonCodec.decode(response_raw)


class RpcServer:
    """Socket listener and request dispatcher for one site runtime."""

    def __init__(self, host: str, port: int, handler: Callable[[Dict[str, Any]], Dict[str, Any]]):
        self.host = host
        self.port = port
        self.handler = handler
        self._server_sock: Optional[socket.socket] = None
        self._running = threading.Event()

    def serve_forever(self) -> None:
        self._running.set()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            server.listen(128)
            self._server_sock = server

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
            while self._running.is_set():
                try:
                    raw = RpcClient.read_until_newline(conn)
                    if not raw:
                        return
                    request = JsonCodec.decode(raw)
                    response = self.handler(request)
                except Exception as exc:  # noqa: BLE001
                    response = {"ok": False, "error": f"internal_error: {exc}"}

                try:
                    conn.sendall(JsonCodec.encode(response))
                except OSError:
                    return
