"""Per-site runtime assembly and handlers.

Current implementation hosts the minimal communication-loop handlers used for
health checks and mesh ping tests.
"""

from __future__ import annotations

import os
import queue
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set

from core.config import SITE_IDS, SITE_NAMES, get_active_site_ids, get_link_delay_seconds, get_preferred_site, get_site_address
from core.types_store import CsetStore, SiteClock, Transaction, VectorTimestamp, Version, VersionedObjectStore
from network.rpc import MessageTypes, RpcClient, RpcServer


class WalterSiteRuntime:
    """Aggregates networking and request handlers for one site process."""

    def __init__(self, site_id: int):
        self.site_id = site_id
        self.site_name = SITE_NAMES[site_id]
        self.active_site_ids = get_active_site_ids()
        self.read_base_ms = max(0.0, float(os.environ.get("WALTER_BENCH_LOCAL_READ_BASE_MS", "0.0")))
        self.write_base_ms = max(0.0, float(os.environ.get("WALTER_BENCH_LOCAL_WRITE_BASE_MS", "0.0")))
        self.cache_miss_ratio = min(1.0, max(0.0, float(os.environ.get("WALTER_BENCH_CACHE_MISS_RATIO", "0.0"))))
        self.cache_miss_penalty_ms = max(0.0, float(os.environ.get("WALTER_BENCH_CACHE_MISS_PENALTY_MS", "0.0")))
        self.address = get_site_address(site_id)
        self.rpc_client = RpcClient()
        self.clock = SiteClock(site_id=site_id)
        self.store = VersionedObjectStore()
        self.cset_store = CsetStore()
        self.active_txs: Dict[int, Transaction] = {}
        self.prepared_writes: Dict[str, Dict[str, Any]] = {}
        self.key_locks: Dict[str, str] = {}
        self.got_vts: Dict[int, int] = {sid: 0 for sid in self.active_site_ids}
        self.pending_propagations: List[Dict[str, Any]] = []
        self.propagate_batch_interval_ms = max(
            0.0,
            float(os.environ.get("WALTER_PROPAGATE_BATCH_INTERVAL_MS", "20.0")),
        )
        self.propagate_batch_max_txs = max(
            1,
            int(os.environ.get("WALTER_PROPAGATE_BATCH_MAX_TXS", "128")),
        )
        self._next_tid = 1
        self._next_durability_token = 1
        self._state_lock = threading.Lock()
        self._propagation_queue: queue.Queue[Dict[str, Any] | None] = queue.Queue()
        self._durability_pending: Dict[int, Dict[str, Any]] = {}
        self._propagation_stop = threading.Event()
        self._propagation_worker = threading.Thread(target=self._propagation_loop, daemon=True)
        self._propagation_worker.start()
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
        self._propagation_stop.set()
        self._propagation_queue.put(None)
        self.rpc_server.stop()

    def _propagation_loop(self) -> None:
        while not self._propagation_stop.is_set():
            try:
                first = self._propagation_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            if first is None:
                return

            batch_items: List[Dict[str, Any]] = [first]
            interval_s = self.propagate_batch_interval_ms / 1000.0
            if interval_s > 0.0:
                deadline = time.perf_counter() + interval_s
                while len(batch_items) < self.propagate_batch_max_txs:
                    remaining = deadline - time.perf_counter()
                    if remaining <= 0.0:
                        break
                    try:
                        item = self._propagation_queue.get(timeout=remaining)
                    except queue.Empty:
                        break
                    if item is None:
                        return
                    batch_items.append(item)
            self._flush_propagation_batch(batch_items)

    def _enqueue_propagation(self, payload: Dict[str, Any], token: Optional[int] = None) -> None:
        self._propagation_queue.put({"payload": payload, "token": token})

    def _flush_propagation_batch(self, batch_items: List[Dict[str, Any]]) -> None:
        if not batch_items:
            return

        remote_sites = [sid for sid in self.active_site_ids if sid != self.site_id]
        if not remote_sites:
            for item in batch_items:
                token = item.get("token")
                if token is None:
                    continue
                with self._state_lock:
                    pending = self._durability_pending.get(int(token))
                    if pending is None:
                        continue
                    pending["disaster_safe_ms"] = 0.0
                    pending["event"].set()
            return

        entries = [item["payload"] for item in batch_items]

        def _send_batch(site_id: int) -> Dict[str, Any]:
            started = time.perf_counter()
            try:
                resp = self.rpc_client.send_request(
                    to_site=site_id,
                    message={"type": MessageTypes.TX_PROPAGATE_BATCH, "entries": entries},
                    from_site=self.site_id,
                    apply_delay=True,
                )
                finished = time.perf_counter()
                results = resp.get("results", []) if isinstance(resp.get("results"), list) else []
                accepted_indices: Set[int] = set()
                for idx, result in enumerate(results):
                    if not isinstance(result, dict):
                        continue
                    if bool(result.get("ok")) and result.get("type") in {
                        MessageTypes.TX_PROPAGATE_APPLIED,
                        MessageTypes.TX_PROPAGATE_QUEUED,
                    }:
                        accepted_indices.add(idx)
                return {
                    "site_id": site_id,
                    "ok": bool(resp.get("ok")),
                    "accepted_indices": accepted_indices,
                    "latency_ms": (finished - started) * 1000.0,
                    "finished_ts": finished,
                }
            except Exception as exc:  # noqa: BLE001
                finished = time.perf_counter()
                return {
                    "site_id": site_id,
                    "ok": False,
                    "accepted_indices": set(),
                    "latency_ms": (finished - started) * 1000.0,
                    "finished_ts": finished,
                    "error": str(exc),
                }

        site_results: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=len(remote_sites)) as pool:
            futures = [pool.submit(_send_batch, sid) for sid in remote_sites]
            for fut in as_completed(futures):
                site_results.append(fut.result())

        for idx, item in enumerate(batch_items):
            token = item.get("token")
            if token is None:
                continue

            token_int = int(token)
            should_retry = False
            with self._state_lock:
                pending = self._durability_pending.get(token_int)
                if pending is None:
                    continue

                for site_result in site_results:
                    site_id = int(site_result["site_id"])
                    accepted = idx in site_result["accepted_indices"]
                    if accepted and site_id not in pending["acked_ts_by_site"]:
                        pending["acked_ts_by_site"][site_id] = float(site_result["finished_ts"])
                    pending["ack_details"].append(
                        {
                            "site_id": site_id,
                            "ok": bool(accepted),
                            "latency_ms": float(site_result["latency_ms"]),
                            "error": site_result.get("error"),
                        }
                    )

                if pending["required_sites"].issubset(set(pending["acked_ts_by_site"].keys())):
                    done_ts = max(pending["acked_ts_by_site"][sid] for sid in pending["required_sites"])
                    pending["disaster_safe_ms"] = (done_ts - pending["start_ts"]) * 1000.0
                    pending["event"].set()
                else:
                    should_retry = True

            if should_retry and not self._propagation_stop.is_set():
                self._propagation_queue.put(item)

    def _release_locks(self, txid: str) -> None:
        locked_oids = [oid for oid, owner in self.key_locks.items() if owner == txid]
        for oid in locked_oids:
            self.key_locks.pop(oid, None)

    def _allocate_tid_locked(self) -> int:
        tid = self._next_tid
        self._next_tid += 1
        return tid

    def _deps_satisfied(self, start_snapshot: VectorTimestamp, from_site: int) -> bool:
        for dep_site, dep_seq in start_snapshot.clocks.items():
            if dep_site == self.site_id:
                if self.clock.current_snapshot().get(self.site_id) < dep_seq:
                    return False
                continue
            if dep_site == from_site:
                continue
            if self.got_vts.get(dep_site, 0) < dep_seq:
                return False
        return True

    def _try_apply_propagation(self, payload: Dict[str, Any]) -> bool:
        from_site = int(payload["origin_site"])
        seq_no = int(payload["origin_seq_no"])
        start_vts_raw = payload.get("start_vts", {})
        writes = payload.get("writes", [])

        if self.got_vts.get(from_site, 0) >= seq_no:
            return True

        if self.got_vts.get(from_site, 0) != seq_no - 1:
            return False

        start_snapshot = VectorTimestamp(clocks={int(k): int(v) for k, v in start_vts_raw.items()})
        if not self._deps_satisfied(start_snapshot, from_site=from_site):
            return False

        version = Version(site_id=from_site, seq_no=seq_no)
        for item in writes:
            op_type = item.get("op_type", "WRITE")
            if op_type == "WRITE":
                self.store.put(oid=item["oid"], value=item["value"], version=version)
            elif op_type == "CSET_ADD":
                self.cset_store.apply_add(oid=item["oid"], element_id=str(item["element_id"]))
            elif op_type == "CSET_DEL":
                self.cset_store.apply_del(oid=item["oid"], element_id=str(item["element_id"]))

        self.got_vts[from_site] = seq_no
        return True

    def _drain_pending_propagations(self) -> None:
        while True:
            progressed = False
            remaining: List[Dict[str, Any]] = []
            for payload in self.pending_propagations:
                if self._try_apply_propagation(payload):
                    progressed = True
                else:
                    remaining.append(payload)
            self.pending_propagations = remaining
            if not progressed:
                break

    def _spawn_propagation(
        self,
        origin_site: int,
        origin_seq_no: int,
        start_snapshot: VectorTimestamp,
        writes: List[Dict[str, Any]],
    ) -> None:
        if not writes:
            return

        payload = {
            "type": MessageTypes.TX_PROPAGATE,
            "origin_site": origin_site,
            "origin_seq_no": origin_seq_no,
            "start_vts": start_snapshot.to_dict(),
            "writes": list(writes),
        }
        self._enqueue_propagation(payload=payload)

    def _wait_disaster_safe_acks(
        self,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Wait for propagation receive-acks from remotes.

        In this simplified model, a remote site returning either
        TX_PROPAGATE_APPLIED or TX_PROPAGATE_QUEUED counts as durable receive.
        """

        remote_sites = [sid for sid in self.active_site_ids if sid != self.site_id]
        required = len(remote_sites)
        if required == 0:
            return {
                "required_remote_acks": 0,
                "received_remote_acks": 0,
                "disaster_safe_ms": 0.0,
                "acked_sites": [],
                "ack_details": [],
            }

        started = time.perf_counter()
        done_event = threading.Event()
        with self._state_lock:
            token = self._next_durability_token
            self._next_durability_token += 1
            self._durability_pending[token] = {
                "start_ts": started,
                "required_sites": set(remote_sites),
                "acked_ts_by_site": {},
                "disaster_safe_ms": float("inf"),
                "ack_details": [],
                "event": done_event,
            }

        self._enqueue_propagation(payload=payload, token=token)

        wait_timeout_s = max(2.0, self.rpc_client.timeout_seconds * 8.0)
        done_event.wait(wait_timeout_s)

        with self._state_lock:
            pending = self._durability_pending.pop(token, None)

        if pending is None:
            return {
                "required_remote_acks": required,
                "received_remote_acks": 0,
                "disaster_safe_ms": float("inf"),
                "acked_sites": [],
                "ack_details": [],
            }

        acked_sites = sorted(int(sid) for sid in pending["acked_ts_by_site"].keys())
        return {
            "required_remote_acks": required,
            "received_remote_acks": len(acked_sites),
            "disaster_safe_ms": float(pending["disaster_safe_ms"]),
            "acked_sites": acked_sites,
            "ack_details": list(pending["ack_details"]),
        }

    def _handle_bench_fast_durability_tx(
        self,
        objects: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Benchmark-only path: local fast commit + wait disaster-safe receive-acks."""

        with self._state_lock:
            tid = self._allocate_tid_locked()
            tx = Transaction(tid=tid, start_vts=self.clock.current_snapshot())
            for item in objects:
                oid = str(item["oid"])
                payload = item.get("value")
                tx.add_write(oid=oid, payload=payload)

            touched_sites = {get_preferred_site(op.oid, default_site_id=self.site_id) for op in tx.updates}
            if not touched_sites.issubset({self.site_id}):
                return {
                    "ok": False,
                    "type": MessageTypes.TX_COMMIT_ABORT,
                    "reason": "non_fast_writeset",
                }

            write_oids = {op.oid for op in tx.updates if op.op_type == "WRITE"}
            for oid in write_oids:
                owner = self.key_locks.get(oid)
                if owner is not None:
                    return {
                        "ok": False,
                        "type": MessageTypes.TX_COMMIT_ABORT,
                        "reason": f"locked_key={oid}",
                    }
                if self.store.was_modified_since(oid=oid, start_vts=tx.start_vts):
                    return {
                        "ok": False,
                        "type": MessageTypes.TX_COMMIT_ABORT,
                        "reason": f"write_conflict_on={oid}",
                    }

            commit_version = self.clock.next_version()
            replicated_writes: List[Dict[str, Any]] = []
            for op in tx.updates:
                if op.op_type == "WRITE":
                    self.store.put(oid=op.oid, value=op.payload, version=commit_version)
                    replicated_writes.append({"op_type": "WRITE", "oid": op.oid, "value": op.payload})

            payload = {
                "type": MessageTypes.TX_PROPAGATE,
                "origin_site": self.site_id,
                "origin_seq_no": commit_version.seq_no,
                "start_vts": tx.start_vts.to_dict(),
                "writes": replicated_writes,
            }

        wait_info = self._wait_disaster_safe_acks(payload=payload)

        return {
            "ok": True,
            "type": MessageTypes.TX_COMMIT_OK,
            "commit_mode": "FAST",
            "write_count": len(objects),
            "commit_version": commit_version.to_dict(),
            **wait_info,
        }

    def _handle_bench_slow_durability_tx(self, objects: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Benchmark-only path: slow 2PC commit latency + post-commit disaster-safe wait.

        This uses the same simplified durability semantics as fast benchmark:
        disaster-safe wait is measured on the coordinator's propagation stream.
        """

        with self._state_lock:
            tid = self._allocate_tid_locked()
            tx = Transaction(tid=tid, start_vts=self.clock.current_snapshot())
            for item in objects:
                oid = str(item["oid"])
                payload = item.get("value")
                tx.add_write(oid=oid, payload=payload)

        write_oids = {op.oid for op in tx.updates if op.op_type == "WRITE"}
        touched_sites = {get_preferred_site(oid, default_site_id=self.site_id) for oid in write_oids}
        if len(touched_sites) <= 1:
            return {
                "ok": False,
                "type": MessageTypes.TX_COMMIT_ABORT,
                "reason": "not_slow_writeset",
            }

        commit_started = time.perf_counter()
        commit_resp = self._commit_slow_2pc(tx)
        commit_ms = (time.perf_counter() - commit_started) * 1000.0

        if not commit_resp.get("ok"):
            return {
                **commit_resp,
                "commit_ms": commit_ms,
                "ds_wait_ms": float("inf"),
                "ds_durable_ms": float("inf"),
            }

        local_commit = commit_resp.get("local_commit")
        if not isinstance(local_commit, dict):
            return {
                **commit_resp,
                "commit_ms": commit_ms,
                "ds_wait_ms": float("inf"),
                "ds_durable_ms": float("inf"),
            }

        writes = local_commit.get("writes", [])
        start_vts = local_commit.get("start_vts", {})
        seq_no = int(local_commit.get("seq_no", 0))
        payload = {
            "type": MessageTypes.TX_PROPAGATE,
            "origin_site": self.site_id,
            "origin_seq_no": seq_no,
            "start_vts": dict(start_vts),
            "writes": list(writes),
        }
        wait_info = self._wait_disaster_safe_acks(payload=payload)
        ds_wait_ms = float(wait_info.get("disaster_safe_ms", float("inf")))
        ds_durable_ms = commit_ms + ds_wait_ms if ds_wait_ms != float("inf") else float("inf")

        return {
            **commit_resp,
            "commit_ms": commit_ms,
            "ds_wait_ms": ds_wait_ms,
            "ds_durable_ms": ds_durable_ms,
            "required_remote_acks": wait_info.get("required_remote_acks", 0),
            "received_remote_acks": wait_info.get("received_remote_acks", 0),
        }

    def _prepare_local_writes(self, txid: str, start_vts: Dict[int, int], writes: List[Dict[str, Any]]) -> tuple[bool, str]:
        if not writes:
            return True, ""

        for item in writes:
            oid = item["oid"]
            owner = self.key_locks.get(oid)
            if owner is not None and owner != txid:
                return False, f"locked_by={owner}"

        normalized_vts = {int(k): int(v) for k, v in start_vts.items()}
        start_snapshot = VectorTimestamp(clocks=normalized_vts)
        for item in writes:
            oid = item["oid"]
            if self.store.was_modified_since(oid=oid, start_vts=start_snapshot):
                return False, f"write_conflict_on={oid}"

        for item in writes:
            self.key_locks[item["oid"]] = txid

        self.prepared_writes[txid] = {
            "writes": list(writes),
            "start_vts": dict(normalized_vts),
        }
        return True, ""

    def _apply_prepared_commit(self, txid: str) -> Dict[str, Any]:
        prepared = self.prepared_writes.pop(txid, None)
        writes = [] if prepared is None else prepared.get("writes", [])
        start_vts = {} if prepared is None else prepared.get("start_vts", {})
        commit_version = self.clock.next_version()
        for item in writes:
            self.store.put(oid=item["oid"], value=item["value"], version=commit_version)
        self._release_locks(txid)
        return {
            "site_id": self.site_id,
            "seq_no": commit_version.seq_no,
            "write_count": len(writes),
            "writes": [{"op_type": "WRITE", **item} for item in writes],
            "start_vts": dict(start_vts),
        }

    def _abort_prepared(self, txid: str) -> None:
        self.prepared_writes.pop(txid, None)
        self._release_locks(txid)

    def _commit_fast(self, tx: Transaction) -> Dict[str, Any]:
        write_oids = {op.oid for op in tx.updates if op.op_type == "WRITE"}
        for oid in write_oids:
            owner = self.key_locks.get(oid)
            if owner is not None:
                tx.status = "ABORTED"
                return {
                    "ok": False,
                    "type": MessageTypes.TX_COMMIT_ABORT,
                    "tid": tx.tid,
                    "reason": f"locked_key={oid}",
                }
            if self.store.was_modified_since(oid=oid, start_vts=tx.start_vts):
                tx.status = "ABORTED"
                return {
                    "ok": False,
                    "type": MessageTypes.TX_COMMIT_ABORT,
                    "tid": tx.tid,
                    "reason": f"write_conflict_on={oid}",
                }

        tx.status = "COMMITTING"
        commit_version = self.clock.next_version()
        replicated_writes: List[Dict[str, Any]] = []
        for op in tx.updates:
            if op.op_type == "WRITE":
                self.store.put(oid=op.oid, value=op.payload, version=commit_version)
                replicated_writes.append({"op_type": "WRITE", "oid": op.oid, "value": op.payload})
            elif op.op_type == "CSET_ADD":
                element_id = str(op.payload["element_id"])
                self.cset_store.apply_add(oid=op.oid, element_id=element_id)
                replicated_writes.append({"op_type": "CSET_ADD", "oid": op.oid, "element_id": element_id})
            elif op.op_type == "CSET_DEL":
                element_id = str(op.payload["element_id"])
                self.cset_store.apply_del(oid=op.oid, element_id=element_id)
                replicated_writes.append({"op_type": "CSET_DEL", "oid": op.oid, "element_id": element_id})
        tx.commit_version = commit_version
        tx.status = "COMMITTED"

        self._spawn_propagation(
            origin_site=self.site_id,
            origin_seq_no=commit_version.seq_no,
            start_snapshot=tx.start_vts,
            writes=replicated_writes,
        )

        return {
            "ok": True,
            "type": MessageTypes.TX_COMMIT_OK,
            "tid": tx.tid,
            "commit_version": commit_version.to_dict(),
            "write_count": len(tx.updates),
            "commit_mode": "FAST",
        }

    def _commit_slow_2pc(self, tx: Transaction) -> Dict[str, Any]:
        txid = f"{self.site_id}:{tx.tid}"
        start_vts_dict = tx.start_vts.to_dict()
        writes_by_site: Dict[int, List[Dict[str, Any]]] = {}
        cset_updates: List[Dict[str, Any]] = []
        for op in tx.updates:
            if op.op_type == "WRITE":
                preferred = get_preferred_site(op.oid, default_site_id=self.site_id)
                writes_by_site.setdefault(preferred, []).append({"oid": op.oid, "value": op.payload})
            elif op.op_type == "CSET_ADD":
                cset_updates.append({"op_type": "CSET_ADD", "oid": op.oid, "element_id": str(op.payload["element_id"])})
            elif op.op_type == "CSET_DEL":
                cset_updates.append({"op_type": "CSET_DEL", "oid": op.oid, "element_id": str(op.payload["element_id"])})

        participant_sites = sorted(writes_by_site.keys())
        prepared_remote_sites: List[int] = []

        with self._state_lock:
            local_writes = writes_by_site.get(self.site_id, [])
            ok, reason = self._prepare_local_writes(txid=txid, start_vts=start_vts_dict, writes=local_writes)
            if not ok:
                tx.status = "ABORTED"
                return {
                    "ok": False,
                    "type": MessageTypes.TX_COMMIT_ABORT,
                    "tid": tx.tid,
                    "reason": reason,
                    "commit_mode": "SLOW_2PC",
                }

        for site_id in participant_sites:
            if site_id == self.site_id:
                continue
            payload = {
                "type": MessageTypes.TX_PREPARE,
                "txid": txid,
                "coordinator_site": self.site_id,
                "start_vts": start_vts_dict,
                "writes": writes_by_site.get(site_id, []),
            }
            try:
                resp = self.rpc_client.send_request(
                    to_site=site_id,
                    message=payload,
                    from_site=self.site_id,
                    apply_delay=True,
                )
            except Exception as exc:  # noqa: BLE001
                resp = {"ok": False, "error": f"prepare_rpc_error={exc}"}

            if not resp.get("ok"):
                for prepared_site in prepared_remote_sites:
                    try:
                        self.rpc_client.send_request(
                            to_site=prepared_site,
                            message={"type": MessageTypes.TX_REMOTE_ABORT, "txid": txid},
                            from_site=self.site_id,
                            apply_delay=True,
                        )
                    except Exception:
                        pass
                with self._state_lock:
                    self._abort_prepared(txid)
                    tx.status = "ABORTED"
                return {
                    "ok": False,
                    "type": MessageTypes.TX_COMMIT_ABORT,
                    "tid": tx.tid,
                    "reason": resp.get("reason", resp.get("error", "prepare_rejected")),
                    "commit_mode": "SLOW_2PC",
                }

            prepared_remote_sites.append(site_id)

        with self._state_lock:
            local_commit = self._apply_prepared_commit(txid)
            for item in cset_updates:
                if item["op_type"] == "CSET_ADD":
                    self.cset_store.apply_add(oid=item["oid"], element_id=item["element_id"])
                else:
                    self.cset_store.apply_del(oid=item["oid"], element_id=item["element_id"])
            tx.status = "COMMITTED"
            tx.commit_version = Version(site_id=self.site_id, seq_no=int(local_commit["seq_no"]))

            replicated_updates = list(local_commit["writes"]) + list(cset_updates)
            self._spawn_propagation(
                origin_site=self.site_id,
                origin_seq_no=int(local_commit["seq_no"]),
                start_snapshot=VectorTimestamp(clocks={int(k): int(v) for k, v in local_commit["start_vts"].items()}),
                writes=replicated_updates,
            )

        remote_commit_errors: List[str] = []
        for site_id in prepared_remote_sites:
            try:
                self.rpc_client.send_request(
                    to_site=site_id,
                    message={"type": MessageTypes.TX_REMOTE_COMMIT, "txid": txid},
                    from_site=self.site_id,
                    apply_delay=True,
                )
            except Exception as exc:  # noqa: BLE001
                remote_commit_errors.append(f"site={site_id}: {exc}")

        return {
            "ok": True,
            "type": MessageTypes.TX_COMMIT_OK,
            "tid": tx.tid,
            "write_count": len(tx.updates),
            "commit_mode": "SLOW_2PC",
            "participants": participant_sites,
            "local_commit": local_commit,
            "remote_commit_errors": remote_commit_errors,
        }

    def _handle_read_one_tx(self, oid: str) -> Dict[str, Any]:
        """Benchmark-only path: perform TX_START+TX_READ+TX_COMMIT in one RPC."""

        with self._state_lock:
            start_vts = self.clock.current_snapshot()
            visible_value = self.store.get_visible(oid=oid, start_vts=start_vts)
            if visible_value is not None:
                value = visible_value
                source = "TX_SNAPSHOT"
            else:
                value = self.store.get_latest(oid)
                source = "STORE_LATEST"

        return {
            "ok": True,
            "type": MessageTypes.TX_COMMIT_OK,
            "commit_mode": "FAST",
            "oid": oid,
            "value": value,
            "source": source,
        }

    def _handle_write_one_tx(self, oid: str, value: Any) -> Dict[str, Any]:
        """Benchmark-only path: perform TX_START+TX_WRITE+TX_COMMIT in one RPC."""

        preferred = get_preferred_site(oid, default_site_id=self.site_id)
        if preferred != self.site_id:
            return {
                "ok": False,
                "type": MessageTypes.TX_COMMIT_ABORT,
                "reason": f"one_rpc_write_nonlocal_preferred_site={preferred}",
            }

        with self._state_lock:
            tid = self._allocate_tid_locked()
            tx = Transaction(tid=tid, start_vts=self.clock.current_snapshot())
            tx.add_write(oid=oid, payload=value)
            return self._commit_fast(tx)

    def _handle_bench_regular_tx(
        self,
        local_oid: str,
        local_value: Any,
        remote_oid: str,
        remote_value: Any,
    ) -> Dict[str, Any]:
        """Benchmark-only path for regular case in one RPC (still executes real commit logic)."""

        with self._state_lock:
            tid = self._allocate_tid_locked()
            tx = Transaction(tid=tid, start_vts=self.clock.current_snapshot())
            tx.add_write(oid=local_oid, payload=local_value)
            tx.add_write(oid=remote_oid, payload=remote_value)

        write_oids = {op.oid for op in tx.updates if op.op_type == "WRITE"}
        touched_sites = {get_preferred_site(oid, default_site_id=self.site_id) for oid in write_oids}
        if touched_sites.issubset({self.site_id}):
            with self._state_lock:
                return self._commit_fast(tx)
        return self._commit_slow_2pc(tx)

    def _handle_bench_cset_tx(self, oid: str, add_element_id: str, del_element_id: str) -> Dict[str, Any]:
        """Benchmark-only path for cset case in one RPC."""

        with self._state_lock:
            tid = self._allocate_tid_locked()
            tx = Transaction(tid=tid, start_vts=self.clock.current_snapshot())
            tx.add_cset_add(oid=oid, element_id=add_element_id)
            tx.add_cset_del(oid=oid, element_id=del_element_id)
            return self._commit_fast(tx)

    def _handle_bench_fast_tx(self, mode: str, objects: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Benchmark-only path for one-RPC read/write tx with 1 or 5 objects."""

        object_count = len(objects)
        delay_ms = 0.0
        if mode == "read":
            delay_ms += self.read_base_ms
        elif mode == "write":
            delay_ms += self.write_base_ms

        if self.cache_miss_ratio > 0.0 and self.cache_miss_penalty_ms > 0.0:
            misses = sum(1 for _ in range(object_count) if random.random() < self.cache_miss_ratio)
            delay_ms += misses * self.cache_miss_penalty_ms

        if delay_ms > 0.0:
            for _ in range(object_count):
                time.sleep(delay_ms / 1000.0)

        if mode == "read":
            with self._state_lock:
                start_vts = self.clock.current_snapshot()
                results: List[Dict[str, Any]] = []
                for item in objects:
                    oid = str(item["oid"])
                    value = self.store.get_visible(oid=oid, start_vts=start_vts)
                    if value is None:
                        value = self.store.get_latest(oid)
                    results.append({"oid": oid, "value": value})

            return {
                "ok": True,
                "type": MessageTypes.TX_COMMIT_OK,
                "commit_mode": "FAST",
                "results": results,
            }

        if mode == "write":
            with self._state_lock:
                tid = self._allocate_tid_locked()
                tx = Transaction(tid=tid, start_vts=self.clock.current_snapshot())
                for item in objects:
                    oid = str(item["oid"])
                    payload = item.get("value")
                    tx.add_write(oid=oid, payload=payload)

                touched_sites = {get_preferred_site(op.oid, default_site_id=self.site_id) for op in tx.updates}
                if touched_sites.issubset({self.site_id}):
                    return self._commit_fast(tx)

            return self._commit_slow_2pc(tx)

        return {"ok": False, "error": f"unsupported_mode={mode}"}

    def _handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        msg_type = request.get("type")

        if msg_type == MessageTypes.TX_START:
            with self._state_lock:
                tid = self._allocate_tid_locked()
                tx = Transaction(tid=tid, start_vts=self.clock.current_snapshot())
                self.active_txs[tid] = tx
            return {
                "ok": True,
                "type": MessageTypes.TX_STARTED,
                "site_id": self.site_id,
                "tid": tid,
                "start_vts": tx.start_vts.to_dict(),
            }

        if msg_type == MessageTypes.TX_READ_ONE_TX:
            oid = request.get("oid")
            if not isinstance(oid, str):
                return {"ok": False, "error": "oid must be str"}
            return self._handle_read_one_tx(oid=oid)

        if msg_type == MessageTypes.TX_WRITE_ONE_TX:
            oid = request.get("oid")
            value = request.get("value")
            if not isinstance(oid, str):
                return {"ok": False, "error": "oid must be str"}
            return self._handle_write_one_tx(oid=oid, value=value)

        if msg_type == MessageTypes.TX_BENCH_REGULAR_TX:
            local_oid = request.get("local_oid")
            remote_oid = request.get("remote_oid")
            local_value = request.get("local_value")
            remote_value = request.get("remote_value")
            if not isinstance(local_oid, str):
                return {"ok": False, "error": "local_oid must be str"}
            if not isinstance(remote_oid, str):
                return {"ok": False, "error": "remote_oid must be str"}
            return self._handle_bench_regular_tx(
                local_oid=local_oid,
                local_value=local_value,
                remote_oid=remote_oid,
                remote_value=remote_value,
            )

        if msg_type == MessageTypes.TX_BENCH_CSET_TX:
            oid = request.get("oid")
            add_element_id = request.get("add_element_id")
            del_element_id = request.get("del_element_id")
            if not isinstance(oid, str):
                return {"ok": False, "error": "oid must be str"}
            if not isinstance(add_element_id, str):
                return {"ok": False, "error": "add_element_id must be str"}
            if not isinstance(del_element_id, str):
                return {"ok": False, "error": "del_element_id must be str"}
            return self._handle_bench_cset_tx(
                oid=oid,
                add_element_id=add_element_id,
                del_element_id=del_element_id,
            )

        if msg_type == MessageTypes.TX_BENCH_FAST_TX:
            mode = request.get("mode")
            objects = request.get("objects")
            if mode not in {"read", "write"}:
                return {"ok": False, "error": "mode must be read or write"}
            if not isinstance(objects, list):
                return {"ok": False, "error": "objects must be list"}
            if not objects:
                return {"ok": False, "error": "objects cannot be empty"}
            for obj in objects:
                if not isinstance(obj, dict):
                    return {"ok": False, "error": "object item must be dict"}
                if not isinstance(obj.get("oid"), str):
                    return {"ok": False, "error": "object oid must be str"}
                if mode == "write" and "value" not in obj:
                    return {"ok": False, "error": "write object must include value"}
            return self._handle_bench_fast_tx(mode=mode, objects=objects)

        if msg_type == MessageTypes.TX_BENCH_FAST_DURABILITY_TX:
            objects = request.get("objects")
            if not isinstance(objects, list):
                return {"ok": False, "error": "objects must be list"}
            if not objects:
                return {"ok": False, "error": "objects cannot be empty"}
            for obj in objects:
                if not isinstance(obj, dict):
                    return {"ok": False, "error": "object item must be dict"}
                if not isinstance(obj.get("oid"), str):
                    return {"ok": False, "error": "object oid must be str"}
                if "value" not in obj:
                    return {"ok": False, "error": "write object must include value"}
            return self._handle_bench_fast_durability_tx(objects=objects)

        if msg_type == MessageTypes.TX_BENCH_SLOW_DURABILITY_TX:
            objects = request.get("objects")
            if not isinstance(objects, list):
                return {"ok": False, "error": "objects must be list"}
            if not objects:
                return {"ok": False, "error": "objects cannot be empty"}
            for obj in objects:
                if not isinstance(obj, dict):
                    return {"ok": False, "error": "object item must be dict"}
                if not isinstance(obj.get("oid"), str):
                    return {"ok": False, "error": "object oid must be str"}
                if "value" not in obj:
                    return {"ok": False, "error": "write object must include value"}
            return self._handle_bench_slow_durability_tx(objects=objects)

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

        if msg_type in {MessageTypes.TX_CSET_ADD, MessageTypes.TX_CSET_DEL}:
            tid = request.get("tid")
            oid = request.get("oid")
            element_id = request.get("element_id")
            if not isinstance(tid, int):
                return {"ok": False, "error": "tid must be int"}
            if not isinstance(oid, str):
                return {"ok": False, "error": "oid must be str"}
            if not isinstance(element_id, str):
                return {"ok": False, "error": "element_id must be str"}

            with self._state_lock:
                tx = self.active_txs.get(tid)
                if tx is None:
                    return {"ok": False, "error": f"unknown tid={tid}"}
                if msg_type == MessageTypes.TX_CSET_ADD:
                    tx.add_cset_add(oid=oid, element_id=element_id)
                else:
                    tx.add_cset_del(oid=oid, element_id=element_id)

            return {
                "ok": True,
                "type": MessageTypes.TX_CSET_OP_OK,
                "tid": tid,
                "buffered_updates": len(tx.updates),
            }

        if msg_type == MessageTypes.TX_COMMIT_LOCAL:
            tid = request.get("tid")
            if not isinstance(tid, int):
                return {"ok": False, "error": "tid must be int"}

            with self._state_lock:
                tx = self.active_txs.pop(tid, None)
                if tx is None:
                    return {"ok": False, "error": f"unknown tid={tid}"}

            write_oids = {op.oid for op in tx.updates if op.op_type == "WRITE"}
            touched_sites = {get_preferred_site(oid, default_site_id=self.site_id) for oid in write_oids}
            if touched_sites.issubset({self.site_id}):
                with self._state_lock:
                    return self._commit_fast(tx)
            return self._commit_slow_2pc(tx)

        if msg_type == MessageTypes.TX_PREPARE:
            txid = request.get("txid")
            start_vts = request.get("start_vts")
            writes = request.get("writes")
            if not isinstance(txid, str):
                return {"ok": False, "type": MessageTypes.TX_PREPARE_NO, "reason": "txid must be str"}
            if not isinstance(start_vts, dict):
                return {"ok": False, "type": MessageTypes.TX_PREPARE_NO, "reason": "start_vts must be dict"}
            if not isinstance(writes, list):
                return {"ok": False, "type": MessageTypes.TX_PREPARE_NO, "reason": "writes must be list"}

            with self._state_lock:
                ok, reason = self._prepare_local_writes(txid=txid, start_vts=start_vts, writes=writes)
                if not ok:
                    return {"ok": False, "type": MessageTypes.TX_PREPARE_NO, "reason": reason}

            return {"ok": True, "type": MessageTypes.TX_PREPARE_OK, "txid": txid, "site_id": self.site_id}

        if msg_type == MessageTypes.TX_REMOTE_COMMIT:
            txid = request.get("txid")
            if not isinstance(txid, str):
                return {"ok": False, "error": "txid must be str"}
            with self._state_lock:
                info = self._apply_prepared_commit(txid)

                self._spawn_propagation(
                    origin_site=self.site_id,
                    origin_seq_no=int(info["seq_no"]),
                    start_snapshot=VectorTimestamp(clocks={int(k): int(v) for k, v in info["start_vts"].items()}),
                    writes=list(info["writes"]),
                )
            return {"ok": True, "type": MessageTypes.TX_COMMIT_OK, "txid": txid, "site_commit": info}

        if msg_type == MessageTypes.TX_REMOTE_ABORT:
            txid = request.get("txid")
            if not isinstance(txid, str):
                return {"ok": False, "error": "txid must be str"}
            with self._state_lock:
                self._abort_prepared(txid)
            return {"ok": True, "type": MessageTypes.TX_COMMIT_ABORT, "txid": txid}

        if msg_type == MessageTypes.TX_PROPAGATE:
            origin_site = request.get("origin_site")
            origin_seq_no = request.get("origin_seq_no")
            writes = request.get("writes")
            start_vts = request.get("start_vts")
            if not isinstance(origin_site, int):
                return {"ok": False, "error": "origin_site must be int"}
            if not isinstance(origin_seq_no, int):
                return {"ok": False, "error": "origin_seq_no must be int"}
            if not isinstance(writes, list):
                return {"ok": False, "error": "writes must be list"}
            if not isinstance(start_vts, dict):
                return {"ok": False, "error": "start_vts must be dict"}

            payload = {
                "origin_site": origin_site,
                "origin_seq_no": origin_seq_no,
                "writes": writes,
                "start_vts": start_vts,
            }

            with self._state_lock:
                if self._try_apply_propagation(payload):
                    self._drain_pending_propagations()
                    return {
                        "ok": True,
                        "type": MessageTypes.TX_PROPAGATE_APPLIED,
                        "origin_site": origin_site,
                        "origin_seq_no": origin_seq_no,
                    }

                self.pending_propagations.append(payload)
                return {
                    "ok": True,
                    "type": MessageTypes.TX_PROPAGATE_QUEUED,
                    "origin_site": origin_site,
                    "origin_seq_no": origin_seq_no,
                    "pending": len(self.pending_propagations),
                }

        if msg_type == MessageTypes.TX_PROPAGATE_BATCH:
            entries = request.get("entries")
            if not isinstance(entries, list):
                return {"ok": False, "error": "entries must be list"}

            results: List[Dict[str, Any]] = []
            with self._state_lock:
                for entry in entries:
                    if not isinstance(entry, dict):
                        results.append({"ok": False, "error": "entry must be dict"})
                        continue

                    origin_site = entry.get("origin_site")
                    origin_seq_no = entry.get("origin_seq_no")
                    writes = entry.get("writes")
                    start_vts = entry.get("start_vts")
                    if not isinstance(origin_site, int):
                        results.append({"ok": False, "error": "origin_site must be int"})
                        continue
                    if not isinstance(origin_seq_no, int):
                        results.append({"ok": False, "error": "origin_seq_no must be int"})
                        continue
                    if not isinstance(writes, list):
                        results.append({"ok": False, "error": "writes must be list"})
                        continue
                    if not isinstance(start_vts, dict):
                        results.append({"ok": False, "error": "start_vts must be dict"})
                        continue

                    payload = {
                        "origin_site": origin_site,
                        "origin_seq_no": origin_seq_no,
                        "writes": writes,
                        "start_vts": start_vts,
                    }

                    if self._try_apply_propagation(payload):
                        results.append(
                            {
                                "ok": True,
                                "type": MessageTypes.TX_PROPAGATE_APPLIED,
                                "origin_site": origin_site,
                                "origin_seq_no": origin_seq_no,
                            }
                        )
                    else:
                        self.pending_propagations.append(payload)
                        results.append(
                            {
                                "ok": True,
                                "type": MessageTypes.TX_PROPAGATE_QUEUED,
                                "origin_site": origin_site,
                                "origin_seq_no": origin_seq_no,
                                "pending": len(self.pending_propagations),
                            }
                        )

                self._drain_pending_propagations()

            return {
                "ok": True,
                "type": MessageTypes.TX_PROPAGATE_BATCH_ACK,
                "results": results,
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

        if msg_type == MessageTypes.TX_CSET_READ:
            tid = request.get("tid")
            oid = request.get("oid")
            if not isinstance(oid, str):
                return {"ok": False, "error": "oid must be str"}

            with self._state_lock:
                base_counts = self.cset_store.get_counts(oid)
                tx = self.active_txs.get(tid) if isinstance(tid, int) else None
                if tx is not None:
                    merged_counts = tx.apply_buffered_cset_to_counts(oid=oid, counts=base_counts)
                else:
                    merged_counts = base_counts
                members = self.cset_store.read_members_from_counts(merged_counts)

            return {
                "ok": True,
                "type": MessageTypes.TX_CSET_READ_RESULT,
                "oid": oid,
                "members": members,
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
            if target_site not in self.active_site_ids:
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
