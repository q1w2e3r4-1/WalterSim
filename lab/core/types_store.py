"""Core types and local storage abstractions.

Planned responsibilities:
- VectorTimestamp, Version, Transaction, UpdateOp definitions.
- Versioned object store (MVCC history per key).
- Cset store for commutative add/del operations.
- Lock table and local sequence clock utilities.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class VectorTimestamp:
    """Tracks per-site logical progress for visibility and causality checks."""

    clocks: Dict[int, int] = field(default_factory=dict)

    def get(self, site_id: int) -> int:
        return self.clocks.get(site_id, 0)

    def set(self, site_id: int, value: int) -> None:
        self.clocks[site_id] = value

    def copy(self) -> "VectorTimestamp":
        return VectorTimestamp(clocks=dict(self.clocks))

    def merge_max(self, other: "VectorTimestamp") -> None:
        for site_id, value in other.clocks.items():
            self.clocks[site_id] = max(self.get(site_id), value)

    def to_dict(self) -> Dict[int, int]:
        return dict(self.clocks)


@dataclass
class Version:
    """Identifies one committed write version as (site_id, seq_no)."""

    site_id: int
    seq_no: int

    def to_dict(self) -> Dict[str, int]:
        return {"site_id": self.site_id, "seq_no": self.seq_no}


@dataclass
class UpdateOp:
    """Represents one buffered update in a transaction write set."""

    oid: str
    op_type: str
    payload: Any

    def to_dict(self) -> Dict[str, Any]:
        return {"oid": self.oid, "op_type": self.op_type, "payload": self.payload}


@dataclass
class Transaction:
    """Transaction envelope captured at start, used across commit paths."""

    tid: int
    start_vts: VectorTimestamp
    updates: List[UpdateOp] = field(default_factory=list)
    status: str = "ACTIVE"
    commit_version: Optional[Version] = None

    def add_write(self, oid: str, payload: Any) -> None:
        self.updates.append(UpdateOp(oid=oid, op_type="WRITE", payload=payload))

    def get_buffered_write(self, oid: str) -> Optional[Any]:
        for op in reversed(self.updates):
            if op.op_type == "WRITE" and op.oid == oid:
                return op.payload
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tid": self.tid,
            "start_vts": self.start_vts.to_dict(),
            "updates": [op.to_dict() for op in self.updates],
            "status": self.status,
            "commit_version": None if self.commit_version is None else self.commit_version.to_dict(),
        }


class VersionedObjectStore:
    """MVCC storage for regular objects.

    Future behavior:
    - Keep history per oid as list of (value, Version).
    - Return newest visible version for a given start_vts.
    - Support conflict checks against start_vts.
    """

    def __init__(self):
        self.history: Dict[str, List[tuple[Any, Version]]] = {}

    def put(self, oid: str, value: Any, version: Version) -> None:
        self.history.setdefault(oid, []).append((value, version))

    def get_latest(self, oid: str) -> Optional[Any]:
        versions = self.history.get(oid, [])
        if not versions:
            return None
        return versions[-1][0]

    def get_visible(self, oid: str, start_vts: VectorTimestamp) -> Optional[Any]:
        versions = self.history.get(oid, [])
        for value, version in reversed(versions):
            if version.seq_no <= start_vts.get(version.site_id):
                return value
        return None

    def was_modified_since(self, oid: str, start_vts: VectorTimestamp) -> bool:
        versions = self.history.get(oid, [])
        for _, version in versions:
            if version.seq_no > start_vts.get(version.site_id):
                return True
        return False


class CsetStore:
    """Commutative counting-set storage.

    Future behavior:
    - Track element counters per cset object.
    - Apply add/del updates without write-write conflicts.
    - Materialize visible members where count > 0.
    """

    # TODO: Implement apply_add/apply_del/read_members methods.


class LockTable:
    """In-memory lock table used by slow-commit prepare phase."""

    # TODO: Implement try_lock/release/release_all_for_tx methods.


class SiteClock:
    """Per-site monotonic sequence allocator and committed VTS helper."""

    def __init__(self, site_id: int):
        self.site_id = site_id
        self.curr_seq_no = 0
        self.committed_vts = VectorTimestamp()

    def next_version(self) -> Version:
        self.curr_seq_no += 1
        self.committed_vts.set(self.site_id, self.curr_seq_no)
        return Version(site_id=self.site_id, seq_no=self.curr_seq_no)

    def current_snapshot(self) -> VectorTimestamp:
        return self.committed_vts.copy()
