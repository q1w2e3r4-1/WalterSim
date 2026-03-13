"""Core types and local storage abstractions.

Planned responsibilities:
- VectorTimestamp, Version, Transaction, UpdateOp definitions.
- Versioned object store (MVCC history per key).
- Cset store for commutative add/del operations.
- Lock table and local sequence clock utilities.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# TODO: Implement full vector timestamp helper methods.
@dataclass
class VectorTimestamp:
    """Tracks per-site logical progress for visibility and causality checks."""

    clocks: Dict[int, int] = field(default_factory=dict)


# TODO: Add ordering/serialization helpers.
@dataclass
class Version:
    """Identifies one committed write version as (site_id, seq_no)."""

    site_id: int
    seq_no: int


# TODO: Expand op typing for regular write and cset add/del updates.
@dataclass
class UpdateOp:
    """Represents one buffered update in a transaction write set."""

    oid: str
    op_type: str
    payload: Any


# TODO: Add status lifecycle and commit metadata fields.
@dataclass
class Transaction:
    """Transaction envelope captured at start, used across commit paths."""

    tid: int
    start_vts: VectorTimestamp
    updates: List[UpdateOp] = field(default_factory=list)
    status: str = "ACTIVE"


class VersionedObjectStore:
    """MVCC storage for regular objects.

    Future behavior:
    - Keep history per oid as list of (value, Version).
    - Return newest visible version for a given start_vts.
    - Support conflict checks against start_vts.
    """

    # TODO: Implement put/get_visible/was_modified_since methods.


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

    # TODO: Implement next_seq/update_committed_vts helpers.
