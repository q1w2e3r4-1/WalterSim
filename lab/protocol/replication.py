"""Asynchronous replication and causal ordering guards.

Planned responsibilities:
- Background propagate sender.
- Remote-apply queue when dependencies are not yet satisfied.
- got_vts / causal dependency checks and ordered apply.
"""


class ReplicationEngine:
    """Sends committed transactions to peer sites asynchronously."""

    # TODO: Implement batch send and backpressure-friendly queueing.


class CausalApplyQueue:
    """Buffers remote updates until causal and per-site order checks pass."""

    # TODO: Implement enqueue/try_apply/drain workflow.


class VisibilityGuard:
    """Encapsulates VTS-based dependency and sequence checks."""

    # TODO: Implement causal_deps_satisfied and in_order_for_site checks.
