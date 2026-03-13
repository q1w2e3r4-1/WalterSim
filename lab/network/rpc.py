"""RPC/message transport abstractions.

Planned responsibilities:
- Message codec and schema guards.
- Client request helper with latency injection.
- Server listener and message dispatch integration.
- Retry/timeout strategy hooks for later experiments.
"""

# TODO: Gradually migrate transport logic from `walter_comm.py` into this module.
# TODO: Define message envelope structure and validators.


class MessageTypes:
    """Enum-like container for RPC message names used across modules."""

    # TODO: Add all protocol message names (PING, PREPARE, COMMIT, PROPAGATE, ...).


class RpcClient:
    """Client-side request helper for local process and inter-site RPC calls."""

    # TODO: Add typed wrappers for health/ping/prepare/commit/propagate calls.


class RpcServer:
    """Socket listener and request dispatcher for one site runtime."""

    # TODO: Add lifecycle management and handler registration APIs.
