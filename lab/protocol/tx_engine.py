"""Transaction execution and commit-path selection.

Planned responsibilities:
- start_tx/read/write API surface for a site.
- Fast commit decision and execution.
- Slow commit (2PC) coordination with participants.
- Conflict detection for regular objects and cset special-case logic.
"""

# TODO: Implement these classes with real logic.


class PSIReadEngine:
    """Snapshot read visibility checks against transaction start_vts."""


class ConflictDetector:
    """Conflict checks for regular object writes and lock state."""


class FastCommitEngine:
    """Fast path commit for local-preferred regular writes and cset-only writes."""


class SlowCommitCoordinator:
    """Two-phase commit coordinator for remote-preferred regular writes."""


class PrepareHandler:
    """Participant-side prepare vote and lock handling."""
