# WalterSim Test Plan (Scaffold)

This file tracks test coverage targets while modules are being implemented.

Unit-test targets:
- `core.types_store.VectorTimestamp`: compare/merge/copy semantics.
- `core.types_store.VersionedObjectStore`: visible-version selection.
- `protocol.tx_engine.ConflictDetector`: write-write conflict checks.
- `protocol.replication.VisibilityGuard`: causal and in-order checks.

Integration-test targets:
- 4-site health and ping mesh.
- Fast-commit local write path.
- Slow-commit 2PC success/abort paths.
- Replication causal ordering with delayed dependencies.

Experiment validation targets:
- Fast-commit latency trend.
- Slow-commit write-set/remote-site penalty trend.
- Cset conflict-free throughput trend.
