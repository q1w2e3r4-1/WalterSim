# WalterSim Lab Scaffold

This folder contains a staged implementation scaffold for a simplified Walter-style
geo-replicated transactional KV system.

Current status:
- Minimal inter-process RPC loop is implemented in `walter_comm.py` and `launcher.py`.
- The remaining modules are intentionally scaffolded with TODO comments so progress
  can be tracked and resumed quickly.

Planned package layout:
- `core/`: shared data structures, clocks, local stores.
- `network/`: message protocol, RPC wrappers, transport-level helpers.
- `protocol/`: PSI read path, fast/slow commit, replication ordering.
- `site/`: per-site runtime assembly and process entry points.
- `experiments/`: workloads, metrics, and experiment runners.
- `scripts/`: convenience CLIs for local runs.
- `tests/`: unit/integration test placeholders.
