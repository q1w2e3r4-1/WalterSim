# WalterSim Lab Scaffold

This folder contains a staged implementation scaffold for a simplified Walter-style
geo-replicated transactional KV system.

Current status:
- Minimal inter-process RPC loop is implemented in `network/rpc.py`, `site/node.py`, and `site/cluster.py`.
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

## Run Demo

From workspace root:

```bash
python WalterSim/lab/scripts/run_cluster.py
```

Or inside `WalterSim/lab`:

```bash
python scripts/run_cluster.py
```

Minimal transaction smoke test:

```bash
python WalterSim/lab/scripts/run_tx_smoke.py
```

Fast-commit conflict smoke test:

```bash
python WalterSim/lab/scripts/run_fast_commit_conflict_smoke.py
```
