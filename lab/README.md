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

Slow-commit (2PC) smoke test:

```bash
python WalterSim/lab/scripts/run_slow_commit_smoke.py
```

Async replication smoke test:

```bash
python WalterSim/lab/scripts/run_replication_smoke.py
```

Out-of-order propagation smoke test:

```bash
python WalterSim/lab/scripts/run_out_of_order_propagation_smoke.py
```

Cset conflict-free smoke test:

```bash
python WalterSim/lab/scripts/run_cset_smoke.py
```

Cset vs Regular small benchmark:

```bash
python WalterSim/lab/scripts/run_cset_vs_regular_benchmark.py --iters 50
```
Results:

```
[regular] ok=50/50 throughput=6.02 tx/s avg=166.23ms p50=166.12ms p95=166.53ms modes=['SLOW_2PC']
[cset] ok=50/50 throughput=8527.17 tx/s avg=0.11ms p50=0.05ms p95=0.27ms modes=['FAST']
```

## Run experiments:
1. Baseline read/write benchmark:

```bash
python WalterSim/lab/scripts/run_baseline_rw_benchmark.py --keys 50000 --tx 5000 --concurrency 8 --persistent --single-rpc
```
Results:

```
[read] ok=5000/5000 throughput=39839.90 tx/s avg=0.18ms p50=0.14ms p95=0.35ms p99=0.61ms modes={'FAST': 5000} persistent=True single_rpc=True
[write] ok=5000/5000 throughput=39250.43 tx/s avg=0.19ms p50=0.15ms p95=0.38ms p99=0.57ms modes={'FAST': 5000} persistent=True single_rpc=True
```