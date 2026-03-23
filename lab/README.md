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
python WalterSim/lab/scripts/smoke/run_tx_smoke.py
```

Fast-commit conflict smoke test:

```bash
python WalterSim/lab/scripts/smoke/run_fast_commit_conflict_smoke.py
```

Slow-commit (2PC) smoke test:

```bash
python WalterSim/lab/scripts/smoke/run_slow_commit_smoke.py
```

Async replication smoke test:

```bash
python WalterSim/lab/scripts/smoke/run_replication_smoke.py
```

Out-of-order propagation smoke test:

```bash
python WalterSim/lab/scripts/smoke/run_out_of_order_propagation_smoke.py
```

Cset conflict-free smoke test:

```bash
python WalterSim/lab/scripts/smoke/run_cset_smoke.py
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
python WalterSim/lab/scripts/run_baseline_rw_benchmark.py --keys 10000 --tx 2000 --concurrency 1 --persistent --single-rpc
```
Results:

```
[read] ok=2000/2000 throughput=35194.47 tx/s avg=0.02ms p50=0.02ms p95=0.02ms p99=0.04ms modes={'FAST': 2000} persistent=True single_rpc=True
[write] ok=2000/2000 throughput=30983.71 tx/s avg=0.03ms p50=0.02ms p95=0.03ms p99=0.03ms modes={'FAST': 2000} persistent=True single_rpc=True
```

2. Fast Commit benchmark
```bash
python WalterSim/lab/scripts/run_fast_commit_experiment.py --tx-per-site 2000 --concurrency-per-site 1 --keys-per-site 10000 --read-base-ms 0.01 --write-base-ms 0.02 --cache-miss-ratio 0 --cache-miss-penalty-ms 0.40

python WalterSim/lab/scripts/plot_fast_commit_scalability.py \
  --input WalterSim/lab/experiments/results/csv/fast_commit_scalability.csv \
  --output-dir WalterSim/lab/experiments/results/png
```

3. Fast-commit latency CDF (single site):

```bash
python WalterSim/lab/scripts/run_fast_commit_latency_cdf.py \
  --tx-count 20000 \
  --warmup 1000 \
  --concurrency 1 \
  --write-objects-per-tx 5 \
  --keys 10000 \
  --read-base-ms 0.01 \
  --write-base-ms 0.02 \
  --cache-miss-ratio 0 \
  --cache-miss-penalty-ms 0.40
```

Results:

```text
WalterSim/lab/experiments/results/csv/fast_commit_latency_cdf_raw.csv
WalterSim/lab/experiments/results/csv/fast_commit_latency_cdf_points.csv
WalterSim/lab/experiments/results/png/fast_commit_latency_cdf.png
```