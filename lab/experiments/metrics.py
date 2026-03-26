"""Metric collection and export helpers.

Planned responsibilities:
- Commit latency measurements and CDF-ready buckets.
- Throughput counters (ops/sec, tx/sec).
- Replication lag measurement per source->target site.
- CSV export for plotting.
"""


class MetricsCollector:
    """Collects runtime metrics from workloads and cluster events."""

    # TODO: Implement record and export APIs.
