"""Cluster configuration and static lookup tables.

Planned responsibilities:
- Site IDs, names, and ports.
- Latency matrix (RTT / one-way simulation modes).
- Preferred-site mapping for regular objects.
- Experiment presets and reusable constants.
"""

# TODO: Move static constants from `walter_comm.py` into this module.
# TODO: Add helper functions:
#   - get_site_address(site_id)
#   - get_link_delay_seconds(src, dst, mode="rtt"|"one_way")
#   - get_preferred_site(oid)
