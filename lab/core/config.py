"""Cluster configuration and static lookup tables.

This module centralizes topology-related constants so transport and runtime
layers can share one source of truth.
"""

from dataclasses import dataclass

SITE_IDS = [0, 1, 2, 3]
SITE_NAMES = {0: "VA", 1: "CA", 2: "IE", 3: "SG"}
SITE_PORTS = {0: 5001, 1: 5002, 2: 5003, 3: 5004}

# RTT values from the paper experiment setup (seconds).
LATENCY_MATRIX_RTT = {
	0: [0.000, 0.082, 0.087, 0.261],
	1: [0.082, 0.000, 0.153, 0.190],
	2: [0.087, 0.153, 0.000, 0.277],
	3: [0.261, 0.190, 0.277, 0.000],
}


@dataclass
class SiteAddress:
	site_id: int
	host: str
	port: int


def get_site_address(site_id: int) -> SiteAddress:
	return SiteAddress(site_id=site_id, host="127.0.0.1", port=SITE_PORTS[site_id])


def get_link_delay_seconds(from_site: int, to_site: int) -> float:
	return LATENCY_MATRIX_RTT[from_site][to_site]

