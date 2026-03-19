"""Cluster configuration and static lookup tables.

This module centralizes topology-related constants so transport and runtime
layers can share one source of truth.
"""

import os
from dataclasses import dataclass

SITE_IDS = [0, 1, 2, 3]
SITE_NAMES = {0: "VA", 1: "CA", 2: "IE", 3: "SG"}
SITE_PORTS = {0: 5001, 1: 5002, 2: 5003, 3: 5004}

# Optional explicit preferred-site mapping for named objects.
PREFERRED_SITES = {
	"user_va_profile": 0,
	"user_sg_profile": 3,
	"global_timeline": 0,
}

# RTT values from the paper experiment setup (seconds).
LATENCY_MATRIX_RTT = {
	0: [0.000, 0.082, 0.087, 0.261],
	1: [0.082, 0.000, 0.153, 0.190],
	2: [0.087, 0.153, 0.000, 0.277],
	3: [0.261, 0.190, 0.277, 0.000],
}


def get_active_site_ids() -> list[int]:
	"""Resolve active sites for one experiment run.

	By default, all configured sites are active. For scalability experiments,
	set WALTER_ACTIVE_SITE_IDS as comma-separated integers, e.g. "0,1,2".
	"""

	raw = os.environ.get("WALTER_ACTIVE_SITE_IDS", "").strip()
	if not raw:
		return list(SITE_IDS)

	active: list[int] = []
	for token in raw.split(","):
		item = token.strip()
		if not item:
			continue
		if not item.isdigit():
			continue
		site_id = int(item)
		if site_id in SITE_IDS and site_id not in active:
			active.append(site_id)

	return active if active else list(SITE_IDS)


@dataclass
class SiteAddress:
	site_id: int
	host: str
	port: int


def get_site_address(site_id: int) -> SiteAddress:
	return SiteAddress(site_id=site_id, host="127.0.0.1", port=SITE_PORTS[site_id])


def get_link_delay_seconds(from_site: int, to_site: int) -> float:
	return LATENCY_MATRIX_RTT[from_site][to_site]


def get_preferred_site(oid: str, default_site_id: int = 0) -> int:
	"""Return preferred site for an object id.

	Rules:
	- If oid is listed in `PREFERRED_SITES`, use that mapping.
	- If oid starts with `ps<site_id>:` (for smoke tests), use that site id.
	- Otherwise, fallback to `default_site_id`.
	"""

	if oid in PREFERRED_SITES:
		return PREFERRED_SITES[oid]

	if oid.startswith("ps") and ":" in oid:
		prefix = oid.split(":", 1)[0]
		candidate = prefix[2:]
		if candidate.isdigit():
			site_id = int(candidate)
			if site_id in SITE_IDS:
				return site_id

	return default_site_id

