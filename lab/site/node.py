"""Per-site runtime assembly and handlers.

Planned responsibilities:
- Build one site runtime from config, stores, protocol engines, and RPC server.
- Register RPC handlers for tx APIs and replication messages.
- Expose process entry function for multiprocessing launcher.
"""


class WalterSiteRuntime:
    """Aggregates all components required by one site process."""

    # TODO: Wire together rpc, storage, tx engine, replication engine.


def run_site(site_id: int) -> None:
    """Process entry point for one site instance."""

    # TODO: Build runtime and block on RPC server loop.
    raise NotImplementedError("site runtime scaffold only")
