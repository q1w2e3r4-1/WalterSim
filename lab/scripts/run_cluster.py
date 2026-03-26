"""Convenience script to run the current communication-loop launcher.

This is a stable command target while implementation migrates from scaffold
modules to full protocol logic.
"""

from pathlib import Path
import sys

LAB_ROOT = Path(__file__).resolve().parent.parent
SITE_DIR = LAB_ROOT / "site"
if str(LAB_ROOT) not in sys.path:
    sys.path.insert(0, str(LAB_ROOT))
if str(SITE_DIR) not in sys.path:
    sys.path.insert(0, str(SITE_DIR))

from cluster import run_cluster_demo


def main() -> None:
    run_cluster_demo()


if __name__ == "__main__":
    main()
