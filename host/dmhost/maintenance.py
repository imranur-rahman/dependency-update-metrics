"""The weekly maintenance of the Host (ticket #16).

A timer on the VM runs ``python -m dmhost.maintenance`` every 7 days in a worker container
(``host/weekly-maintenance.sh``). It runs two Tool commands, in this order:

1. ``--build-osv`` rebuilds the OSV index from the three ecosystem zips. The Tool writes the
   new index to a temporary file and renames it, so a failed rebuild keeps the old index.
2. ``--cache-prune-gb 10`` prunes the ResolverCache to 10 GB. It runs also after a failed
   rebuild, because the cache size does not depend on the index.

The command exits with 1 when a step fails, so the timer reports the failure.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Sequence, Tuple

logger = logging.getLogger("dmhost.maintenance")

CACHE_PRUNE_GB = 10
TOOL = "dependency-metrics"

Runner = Callable[[Sequence[str]], int]


@dataclass(frozen=True)
class MaintenanceReport:
    # (step name, exit code) for each step, in the order of the run.
    steps: Tuple[Tuple[str, int], ...]

    @property
    def ok(self) -> bool:
        return all(code == 0 for _, code in self.steps)

    @property
    def failed_steps(self) -> List[str]:
        return [name for name, code in self.steps if code != 0]


def _run_subprocess(argv: Sequence[str]) -> int:
    return subprocess.run(list(argv), check=False).returncode


def run_weekly_maintenance(output_dir: Path, run: Runner = _run_subprocess) -> MaintenanceReport:
    """Rebuild the OSV index, then prune the ResolverCache. Return the exit code of each step."""
    out = str(output_dir)
    steps = [
        ("osv rebuild", [TOOL, "--build-osv", "--output-dir", out]),
        ("cache prune", [TOOL, "--cache-prune-gb", str(CACHE_PRUNE_GB), "--output-dir", out]),
    ]
    results = []
    for name, argv in steps:
        code = run(argv)
        if code != 0:
            logger.error("Maintenance step %r failed with exit code %d", name, code)
        results.append((name, code))
    return MaintenanceReport(steps=tuple(results))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    output_dir = Path(os.environ.get("DM_HOST_OUTPUT_DIR", "host-data/tool"))
    report = run_weekly_maintenance(output_dir)
    if report.ok:
        logger.info("Weekly maintenance done")
        sys.exit(0)
    logger.error("Weekly maintenance failed: %s", ", ".join(report.failed_steps))
    sys.exit(1)


if __name__ == "__main__":
    main()
