"""The worker: it takes Jobs from the queue and runs the analysis for each package.

Run one worker process with ``python -m dmhost.worker`` (see ``host/README.md``).
Tests call :meth:`Worker.run_once` in the test process.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, List, Optional

from .job_queue import JobQueue
from .models import Comparison

logger = logging.getLogger(__name__)

Analysis = Callable[[Comparison, str], Dict[str, Any]]


class Worker:
    """Runs the Jobs of one queue, one Job at a time."""

    def __init__(self, queue: JobQueue, analysis: Analysis) -> None:
        self.queue = queue
        self.analysis = analysis

    def run_once(self) -> bool:
        """Run the oldest waiting Job. Return ``False`` if no Job waits."""
        job = self.queue.claim_next()
        if job is None:
            return False
        try:
            results: List[Dict[str, Any]] = [
                self.analysis(job.comparison, package) for package in job.comparison.packages
            ]
        except Exception:
            logger.exception("Job %s failed", job.job_id)
            self.queue.fail(job.job_id, f"Something went wrong (ref: {job.job_id})")
        else:
            self.queue.finish(job.job_id, results)
        return True

    def run_forever(self, poll_seconds: float = 1.0, stop: Optional[Callable[[], bool]] = None):
        """Run Jobs until ``stop()`` returns true. Sleep ``poll_seconds`` when no Job waits."""
        while stop is None or not stop():
            if not self.run_once():
                time.sleep(poll_seconds)


def main() -> None:
    """Run one worker process over the queue and the Tool that ``HostSettings`` name."""
    from .settings import HostSettings
    from .tool_analysis import ToolAnalysis

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    settings = HostSettings.from_env()
    worker = Worker(JobQueue(settings.db_path), ToolAnalysis(settings.output_dir))
    logger.info("Worker started on %s", settings.db_path)
    worker.run_forever()


if __name__ == "__main__":
    main()
