"""The worker: it takes Jobs from the queue and runs the analysis for each package.

Run one worker process with ``python -m dmhost.worker`` (see ``host/README.md``).
Tests call :meth:`Worker.run_once` in the test process.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, List, Optional

from .job_queue import JobQueue
from .models import Comparison, Job

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
        # The packages run in sequence. A failure of one package stores its error and
        # does not stop the others (spec #1, "Job execution").
        results: List[Dict[str, Any]] = []
        failures = 0
        packages = job.comparison.packages
        for index, package in enumerate(packages, start=1):
            self.queue.set_progress(job.job_id, index, len(packages))
            try:
                results.append(self.analysis(job.comparison, package))
            except Exception:
                logger.exception("Job %s: package %s failed", job.job_id, package)
                failures += 1
                results.append(failed_result(job.comparison, package, _generic_message(job)))
        if failures == len(results):
            # No package gave a result, so the Job itself failed.
            self.queue.fail(job.job_id, _generic_message(job))
        else:
            self.queue.finish(job.job_id, results)
        return True

    def run_forever(self, poll_seconds: float = 1.0, stop: Optional[Callable[[], bool]] = None):
        """Run Jobs until ``stop()`` returns true. Sleep ``poll_seconds`` when no Job waits."""
        while stop is None or not stop():
            if not self.run_once():
                time.sleep(poll_seconds)


def _generic_message(job: Job) -> str:
    return f"Something went wrong (ref: {job.job_id})"


def failed_result(comparison: Comparison, package: str, message: str) -> Dict[str, Any]:
    """Return the result of one package whose analysis raised, in the shape of a Tool result.

    The shape is the one of ``AnalysisResult.to_dict()`` for an error, so that the Webapp
    reads a failed package the same way as a package that the Tool reports as an error.
    """
    window_summary = comparison.mode == "window_summary"
    return {
        "ecosystem": comparison.ecosystem,
        "package": package,
        "mode": comparison.mode,
        "start_date": comparison.start_date.isoformat(),
        "end_date": comparison.end_date.isoformat(),
        "weighting_type": comparison.weighting_type,
        "half_life": comparison.half_life,
        "data_source": "depsdev",
        "summary": (
            {
                "mttu": None,
                "mttr": None,
                "num_dependencies": 0,
                "status": "error",
                "error": message,
            }
            if window_summary
            else None
        ),
        "releases": [],
        "error": message,
    }


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
