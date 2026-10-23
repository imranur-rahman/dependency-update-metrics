"""The worker: it takes Jobs from the queue and runs the analysis for each package.

``python -m dmhost.worker`` starts the workers of the Host (see ``host/README.md``).
Tests call :meth:`Worker.run_once` in the test process.

A worker with :class:`JobLimits` runs each Job in its own child process and watches it.
The worker stops the child process when the user cancels the Job, when the Job runs longer
than the timeout, or when the RSS of the child process goes over the memory limit. Each
Job gets a new child process, so the worker starts again with a clean process for the next
Job. A worker without limits runs the Job in its own thread; tests use this form, and it
stops a cancelled Job only between two packages.
"""

from __future__ import annotations

import logging
import multiprocessing
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import psutil
from dependency_metrics import api

from . import errors
from .job_queue import JobQueue
from .models import RUNNING, Comparison, Job

logger = logging.getLogger(__name__)

Analysis = Callable[[Comparison, str], Dict[str, Any]]


@dataclass(frozen=True)
class JobLimits:
    """The limits of one Job in an isolated worker. Tests give short values.

    Args:
        timeout_seconds: The longest time that one Job can run (spec #1: 15 minutes).
        max_rss_mb: The RSS limit of the child process in MB (spec #1: 2 GB). 0 means
            no limit. The check is the one of the Tool's ``--max-worker-memory-mb``:
            ``psutil`` RSS in MB, compared with the limit.
        poll_seconds: How often the worker reads the child process and the Job state.
    """

    timeout_seconds: float = 15 * 60
    max_rss_mb: int = 2048
    poll_seconds: float = 0.5


class Worker:
    """Runs the Jobs of one queue, one Job at a time.

    With ``limits``, each Job runs in a new child process (see the module docstring), so
    ``analysis`` must be picklable: a module-level function or an object such as
    :class:`~dmhost.tool_analysis.ToolAnalysis`.
    """

    def __init__(
        self, queue: JobQueue, analysis: Analysis, limits: Optional[JobLimits] = None
    ) -> None:
        self.queue = queue
        self.analysis = analysis
        self.limits = limits

    def run_once(self) -> bool:
        """Run the oldest waiting Job. Return ``False`` if no Job waits."""
        job = self.queue.claim_next()
        if job is None:
            return False
        if self.limits is None:
            run_job(self.queue, self.analysis, job)
        else:
            self._run_isolated(job, self.limits)
        return True

    def _run_isolated(self, job: Job, limits: JobLimits) -> None:
        # "spawn" on every OS: a forked child of a process with threads (the pool) can
        # hold a lock that no thread releases.
        context = multiprocessing.get_context("spawn")
        child = context.Process(
            target=_run_job_in_child,
            args=(self.queue, self.analysis, job),
            name=f"job-{job.job_id}",
            daemon=True,
        )
        started = time.monotonic()
        child.start()
        try:
            stop_reason = self._watch(job, child, started, limits)
        finally:
            if child.is_alive():
                _kill_process_tree(child.pid)
            child.join()
        if stop_reason is not None:
            logger.error("Job %s: stopped, %s", job.job_id, stop_reason)
            message = (
                errors.TOOK_TOO_LONG
                if stop_reason == "timeout"
                else errors.something_went_wrong(job.job_id)
            )
            self.queue.fail(job.job_id, message)
        elif self.queue.state(job.job_id) == RUNNING:
            # The child process ended without a result, for example because the kernel
            # stopped it.
            logger.error(
                "Job %s: the child process ended with exit code %s and no result",
                job.job_id,
                child.exitcode,
            )
            self.queue.fail(job.job_id, errors.something_went_wrong(job.job_id))

    def _watch(self, job: Job, child: Any, started: float, limits: JobLimits) -> Optional[str]:
        """Wait for the child process. Return why the worker must stop it, or ``None``
        when the child process ended or the Job no longer runs (the user cancelled it)."""
        while True:
            child.join(limits.poll_seconds)
            if not child.is_alive():
                return None
            if self.queue.state(job.job_id) != RUNNING:
                logger.info("Job %s: cancelled, the worker stops it", job.job_id)
                return None
            if time.monotonic() - started > limits.timeout_seconds:
                return "timeout"
            rss_mb = _tree_rss_mb(child.pid)
            if limits.max_rss_mb > 0 and rss_mb > limits.max_rss_mb:
                return f"RSS {rss_mb:.0f} MB exceeds the limit of {limits.max_rss_mb} MB"

    def run_forever(self, poll_seconds: float = 1.0, stop: Optional[Callable[[], bool]] = None):
        """Run Jobs until ``stop()`` returns true. Sleep ``poll_seconds`` when no Job waits."""
        while stop is None or not stop():
            if not self.run_once():
                time.sleep(poll_seconds)


class WorkerPool:
    """Runs ``workers`` workers over one queue, each in its own thread.

    Each worker takes one Job at a time, so the pool runs up to ``workers`` Jobs in
    parallel. With ``limits``, each Job runs in its own child process (see :class:`Worker`),
    so a Job that uses too much memory stops only its own child process.
    """

    def __init__(
        self,
        queue: JobQueue,
        analysis: Analysis,
        workers: int,
        limits: Optional[JobLimits] = None,
        poll_seconds: float = 1.0,
    ) -> None:
        if workers < 1:
            raise ValueError("A pool needs at least 1 worker")
        self.workers = [Worker(queue, analysis, limits) for _ in range(workers)]
        self.poll_seconds = poll_seconds
        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []

    def start(self) -> None:
        """Start the worker threads."""
        self._stop.clear()
        self._threads = [
            threading.Thread(target=self._run, args=(worker,), name=f"worker-{i}", daemon=True)
            for i, worker in enumerate(self.workers, start=1)
        ]
        for thread in self._threads:
            thread.start()

    def _run(self, worker: Worker) -> None:
        while not self._stop.is_set():
            try:
                ran = worker.run_once()
            except Exception:
                # A worker must survive a queue error (for example a locked database).
                logger.exception("A worker failed; it continues with the next Job")
                ran = False
            if not ran:
                self._stop.wait(self.poll_seconds)

    def stop(self, timeout: Optional[float] = None) -> None:
        """Stop the worker threads after their current Job."""
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout)

    def wait(self) -> None:
        """Block until :meth:`stop` is called (from a signal handler, for example)."""
        while not self._stop.wait(1.0):
            pass


def run_job(queue: JobQueue, analysis: Analysis, job: Job) -> None:
    """Run the packages of a claimed Job in sequence and store the result.

    A failure of one package stores its error and does not stop the others (spec #1,
    "Job execution"). Before each package, the function reads the state of the Job, and it
    stops when the Job no longer runs (the user cancelled it).
    """
    results: List[Dict[str, Any]] = []
    failure_messages: List[str] = []
    packages = job.comparison.packages
    for index, package in enumerate(packages, start=1):
        if queue.state(job.job_id) != RUNNING:
            logger.info("Job %s: stopped before package %d", job.job_id, index)
            return
        queue.set_progress(job.job_id, index, len(packages))
        try:
            results.append(user_facing_result(analysis(job.comparison, package), job))
        except Exception as error:
            # The log has the full error under the job ID. The page gets only the message.
            logger.exception("Job %s: package %s failed: %s", job.job_id, package, error)
            message = errors.message_for(error, job.job_id)
            failure_messages.append(message)
            results.append(failed_result(job.comparison, package, message))
    if len(failure_messages) == len(results):
        # No package gave a result, so the Job itself failed. When all packages failed
        # for one reason, the Job shows that reason.
        reasons = set(failure_messages)
        message = reasons.pop() if len(reasons) == 1 else errors.something_went_wrong(job.job_id)
        queue.fail(job.job_id, message)
    else:
        queue.finish(job.job_id, results)


def user_facing_result(result: Dict[str, Any], job: Job) -> Dict[str, Any]:
    """Return ``result`` with each Tool error replaced by a user message.

    The Tool reports a failure that it caught as ``error`` text (``AnalysisResult``). The
    text can hold internal details, so the Host logs it under the job ID and the page gets
    :func:`errors.something_went_wrong`. A failure with its own message raises in the
    analysis function (see :mod:`dmhost.errors`). The same holds for the error of each
    Per-release row (a release that failed), so the other releases keep their values.
    """
    message = errors.something_went_wrong(job.job_id)
    package = result.get("package")
    releases = result.get("releases") or []
    if any(release.get("error") for release in releases):
        releases = [_user_facing_release(release, package, job, message) for release in releases]
        result = {**result, "releases": releases}
    detail = result.get("error") or ""
    if not detail:
        return result
    logger.error("Job %s: package %s failed: %s", job.job_id, package, detail)
    summary = result.get("summary")
    if summary is not None:
        summary = {**summary, "error": message}
    return {**result, "error": message, "summary": summary}


def _user_facing_release(
    release: Dict[str, Any], package: Any, job: Job, message: str
) -> Dict[str, Any]:
    detail = release.get("error") or ""
    if not detail:
        return release
    logger.error(
        "Job %s: package %s release %s failed: %s",
        job.job_id,
        package,
        release.get("package_version"),
        detail,
    )
    return {**release, "error": message}


def _run_job_in_child(queue: JobQueue, analysis: Analysis, job: Job) -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    run_job(queue, analysis, job)


def _tree_rss_mb(pid: Optional[int]) -> float:
    """Return the RSS in MB of a process and its children.

    This is the RSS check of the Tool's ``--max-worker-memory-mb``
    (``psutil.Process().memory_info().rss / 1024 / 1024``), read from outside the process.
    """
    try:
        process = psutil.Process(pid)
        total = process.memory_info().rss
        for child in process.children(recursive=True):
            try:
                total += child.memory_info().rss
            except psutil.Error:
                pass
    except psutil.Error:
        return 0.0
    return total / 1024 / 1024


def _kill_process_tree(pid: Optional[int]) -> None:
    try:
        process = psutil.Process(pid)
        processes = process.children(recursive=True) + [process]
    except psutil.Error:
        return
    for each in processes:
        try:
            each.kill()
        except psutil.Error:
            pass


def failed_result(comparison: Comparison, package: str, message: str) -> Dict[str, Any]:
    """Return the result of one package whose analysis raised, in the shape of a Tool result.

    The Tool builds the shape (``dependency_metrics.api.failed_result``), so the Webapp
    reads a failed package the same way as a package that the Tool reports as an error.
    """
    return api.failed_result(
        ecosystem=comparison.ecosystem,
        package=package,
        mode=comparison.mode,
        start_date=comparison.start_date.isoformat(),
        end_date=comparison.end_date.isoformat(),
        weighting_type=comparison.weighting_type,
        half_life=comparison.half_life,
        data_source=api.DEPSDEV,
        message=message,
    ).to_dict()


def main() -> None:
    """Run the worker pool over the queue and the Tool that ``HostSettings`` name.

    Each Job runs in its own child process with the limits of ``HostSettings.job_limits``.
    SIGTERM (``docker stop``) and SIGINT stop the pool after the current Jobs.
    """
    import signal

    from .settings import HostSettings
    from .tool_analysis import ToolAnalysis

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    settings = HostSettings.from_env()
    pool = WorkerPool(
        JobQueue(settings.db_path),
        ToolAnalysis(settings.output_dir),
        workers=settings.workers,
        limits=settings.job_limits,
    )
    for signal_number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signal_number, lambda *_: pool.stop(timeout=0))
    logger.info(
        "%d workers started on %s (%s)", settings.workers, settings.db_path, settings.job_limits
    )
    pool.start()
    pool.wait()


if __name__ == "__main__":
    main()
