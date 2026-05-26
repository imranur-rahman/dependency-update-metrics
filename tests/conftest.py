"""Shared pytest fixtures for CLI integration tests."""

from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest


class _InProcessExecutor(ThreadPoolExecutor):
    """Thread-based replacement for ProcessPoolExecutor used in CLI tests.

    Runs worker tasks in threads within the same process so that
    unittest.mock patches applied before main() are visible to workers.
    Accepts ProcessPoolExecutor's keyword arguments; mp_context is ignored.
    """

    def __init__(self, max_workers=None, mp_context=None, initializer=None, initargs=()):
        super().__init__(max_workers=1, initializer=initializer, initargs=initargs)


@pytest.fixture
def in_process_executor():
    """Replace ProcessPoolExecutor with a thread executor for the duration of a test.

    This makes mocks applied in the test process visible inside worker
    functions that the CLI submits to the executor.
    """
    with patch("dependency_metrics.cli.ProcessPoolExecutor", _InProcessExecutor):
        yield
