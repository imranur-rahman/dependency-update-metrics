"""The six user messages of the Host, and the mapping from a failure to one of them.

The page shows only these messages, with no stack traces (spec #1, "Error mapping"). The
Host logs the full error under the job ID.

- :data:`PACKAGE_NOT_FOUND` and :data:`NO_RELEASES`: a package fails. ``ToolAnalysis``
  raises :class:`PackageNotFound` or :class:`NoReleasesInWindow`.
- :func:`too_many_requests` and :data:`SERVER_BUSY`: the API refuses a submit (the rate
  limits and the queue cap). Give the message as the ``detail`` of the HTTP error.
- :data:`TOOK_TOO_LONG`: a Job reaches the timeout. Raise :class:`AnalysisTimedOut` in
  the analysis of a package, or fail the Job with this message.
- :func:`something_went_wrong`: every other failure.

A new failure type with its own message raises a subclass of :class:`UserFacingError`.
:func:`message_for` maps it, and every other exception, to the user message.
"""

from __future__ import annotations

PACKAGE_NOT_FOUND = "Package not found in deps.dev"
NO_RELEASES = "No releases in this window"
SERVER_BUSY = "The server is busy, try later"
TOOK_TOO_LONG = "The analysis took too long"


def too_many_requests(minutes: int) -> str:
    """Return the message for a user who reached a rate limit.

    ``minutes`` is the time until the Host accepts the next Job from the user. The message
    shows at least 1 minute, because "0 min" tells the user nothing.
    """
    return f"Too many requests, try again in {max(1, minutes)} min"


def something_went_wrong(job_id: str) -> str:
    """Return the message for a failure that has no message of its own."""
    return f"Something went wrong (ref: {job_id})"


class UserFacingError(Exception):
    """A failure that has its own user message.

    ``str(error)`` is the full error for the log. ``user_message`` is what the page shows.
    """

    user_message = ""

    def __init__(self, detail: str = "") -> None:
        super().__init__(detail or self.user_message)


class PackageNotFound(UserFacingError):
    """deps.dev does not know the package."""

    user_message = PACKAGE_NOT_FOUND


class NoReleasesInWindow(UserFacingError):
    """The package has no release that the window can use."""

    user_message = NO_RELEASES


class AnalysisTimedOut(UserFacingError):
    """The analysis reached the job timeout."""

    user_message = TOOK_TOO_LONG


def message_for(error: BaseException, job_id: str) -> str:
    """Return the user message for ``error``, a failure in the Job ``job_id``."""
    if isinstance(error, UserFacingError):
        return error.user_message
    return something_went_wrong(job_id)
