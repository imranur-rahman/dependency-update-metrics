"""The configuration values of the Host, read from environment variables.

``host/README.md`` lists the variables (``DM_HOST_DB``, ``DM_HOST_OUTPUT_DIR``,
``DM_HOST_ALLOWED_ORIGINS``, ``DM_HOST_WORKERS``, ``DM_HOST_JOB_TIMEOUT_SECONDS``,
``DM_HOST_WORKER_MEMORY_MB``) and their defaults.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from .worker import JobLimits


@dataclass(frozen=True)
class HostSettings:
    db_path: Path
    output_dir: Path
    allowed_origins: Tuple[str, ...]
    # The number of worker threads; each runs one Job at a time (spec #1: 6).
    workers: int = 6
    # The timeout and the RSS limit of each Job (spec #1: 15 minutes and 2 GB).
    job_limits: JobLimits = JobLimits()

    @classmethod
    def from_env(cls) -> "HostSettings":
        origins = os.environ.get("DM_HOST_ALLOWED_ORIGINS", "http://localhost:8000")
        return cls(
            db_path=Path(os.environ.get("DM_HOST_DB", "host-data/jobs.db")),
            output_dir=Path(os.environ.get("DM_HOST_OUTPUT_DIR", "host-data/tool")),
            allowed_origins=tuple(o.strip() for o in origins.split(",") if o.strip()),
            workers=int(os.environ.get("DM_HOST_WORKERS", "6")),
            job_limits=JobLimits(
                timeout_seconds=float(os.environ.get("DM_HOST_JOB_TIMEOUT_SECONDS", "900")),
                max_rss_mb=int(os.environ.get("DM_HOST_WORKER_MEMORY_MB", "2048")),
            ),
        )


def accept_all_tokens(token: str, remote_ip: Optional[str]) -> bool:
    """A Turnstile check that accepts every token. For local runs until ticket #10."""
    return True
