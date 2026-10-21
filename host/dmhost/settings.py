"""The configuration values of the Host, read from environment variables.

``host/README.md`` lists the variables (``DM_HOST_DB``, ``DM_HOST_OUTPUT_DIR``,
``DM_HOST_ALLOWED_ORIGINS``) and their defaults.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple


@dataclass(frozen=True)
class HostSettings:
    db_path: Path
    output_dir: Path
    allowed_origins: Tuple[str, ...]

    @classmethod
    def from_env(cls) -> "HostSettings":
        origins = os.environ.get("DM_HOST_ALLOWED_ORIGINS", "http://localhost:8000")
        return cls(
            db_path=Path(os.environ.get("DM_HOST_DB", "host-data/jobs.db")),
            output_dir=Path(os.environ.get("DM_HOST_OUTPUT_DIR", "host-data/tool")),
            allowed_origins=tuple(o.strip() for o in origins.split(",") if o.strip()),
        )


def accept_all_tokens(token: str, remote_ip: Optional[str]) -> bool:
    """A Turnstile check that accepts every token. For local runs until ticket #10."""
    return True
