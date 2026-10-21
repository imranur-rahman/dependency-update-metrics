"""The configuration values of the Host, read from environment variables.

``host/README.md`` lists the variables (``DM_HOST_*``) and their defaults.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional, Tuple

from .client_ip import Network, parse_networks
from .limits import SubmitLimits


@dataclass(frozen=True)
class HostSettings:
    db_path: Path
    output_dir: Path
    allowed_origins: Tuple[str, ...]
    limits: SubmitLimits = SubmitLimits()
    # The secret key of the Turnstile widget. It is only on the instance, never in the repo.
    turnstile_secret: str = ""
    # The key of the IP hash. Without it, the API makes a random key at each start.
    ip_hash_key: Optional[bytes] = None
    trusted_proxies: Tuple[Network, ...] = ()

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "HostSettings":
        env = os.environ if env is None else env
        defaults = SubmitLimits()
        limits = SubmitLimits(
            max_active_jobs_per_ip=int(
                env.get("DM_HOST_MAX_ACTIVE_JOBS_PER_IP", defaults.max_active_jobs_per_ip)
            ),
            max_jobs_per_ip_per_window=int(
                env.get("DM_HOST_MAX_JOBS_PER_IP_PER_HOUR", defaults.max_jobs_per_ip_per_window)
            ),
            max_waiting_jobs=int(env.get("DM_HOST_MAX_WAITING_JOBS", defaults.max_waiting_jobs)),
        )
        hash_key = env.get("DM_HOST_IP_HASH_KEY", "")
        return cls(
            db_path=Path(env.get("DM_HOST_DB", "host-data/jobs.db")),
            output_dir=Path(env.get("DM_HOST_OUTPUT_DIR", "host-data/tool")),
            allowed_origins=_split(env.get("DM_HOST_ALLOWED_ORIGINS", "http://localhost:8000")),
            limits=limits,
            turnstile_secret=env.get("DM_HOST_TURNSTILE_SECRET", ""),
            ip_hash_key=hash_key.encode("utf-8") if hash_key else None,
            trusted_proxies=tuple(parse_networks(_split(env.get("DM_HOST_TRUSTED_PROXIES", "")))),
        )


def _split(value: str) -> Tuple[str, ...]:
    return tuple(part.strip() for part in value.split(",") if part.strip())
