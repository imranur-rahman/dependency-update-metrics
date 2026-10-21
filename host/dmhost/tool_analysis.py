"""The analysis function that the workers use in production: it runs the Tool.

The Host always uses deps.dev data (``data_source="depsdev"``). It never calls the
native registries (spec #1).
"""

from __future__ import annotations

import re
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import requests

import dependency_metrics
from dependency_metrics.api import DEPSDEV, PER_RELEASE
from dependency_metrics.depsdev_client import DepsDevClient
from dependency_metrics.resolvers import ResolverCache
from dependency_metrics.time_utils import parse_timestamp

from .errors import NoReleasesInWindow, PackageNotFound
from .models import Comparison

_DEPSDEV_SYSTEMS = {"npm": "NPM", "pypi": "PYPI", "cargo": "CARGO"}
# The error of a Window summary for a package with no version on or before the end date
# (``DepsDevResolver.get_package_version_at_date``).
_NO_VERSION_ERROR = re.compile(r"No version of .+ found on or before ")


class ToolAnalysis:
    """Analyze one package of a Comparison with the Tool and return plain data.

    It raises :class:`PackageNotFound` when deps.dev does not know the package, and
    :class:`NoReleasesInWindow` when the package has no release that the window can use.

    Args:
        output_dir: The Tool's output directory. It holds ``cache/`` (the ResolverCache
            that all workers share) and ``osv_database.parquet``.
        depsdev_client: Optional deps.dev client. Tests give a stub.
        osv_df: Optional OSV records. If omitted, the Tool reads the OSV index in
            ``output_dir``.
    """

    def __init__(
        self,
        output_dir: Path | str,
        depsdev_client: Optional[DepsDevClient] = None,
        osv_df: Optional[pd.DataFrame] = None,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.depsdev_client = depsdev_client
        self.osv_df = osv_df

    def _client(self) -> DepsDevClient:
        if self.depsdev_client is None:
            self.depsdev_client = DepsDevClient(
                cache=ResolverCache(cache_dir=self.output_dir / "cache")
            )
        return self.depsdev_client

    def __call__(self, comparison: Comparison, package: str) -> Dict[str, Any]:
        result = dependency_metrics.analyze(
            ecosystem=comparison.ecosystem,
            package=package,
            start_date=_utc_midnight(comparison.start_date),
            end_date=_utc_midnight(comparison.end_date),
            mode=comparison.mode,
            weighting_type=comparison.weighting_type,
            half_life=comparison.half_life,
            data_source=DEPSDEV,
            output_dir=self.output_dir,
            depsdev_client=self._client(),
            osv_df=self.osv_df,
        ).to_dict()
        if _has_no_release(result):
            self._raise_why_no_release(comparison, package, result)
        return result

    def _raise_why_no_release(
        self, comparison: Comparison, package: str, result: Dict[str, Any]
    ) -> None:
        """Find out why the Tool found no release of ``package``, and raise it.

        ``DepsDevResolver`` turns every failed GetPackage request into an empty version
        list, so the result alone cannot tell an unknown package from a window without a
        release. This asks deps.dev for the package again.
        """
        detail = result["error"] or f"No release of {package} in the window"
        system = _DEPSDEV_SYSTEMS[comparison.ecosystem]
        end = _utc_midnight(comparison.end_date)
        try:
            listing = self._client().get_package(system, package, end_date=end)
        except requests.HTTPError as error:
            if error.response is not None and error.response.status_code == 404:
                raise PackageNotFound(f"{detail}: {error}") from error
            raise
        start = _utc_midnight(comparison.start_date)
        if comparison.mode != PER_RELEASE:
            start = datetime.min.replace(tzinfo=timezone.utc)
        for entry in listing.get("versions", []) or []:
            published = parse_timestamp(entry.get("publishedAt", ""))
            if published is not None and start <= published <= end:
                # deps.dev lists a release that the analysis did not get, for example
                # after a failed request. This is not a window without a release.
                raise RuntimeError(f"{detail}, but deps.dev lists {entry.get('versionKey')}")
        raise NoReleasesInWindow(detail)


def _has_no_release(result: Dict[str, Any]) -> bool:
    """Return true if the Tool result says that the package has no usable release.

    A Per-release result has no release row and no error. A Window summary result has the
    error of ``DepsDevResolver.get_package_version_at_date``.
    """
    if result["mode"] == PER_RELEASE:
        return not result["releases"] and not result["error"]
    return bool(_NO_VERSION_ERROR.match(result["error"] or ""))


def _utc_midnight(day) -> datetime:
    return datetime.combine(day, time(0, 0), tzinfo=timezone.utc)
