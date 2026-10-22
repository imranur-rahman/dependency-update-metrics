"""The analysis function that the workers use in production: it runs the Tool.

The Host always uses deps.dev data (``data_source="depsdev"``). It never calls the
native registries (spec #1).
"""

from __future__ import annotations

from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import pandas as pd

import dependency_metrics
from dependency_metrics.api import DEPSDEV, PER_RELEASE
from dependency_metrics.depsdev_client import DepsDevClient
from dependency_metrics.errors import NoReleaseInWindowError, PackageNotFoundError
from dependency_metrics.interfaces import PackageResolver
from dependency_metrics.resolvers import ResolverCache

from .errors import NoReleasesInWindow, PackageNotFound
from .models import Comparison

# Builds the resolver for one package: (comparison, package, start, end) -> resolver.
ResolverFactory = Callable[[Comparison, str, datetime, datetime], PackageResolver]


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
        resolver_factory: Optional. It builds the resolver for each package, in place of
            the Tool's deps.dev resolver. Tests use it to make one release fail.
    """

    def __init__(
        self,
        output_dir: Path | str,
        depsdev_client: Optional[DepsDevClient] = None,
        osv_df: Optional[pd.DataFrame] = None,
        resolver_factory: Optional[ResolverFactory] = None,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.depsdev_client = depsdev_client
        self.osv_df = osv_df
        self.resolver_factory = resolver_factory

    def _client(self) -> DepsDevClient:
        if self.depsdev_client is None:
            self.depsdev_client = DepsDevClient(
                cache=ResolverCache(cache_dir=self.output_dir / "cache")
            )
        return self.depsdev_client

    def __call__(self, comparison: Comparison, package: str) -> Dict[str, Any]:
        start_date = _utc_midnight(comparison.start_date)
        end_date = _utc_midnight(comparison.end_date)
        resolver = (
            self.resolver_factory(comparison, package, start_date, end_date)
            if self.resolver_factory is not None
            else None
        )
        try:
            result = dependency_metrics.analyze(
                ecosystem=comparison.ecosystem,
                package=package,
                start_date=start_date,
                end_date=end_date,
                mode=comparison.mode,
                weighting_type=comparison.weighting_type,
                half_life=comparison.half_life,
                data_source=DEPSDEV,
                output_dir=self.output_dir,
                resolver=resolver,
                depsdev_client=self._client(),
                osv_df=self.osv_df,
                raise_errors=True,
            ).to_dict()
        except Exception as error:
            raise _user_facing(error) from error
        if result["mode"] == PER_RELEASE and not result["releases"]:
            raise NoReleasesInWindow(f"No release of {package} in the window")
        return result


def _user_facing(error: Exception) -> Exception:
    """Map a typed Tool outcome to the Host error with its own user message.

    This is the one place that does it. Every other error stays as it is, so the worker
    shows "Something went wrong" and logs the full error.
    """
    if isinstance(error, PackageNotFoundError):
        return PackageNotFound(f"{error}: {error.__cause__ or ''}")
    if isinstance(error, NoReleaseInWindowError):
        return NoReleasesInWindow(str(error))
    return error


def _utc_midnight(day) -> datetime:
    return datetime.combine(day, time(0, 0), tzinfo=timezone.utc)
