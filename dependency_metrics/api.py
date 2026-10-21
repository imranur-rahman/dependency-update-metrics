"""
Public library interface: run one package analysis and get plain-data results.

The CLI (single ``--package`` runs) and the Host both call :func:`analyze`, so
the two share one code path.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from .analyzer import DependencyAnalyzer
from .depsdev_client import DepsDevClient
from .depsdev_resolver import DepsDevResolver, depsdev_system
from .interfaces import PackageResolver
from .models import ProgressEvent
from .resolvers import ResolverCache

logger = logging.getLogger(__name__)

WINDOW_SUMMARY = "window_summary"
PER_RELEASE = "per_release"
MODES = (WINDOW_SUMMARY, PER_RELEASE)
NATIVE = "native"
DEPSDEV = "depsdev"
DATA_SOURCES = (NATIVE, DEPSDEV)


@dataclass(frozen=True)
class WindowSummary:
    """One MTTU and one MTTR for the package over the whole time window.

    ``status`` is ``"ok"``, ``"no_deps"``, or ``"error"``. On an error, ``mttu`` and
    ``mttr`` are ``None`` and ``error`` holds the message.
    """

    mttu: Optional[float]
    mttr: Optional[float]
    num_dependencies: int
    status: str
    error: str = ""


@dataclass(frozen=True)
class ReleaseResult:
    """MTTU and MTTR at one release, over the window from the start date to the release.

    ``status`` is ``"ok"``, ``"no_deps"``, or ``"error"``. On an error, ``mttu`` and
    ``mttr`` are ``None`` and ``error`` holds the message; the other releases keep
    their values.
    """

    package_version: str
    package_release_date: str
    mttu: Optional[float]
    mttr: Optional[float]
    num_dependencies: int
    status: str
    error: str = ""


@dataclass
class AnalysisResult:
    """The structured result of one package analysis."""

    ecosystem: str
    package: str
    mode: str
    start_date: str
    end_date: str
    weighting_type: str
    half_life: Optional[float]
    data_source: str
    summary: Optional[WindowSummary] = None
    releases: List[ReleaseResult] = field(default_factory=list)
    # The message of an error that stopped the analysis of the package; "" if none.
    error: str = ""
    # The raw analyzer output, for the CLI exports. Not part of the plain-data contract.
    details: Any = field(default=None, repr=False, compare=False)

    def to_dict(self) -> Dict[str, Any]:
        """Return the result as JSON-ready plain data (without ``details``)."""
        return {
            "ecosystem": self.ecosystem,
            "package": self.package,
            "mode": self.mode,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "weighting_type": self.weighting_type,
            "half_life": self.half_life,
            "data_source": self.data_source,
            "summary": asdict(self.summary) if self.summary is not None else None,
            "releases": [asdict(r) for r in self.releases],
            "error": self.error,
        }


def analyze(
    ecosystem: str,
    package: str,
    start_date: datetime,
    end_date: datetime,
    mode: str = WINDOW_SUMMARY,
    weighting_type: str = "disable",
    half_life: Optional[float] = None,
    data_source: str = NATIVE,
    output_dir: Path = Path("./output"),
    resolver: Optional[PackageResolver] = None,
    depsdev_client: Optional[DepsDevClient] = None,
    osv_df: Optional[pd.DataFrame] = None,
    dependency_frames: bool = False,
    raise_errors: bool = False,
    progress: Optional[Callable[[ProgressEvent], None]] = None,
) -> AnalysisResult:
    """Analyze one package and return structured results.

    Args:
        mode: ``"window_summary"`` (one result for the window) or ``"per_release"``
            (one result for each release in the window).
        data_source: ``"native"`` (the registries of the ecosystem) or ``"depsdev"``
            (the deps.dev API only).
        output_dir: The directory that holds ``cache/`` and ``osv_database.parquet``.
        resolver: Optional pre-built resolver. It replaces the one that
            ``data_source`` selects.
        depsdev_client: Optional deps.dev client for ``data_source="depsdev"``. If
            omitted, a client over ``<output_dir>/cache`` is used.
        osv_df: Optional OSV records. If omitted, ``<output_dir>/osv_database.parquet``
            is read; if that file is missing, every MTTR is 0.
        dependency_frames: Per-release only. Keep the per-interval dependency frames
            in ``details`` (the CLI needs them for ``--get-worksheets``).
        raise_errors: If true, an error in the analysis propagates. If false (the
            default), the result carries the error message and an ``"error"`` status.
        progress: Optional callback. It receives a :class:`ProgressEvent` after each
            direct dependency (Window summary) or each release (Per-release).
    """
    if mode not in MODES:
        raise ValueError(f"Unsupported mode: {mode}. Use one of {', '.join(MODES)}.")
    if data_source not in DATA_SOURCES:
        raise ValueError(
            f"Unsupported data source: {data_source}. Use one of {', '.join(DATA_SOURCES)}."
        )
    output_dir = Path(output_dir)
    if resolver is None and data_source == DEPSDEV:
        client = depsdev_client or DepsDevClient(
            cache=ResolverCache(cache_dir=output_dir / "cache")
        )
        resolver = DepsDevResolver(
            system=depsdev_system(ecosystem),
            package=package,
            start_date=start_date,
            end_date=end_date,
            client=client,
        )
    analyzer = DependencyAnalyzer(
        ecosystem=ecosystem,
        package=package,
        start_date=start_date,
        end_date=end_date,
        weighting_type=weighting_type,
        half_life=half_life,
        output_dir=output_dir,
        resolver=resolver,
    )
    common: Dict[str, Any] = {
        "ecosystem": analyzer.ecosystem,
        "package": package,
        "mode": mode,
        "start_date": analyzer.start_date.date().isoformat(),
        "end_date": analyzer.end_date.date().isoformat(),
        "weighting_type": weighting_type,
        "half_life": half_life,
        "data_source": data_source,
    }
    try:
        if mode == PER_RELEASE:
            release_results = analyzer.analyze_at_release_points(
                {"start_date": analyzer.start_date, "end_date": analyzer.end_date},
                osv_df=osv_df,
                generate_dep_frames=dependency_frames,
                progress=progress,
            )
            return AnalysisResult(
                **common,
                releases=[_release_result(r["summary"]) for r in release_results],
                details=release_results,
            )
        raw: Dict[str, Any] = analyzer.analyze(osv_df=osv_df, progress=progress)
    except Exception as exc:
        if raise_errors:
            raise
        logger.exception("Analysis of %s/%s failed", analyzer.ecosystem, package)
        message = str(exc) or type(exc).__name__
        error_summary = (
            WindowSummary(None, None, 0, "error", message) if mode == WINDOW_SUMMARY else None
        )
        return AnalysisResult(**common, summary=error_summary, error=message)

    num_deps = int(raw.get("num_dependencies", 0))
    summary = WindowSummary(
        mttu=float(raw.get("ttu", 0.0)),
        mttr=float(raw.get("ttr", 0.0)),
        num_dependencies=num_deps,
        status="ok" if num_deps else "no_deps",
    )
    return AnalysisResult(**common, summary=summary, details=raw)


def _release_result(summary: Dict[str, Any]) -> ReleaseResult:
    return ReleaseResult(
        package_version=str(summary.get("package_version", "")),
        package_release_date=str(summary.get("package_release_date", "")),
        mttu=_days(summary.get("mttu", 0.0)),
        mttr=_days(summary.get("mttr", summary.get("mttr_all_severities", 0.0))),
        num_dependencies=int(summary.get("num_dependencies", 0)),
        status=str(summary.get("status", "")),
        error=str(summary.get("error", "")),
    )


def _days(value: Any) -> Optional[float]:
    return None if value is None else float(value)
