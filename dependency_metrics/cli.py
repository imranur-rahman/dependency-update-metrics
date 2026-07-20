"""
Command-line interface for the dependency metrics tool.
"""

import argparse
import gc
import io
import multiprocessing
import os
import sys
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import psutil

import pandas as pd
from concurrent.futures import (
    FIRST_COMPLETED,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from concurrent.futures.process import BrokenProcessPool

from .analyzer import DependencyAnalyzer, build_osv_index
from .osv_builder import OSVBuilder
from .resolvers import ResolverCache
from .depsdev_client import DepsDevClient
from .depsdev_resolver import DepsDevResolver
from .reporting import (
    export_osv_data,
    export_per_release_worksheets,
    export_worksheets,
    print_summary,
    safe_filename_stem,
    save_results_json,
)

# ---------------------------------------------------------------------------
# Module-level worker state — populated by _init_worker_process() in each
# child process spawned by ProcessPoolExecutor.  Using a plain dict keeps
# init simple and avoids any pickling of non-serialisable objects.
# ---------------------------------------------------------------------------

_WORKER_STATE: Dict[str, Any] = {}
_VALID_ECOSYSTEMS = {"npm", "pypi", "cargo"}
_ECOSYSTEM_ALIASES = {"crates.io": "cargo"}
_OSV_ECOSYSTEM_NAMES = {
    "npm": "NPM",
    "pypi": "PYPI",
    "cargo": "CRATES.IO",
}


def _normalize_ecosystem(value: Any) -> str:
    """Normalize supported ecosystem aliases to internal names."""
    raw = str(value or "").strip().lower()
    return _ECOSYSTEM_ALIASES.get(raw, raw)


def _init_worker_process(
    cache_dir_str: Optional[str],
    osv_index_path_str: str,
    use_depsdev: bool,
    severity_breakdown: bool,
    weighting_type: str,
    half_life: Optional[float],
    output_dir_str: str,
    default_start_date: datetime,
    log_file_str: Optional[str],
    max_worker_memory_mb: int = 0,
) -> None:
    """Initialise per-process state for ProcessPoolExecutor workers.

    Called once per worker process at startup (via *initializer* kwarg).

    The OSV index is written to a temp file by the main process and loaded here
    by path string.  This avoids pickling the index dict 35× via IPC pipes
    (which caused OOM when the index is several hundred MB).  The OS page cache
    means every worker after the first reads from RAM rather than disk.

    SQLite warm-up is intentionally skipped: workers open their own thread-local
    connections on demand.  The OS page cache (primed by the main-process
    warm-up) makes these reads fast without paying the 25%-of-RAM warm-up cost
    in every worker process.
    """
    import pickle

    # Worker logging — write to the same file as the main process (append mode).
    _wlogger = logging.getLogger("dependency_metrics")
    _wlogger.propagate = False
    _wlogger.setLevel(logging.WARNING)
    if not _wlogger.handlers:
        _sh = logging.StreamHandler()
        _sh.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        _wlogger.addHandler(_sh)
        if log_file_str:
            _fh = logging.FileHandler(log_file_str, encoding="utf-8")
            _fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
            _wlogger.addHandler(_fh)

    # Per-process ResolverCache.  No warm_from_disk() here — the main process
    # already primed the OS page cache; on-demand SQLite reads are fast enough.
    cache_dir = Path(cache_dir_str) if cache_dir_str else None
    cache = ResolverCache(cache_dir=cache_dir)

    # Load the pre-built OSV index from the temp file written by the main process.
    # Subsequent workers benefit from the OS page cache — effectively free.
    with open(osv_index_path_str, "rb") as _f:
        osv_index_by_ecosystem: Dict[str, Any] = pickle.load(_f)

    # Empty DataFrames as fallback (index path is taken whenever osv_index is
    # non-empty, so the DataFrame fallback is unused in normal operation).
    osv_by_eco: Dict[str, Any] = {eco: pd.DataFrame() for eco in osv_index_by_ecosystem}

    depsdev_client: Optional[DepsDevClient] = DepsDevClient(cache=cache) if use_depsdev else None

    _WORKER_STATE.update(
        {
            "cache": cache,
            "depsdev_client": depsdev_client,
            "osv_by_ecosystem": osv_by_eco,
            "osv_index_by_ecosystem": osv_index_by_ecosystem,
            "use_depsdev": use_depsdev,
            "severity_breakdown": severity_breakdown,
            "weighting_type": weighting_type,
            "half_life": half_life,
            "output_dir": Path(output_dir_str),
            "default_start_date": default_start_date,
            "max_worker_memory_mb": max_worker_memory_mb,
        }
    )


def _make_timeout_result(
    row: Dict[str, Any],
    seconds: int,
    severity_breakdown: bool,
    per_release: bool,
) -> Dict[str, Any]:
    """Build an error result dict for a package that exceeded ``--package-timeout``."""
    ecosystem = _normalize_ecosystem(row.get("ecosystem", ""))
    package_name = str(row.get("package_name", ""))
    row_num = row.get("_row_num") or row.get("row_num")
    error_msg = f'"Package timed out after {seconds}s"'

    try:
        start_raw = str(row.get("start_date", ""))
        start_str = start_raw if start_raw else ""
    except Exception:
        start_str = ""
    try:
        end_raw = str(row.get("end_date", ""))
        end_str = end_raw if end_raw else ""
    except Exception:
        end_str = ""

    if per_release and severity_breakdown:
        summary: Dict[str, Any] = {
            "ecosystem": ecosystem,
            "package_name": package_name,
            "package_version": "",
            "package_release_date": "",
            "window_start": start_str,
            "window_end": end_str,
            "mttu": -1.0,
            "mttr_critical": -1.0,
            "mttr_high": -1.0,
            "mttr_medium": -1.0,
            "mttr_low": -1.0,
            "mttr_all_severities": -1.0,
            "num_dependencies": 0,
            "status": "error",
            "error": error_msg,
        }
    elif per_release:
        summary = {
            "ecosystem": ecosystem,
            "package_name": package_name,
            "package_version": "",
            "package_release_date": "",
            "window_start": start_str,
            "window_end": end_str,
            "mttu": -1.0,
            "mttr": -1.0,
            "num_dependencies": 0,
            "status": "error",
            "error": error_msg,
        }
    elif severity_breakdown:
        summary = {
            "ecosystem": ecosystem,
            "package_name": package_name,
            "start_date": start_str,
            "end_date": end_str,
            "mttu": -1.0,
            "mttr_critical": -1.0,
            "mttr_high": -1.0,
            "mttr_medium": -1.0,
            "mttr_low": -1.0,
            "mttr_all_severities": -1.0,
            "num_dependencies": 0,
            "status": "error",
            "error": error_msg,
        }
    else:
        summary = {
            "ecosystem": ecosystem,
            "package_name": package_name,
            "start_date": start_str,
            "end_date": end_str,
            "mttu": -1.0,
            "mttr": -1.0,
            "num_dependencies": 0,
            "status": "error",
            "error": error_msg,
        }
    return {"row_num": row_num, "summary": summary, "dependency_frames": []}


def _worker_make_resolver(eco: str, pkg: str, start: datetime, end: datetime):
    """Create a resolver for *pkg* using the worker-process ``_WORKER_STATE``."""
    from .resolvers import CratesResolver, NpmResolver, PyPIResolver as _PyPIResolverCls

    ws = _WORKER_STATE
    _registry_urls = {
        "npm": "https://registry.npmjs.org",
        "pypi": "https://pypi.org/pypi",
        "cargo": "https://crates.io",
    }
    _eco_to_system = {"npm": "NPM", "pypi": "PYPI", "cargo": "CARGO"}

    if ws["use_depsdev"] and ws["depsdev_client"] is not None:
        return DepsDevResolver(
            system=_eco_to_system.get(eco, eco.upper()),
            package=pkg,
            start_date=start,
            end_date=end,
            client=ws["depsdev_client"],
        )
    if eco == "npm":
        return NpmResolver(
            package=pkg,
            start_date=start,
            end_date=end,
            registry_urls=_registry_urls,
            cache=ws["cache"],
        )
    if eco == "cargo":
        return CratesResolver(
            package=pkg,
            start_date=start,
            end_date=end,
            registry_urls=_registry_urls,
            cache=ws["cache"],
        )
    return _PyPIResolverCls(
        package=pkg,
        start_date=start,
        end_date=end,
        registry_urls=_registry_urls,
        cache=ws["cache"],
    )


def _worker_run_group(task: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Module-level worker for ``analyze_bulk_rows`` (non-per-release mode).

    Mirrors the ``_process_group`` closure from the main function but reads
    configuration from ``_WORKER_STATE`` instead of captured variables.
    """
    ws = _WORKER_STATE
    rows: List[Dict[str, Any]] = task["rows"]
    default_start_date: datetime = ws["default_start_date"]
    severity_breakdown: bool = ws["severity_breakdown"]
    weighting_type: str = ws["weighting_type"]
    half_life: Optional[float] = ws["half_life"]
    output_dir: Path = ws["output_dir"]
    osv_by_ecosystem: Dict[str, Any] = ws["osv_by_ecosystem"]
    osv_index_by_ecosystem: Dict[str, Any] = ws["osv_index_by_ecosystem"]
    use_depsdev: bool = ws["use_depsdev"]

    error_results: List[Dict[str, Any]] = []
    valid_rows: List[Dict[str, Any]] = []

    for row in rows:
        row_num = row.get("_row_num")
        ecosystem = _normalize_ecosystem(row.get("ecosystem", ""))
        package_name = str(row.get("package_name", ""))
        end_date_raw = str(row.get("end_date", ""))
        start_date_raw = str(row.get("start_date", ""))
        first_release_date_raw = str(row.get("first_release_date", ""))

        try:
            if not ecosystem or not package_name or not end_date_raw:
                raise ValueError("ecosystem, package_name, and end_date are required.")
            if ecosystem not in _VALID_ECOSYSTEMS:
                raise ValueError(f"Unsupported ecosystem: {ecosystem}.")

            start_date = default_start_date
            if start_date_raw:
                start_date = _parse_date(start_date_raw, "start_date", row_num)
            elif first_release_date_raw:
                start_date = _parse_date(first_release_date_raw, "first_release_date", row_num)
            end_date = _parse_date(end_date_raw, "end_date", row_num)

            valid_rows.append({"row_num": row_num, "start_date": start_date, "end_date": end_date})
        except Exception as exc:
            start_date = default_start_date
            if start_date_raw:
                try:
                    start_date = _parse_date(start_date_raw, "start_date", row_num)
                except ValueError:
                    start_date = default_start_date
            try:
                end_date = _parse_date(end_date_raw, "end_date", row_num)
            except ValueError:
                end_date = datetime.today()

            if severity_breakdown:
                err_summary: Dict[str, Any] = {
                    "ecosystem": ecosystem,
                    "package_name": package_name,
                    "start_date": start_date.date().isoformat(),
                    "end_date": end_date.date().isoformat(),
                    "mttu": -1.0,
                    "mttr_critical": -1.0,
                    "mttr_high": -1.0,
                    "mttr_medium": -1.0,
                    "mttr_low": -1.0,
                    "mttr_all_severities": -1.0,
                    "num_dependencies": 0,
                    "status": "error",
                    "error": f'"{exc}"',
                }
            else:
                err_summary = {
                    "ecosystem": ecosystem,
                    "package_name": package_name,
                    "start_date": start_date.date().isoformat(),
                    "end_date": end_date.date().isoformat(),
                    "mttu": -1.0,
                    "mttr": -1.0,
                    "num_dependencies": 0,
                    "status": "error",
                    "error": f'"{exc}"',
                }
            error_results.append(
                {"row_num": row_num, "summary": err_summary, "dependency_frames": []}
            )

    if not valid_rows:
        return error_results

    ecosystem = _normalize_ecosystem(rows[0].get("ecosystem", ""))
    package_name = str(rows[0].get("package_name", ""))
    min_start = min(r["start_date"] for r in valid_rows)
    max_end = max(r["end_date"] for r in valid_rows)

    _injected_resolver = (
        _worker_make_resolver(ecosystem, package_name, min_start, max_end) if use_depsdev else None
    )
    analyzer = DependencyAnalyzer(
        ecosystem=ecosystem,
        package=package_name,
        start_date=min_start,
        end_date=max_end,
        weighting_type=weighting_type,
        half_life=half_life,
        output_dir=output_dir,
        resolver_cache=ws["cache"],
        severity_breakdown=severity_breakdown,
        resolver=_injected_resolver,
    )
    analyzer._osv_index = osv_index_by_ecosystem.get(ecosystem, {})
    analyzer._osv_df = osv_by_ecosystem.get(ecosystem, pd.DataFrame())

    try:
        results = analyzer.analyze_bulk_rows(valid_rows, osv_df=osv_by_ecosystem.get(ecosystem))
    except Exception as exc:
        error = f'"{exc}"'
        for row in valid_rows:
            if severity_breakdown:
                exc_summary: Dict[str, Any] = {
                    "ecosystem": ecosystem,
                    "package_name": package_name,
                    "start_date": row["start_date"].date().isoformat(),
                    "end_date": row["end_date"].date().isoformat(),
                    "mttu": -1.0,
                    "mttr_critical": -1.0,
                    "mttr_high": -1.0,
                    "mttr_medium": -1.0,
                    "mttr_low": -1.0,
                    "mttr_all_severities": -1.0,
                    "num_dependencies": 0,
                    "status": "error",
                    "error": error,
                }
            else:
                exc_summary = {
                    "ecosystem": ecosystem,
                    "package_name": package_name,
                    "start_date": row["start_date"].date().isoformat(),
                    "end_date": row["end_date"].date().isoformat(),
                    "mttu": -1.0,
                    "mttr": -1.0,
                    "num_dependencies": 0,
                    "status": "error",
                    "error": error,
                }
            error_results.append(
                {"row_num": row["row_num"], "summary": exc_summary, "dependency_frames": []}
            )
        return error_results

    return results + error_results


def _worker_run_group_per_release(task: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Module-level worker for ``analyze_at_release_points`` (per-release mode).

    Mirrors the ``_process_group_per_release`` closure but reads configuration
    from ``_WORKER_STATE`` instead of captured variables.
    """
    ws = _WORKER_STATE
    rows: List[Dict[str, Any]] = task["rows"]
    default_start_date: datetime = ws["default_start_date"]
    severity_breakdown: bool = ws["severity_breakdown"]
    weighting_type: str = ws["weighting_type"]
    half_life: Optional[float] = ws["half_life"]
    output_dir: Path = ws["output_dir"]
    osv_by_ecosystem: Dict[str, Any] = ws["osv_by_ecosystem"]
    osv_index_by_ecosystem: Dict[str, Any] = ws["osv_index_by_ecosystem"]
    use_depsdev: bool = ws["use_depsdev"]
    max_worker_memory_mb: int = ws.get("max_worker_memory_mb", 0)

    error_results: List[Dict[str, Any]] = []
    valid_rows: List[Dict[str, Any]] = []

    for row in rows:
        row_num = row.get("_row_num")
        ecosystem = _normalize_ecosystem(row.get("ecosystem", ""))
        package_name = str(row.get("package_name", ""))
        end_date_raw = str(row.get("end_date", ""))
        start_date_raw = str(row.get("start_date", ""))
        first_release_date_raw = str(row.get("first_release_date", ""))

        try:
            if not ecosystem or not package_name or not end_date_raw:
                raise ValueError("ecosystem, package_name, and end_date are required.")
            if ecosystem not in _VALID_ECOSYSTEMS:
                raise ValueError(f"Unsupported ecosystem: {ecosystem}.")

            start_date = default_start_date
            if start_date_raw:
                start_date = _parse_date(start_date_raw, "start_date", row_num)
            elif first_release_date_raw:
                start_date = _parse_date(first_release_date_raw, "first_release_date", row_num)
            end_date = _parse_date(end_date_raw, "end_date", row_num)

            valid_rows.append({"row_num": row_num, "start_date": start_date, "end_date": end_date})
        except Exception as exc:
            start_date = default_start_date
            if start_date_raw:
                try:
                    start_date = _parse_date(start_date_raw, "start_date", row_num)
                except ValueError:
                    start_date = default_start_date
            try:
                end_date = _parse_date(end_date_raw, "end_date", row_num)
            except ValueError:
                end_date = datetime.today()

            if severity_breakdown:
                pr_err_summary: Dict[str, Any] = {
                    "ecosystem": ecosystem,
                    "package_name": package_name,
                    "package_version": "",
                    "package_release_date": "",
                    "window_start": start_date.date().isoformat(),
                    "window_end": end_date.date().isoformat(),
                    "mttu": -1.0,
                    "mttr_critical": -1.0,
                    "mttr_high": -1.0,
                    "mttr_medium": -1.0,
                    "mttr_low": -1.0,
                    "mttr_all_severities": -1.0,
                    "num_dependencies": 0,
                    "status": "error",
                    "error": f'"{exc}"',
                }
            else:
                pr_err_summary = {
                    "ecosystem": ecosystem,
                    "package_name": package_name,
                    "package_version": "",
                    "package_release_date": "",
                    "window_start": start_date.date().isoformat(),
                    "window_end": end_date.date().isoformat(),
                    "mttu": -1.0,
                    "mttr": -1.0,
                    "num_dependencies": 0,
                    "status": "error",
                    "error": f'"{exc}"',
                }
            error_results.append(
                {"row_num": row_num, "summary": pr_err_summary, "dependency_frames": []}
            )

    if not valid_rows:
        return error_results

    ecosystem = _normalize_ecosystem(rows[0].get("ecosystem", ""))
    package_name = str(rows[0].get("package_name", ""))
    min_start = min(r["start_date"] for r in valid_rows)
    max_end = max(r["end_date"] for r in valid_rows)

    _injected_resolver_pr = (
        _worker_make_resolver(ecosystem, package_name, min_start, max_end) if use_depsdev else None
    )
    analyzer = DependencyAnalyzer(
        ecosystem=ecosystem,
        package=package_name,
        start_date=min_start,
        end_date=max_end,
        weighting_type=weighting_type,
        half_life=half_life,
        output_dir=output_dir,
        resolver_cache=ws["cache"],
        severity_breakdown=severity_breakdown,
        resolver=_injected_resolver_pr,
    )
    analyzer._osv_index = osv_index_by_ecosystem.get(ecosystem, {})
    analyzer._osv_df = osv_by_ecosystem.get(ecosystem, pd.DataFrame())

    merged_row = {
        "row_num": valid_rows[0]["row_num"],
        "start_date": min_start,
        "end_date": max_end,
    }
    _wlogger = logging.getLogger("dependency_metrics")
    _wlogger.warning(
        "Worker %d: starting %s/%s [%s → %s]",
        os.getpid(),
        ecosystem,
        package_name,
        min_start.date(),
        max_end.date(),
    )
    _w_t0 = time.monotonic()
    all_results: List[Dict[str, Any]] = []
    try:
        release_results = analyzer.analyze_at_release_points(
            merged_row,
            osv_df=osv_by_ecosystem.get(ecosystem),
            max_memory_mb=max_worker_memory_mb,
        )
        all_results.extend(release_results)
        _wlogger.warning(
            "Worker %d: finished %s/%s — %d release points in %.1fs",
            os.getpid(),
            ecosystem,
            package_name,
            len(all_results),
            time.monotonic() - _w_t0,
        )
    except Exception as exc:
        error = f'"{exc}"'
        if severity_breakdown:
            pr_exc_summary: Dict[str, Any] = {
                "ecosystem": ecosystem,
                "package_name": package_name,
                "package_version": "",
                "package_release_date": "",
                "window_start": min_start.date().isoformat(),
                "window_end": max_end.date().isoformat(),
                "mttu": -1.0,
                "mttr_critical": -1.0,
                "mttr_high": -1.0,
                "mttr_medium": -1.0,
                "mttr_low": -1.0,
                "mttr_all_severities": -1.0,
                "num_dependencies": 0,
                "status": "error",
                "error": error,
            }
        else:
            pr_exc_summary = {
                "ecosystem": ecosystem,
                "package_name": package_name,
                "package_version": "",
                "package_release_date": "",
                "window_start": min_start.date().isoformat(),
                "window_end": max_end.date().isoformat(),
                "mttu": -1.0,
                "mttr": -1.0,
                "num_dependencies": 0,
                "status": "error",
                "error": error,
            }
        _wlogger.warning(
            "Worker %d: error on %s/%s after %.1fs — %s",
            os.getpid(),
            ecosystem,
            package_name,
            time.monotonic() - _w_t0,
            exc,
        )
        error_results.append(
            {
                "row_num": merged_row["row_num"],
                "summary": pr_exc_summary,
                "dependency_frames": [],
            }
        )

    return all_results + error_results


def _parse_date(value: str, field: str, row_num: Optional[int] = None) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        location = f" on row {row_num}" if row_num is not None else ""
        raise ValueError(f"Invalid {field} format{location}. Use YYYY-MM-DD.")


def _load_input_csv(path: Path, default_end_date: Optional[str] = None) -> List[Dict[str, Any]]:
    raw = path.read_bytes()
    if raw.lstrip().startswith(b"{\\rtf"):
        raise ValueError("Input file appears to be RTF. Please export as CSV.")

    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            text = ""
    if not text:
        raise ValueError("Unable to decode input CSV. Please save as UTF-8.")

    buffer = io.StringIO(text)
    try:
        df = pd.read_csv(buffer, sep=None, engine="python")
    except Exception as exc:
        raise ValueError(f"Failed to parse CSV: {exc}") from exc

    if df.empty:
        raise ValueError("Input CSV contains no data rows.")

    normalized_fields = []
    for name in df.columns:
        if name is None:
            continue
        normalized_fields.append(str(name).strip().lstrip("\ufeff"))

    field_map = {name.lower(): name for name in normalized_fields}
    required = {"ecosystem", "package_name"}
    missing = required.difference(field_map.keys())
    if "end_date" not in field_map and not default_end_date:
        missing.add("end_date")
    if missing:
        available = ", ".join(normalized_fields) if normalized_fields else "none"
        raise ValueError(
            f"Input CSV missing required columns: {', '.join(sorted(missing))}. "
            f"Found columns: {available}."
        )

    rows: List[Dict[str, Any]] = []
    for idx, record in df.iterrows():
        row_num = int(idx) + 2
        cleaned = {
            str(k).strip().lstrip("\ufeff"): ("" if pd.isna(v) else str(v).strip())
            for k, v in record.items()
        }
        normalized: Dict[str, Any] = {
            "ecosystem": _normalize_ecosystem(cleaned.get(field_map["ecosystem"], "")),
            "package_name": cleaned.get(field_map["package_name"], ""),
            "end_date": (
                cleaned.get(field_map["end_date"], "")
                if "end_date" in field_map
                else default_end_date
            ),
            "start_date": cleaned.get(field_map.get("start_date", ""), ""),
        }
        for key, value in cleaned.items():
            if key not in normalized:
                normalized[key] = value
        normalized["_row_num"] = row_num
        rows.append(normalized)

    if not rows:
        raise ValueError("Input CSV contains no data rows.")

    return rows


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Analyze dependency update and remediation metrics for packages"
    )

    parser.add_argument(
        "--ecosystem",
        choices=["npm", "pypi", "cargo", "crates.io"],
        help="The ecosystem to analyze (npm, pypi, cargo, or crates.io).",
    )

    parser.add_argument(
        "--depsdev",
        action="store_true",
        help=(
            "Use the deps.dev API for all package and dependency data. "
            "Supports npm, pypi, and cargo. No calls are made to native registries."
        ),
    )

    parser.add_argument("--package", help="The name of the package to analyze")

    parser.add_argument(
        "--input-csv",
        help=(
            "CSV file with columns: ecosystem, package_name, optional start_date, "
            "optional end_date. If end_date is omitted, --end-date is used."
        ),
    )

    parser.add_argument(
        "--start-date",
        default="1900-01-01",
        help="Start date for analysis (YYYY-MM-DD). Default: 1900-01-01",
    )

    parser.add_argument(
        "--end-date", default=None, help="End date for analysis (YYYY-MM-DD). Default: today"
    )

    parser.add_argument(
        "--weighting-type",
        choices=["linear", "exponential", "inverse", "disable"],
        default="disable",
        help="Type of weighting to apply. Default: disable",
    )

    parser.add_argument(
        "--half-life",
        type=float,
        default=None,
        help="Half-life in days (required for exponential weighting)",
    )

    parser.add_argument(
        "--build-osv", action="store_true", help="Build the OSV vulnerability database"
    )

    parser.add_argument(
        "--get-osv",
        action="store_true",
        help="Return the OSV dataset for the ecosystem and vulnerable dependencies",
    )

    parser.add_argument(
        "--get-worksheets",
        action="store_true",
        help="Export dependency dataframes to an Excel file with multiple sheets",
    )

    parser.add_argument(
        "--output-dir", default="./output", help="Output directory for results. Default: ./output"
    )

    parser.add_argument(
        "--write-dependency-details",
        action="store_true",
        help=(
            "Write the per-interval dependency details CSV in bulk mode. "
            "Disabled by default."
        ),
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of parallel workers for bulk CSV mode. Default: CPU count",
    )
    parser.add_argument(
        "--max-worker-memory-mb",
        type=int,
        default=3000,
        help=(
            "Per-worker RSS memory limit in MB. If a worker exceeds this during analysis "
            "it exits cleanly with an error result (resumable via --resume) instead of "
            "being OOM-killed. Default: 3000 MB. Set to 0 to disable."
        ),
    )

    parser.add_argument(
        "--package-timeout",
        type=int,
        default=None,
        metavar="SECONDS",
        help=(
            "Per-package analysis timeout in seconds (bulk CSV mode only). "
            "If a package's analysis exceeds this time, an error row is written "
            "and the next package is processed. Default: disabled."
        ),
    )

    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume bulk CSV runs by skipping rows already completed in output summary",
    )

    parser.add_argument(
        "--per-release",
        action="store_true",
        help="Compute MTTU/MTTR at every release of the parent package within the window",
    )

    parser.add_argument(
        "--severity-breakdown",
        action="store_true",
        help="Report MTTR separately for Critical, High, Medium, Low, and all_severities",
    )

    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    parser.add_argument(
        "--log-file",
        default=None,
        help="Mirror all log output to this file (survives tmux/terminal death). "
        "Default: <output-dir>/run.log",
    )

    args = parser.parse_args()

    # Validate arguments
    if args.weighting_type == "exponential" and args.half_life is None:
        parser.error("--half-life is required when --weighting-type is exponential")

    if args.ecosystem:
        args.ecosystem = _normalize_ecosystem(args.ecosystem)

    # Parse default start date
    try:
        default_start_date = _parse_date(args.start_date, "start_date")
    except ValueError as exc:
        logging.getLogger("dependency_metrics").error("Error: %s", exc)
        sys.exit(1)
    try:
        default_end_date = (
            _parse_date(args.end_date, "end_date") if args.end_date else datetime.today()
        )
    except ValueError as exc:
        logging.getLogger("dependency_metrics").error("Error: %s", exc)
        sys.exit(1)

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Configure logging — explicit handlers with propagate=False to prevent double-printing
    _dm_logger = logging.getLogger("dependency_metrics")
    _dm_logger.propagate = False
    _dm_logger.setLevel(logging.DEBUG)

    _stream_handler = logging.StreamHandler()
    _stream_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    _stream_handler.setLevel(logging.DEBUG if args.verbose else logging.WARNING)
    _dm_logger.addHandler(_stream_handler)

    log_file_path = Path(args.log_file) if args.log_file else output_dir / "run.log"
    _file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
    _file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    _file_handler.setLevel(logging.DEBUG)
    _dm_logger.addHandler(_file_handler)

    if args.input_csv:

        input_csv = Path(args.input_csv)
        if not input_csv.exists():
            logging.getLogger("dependency_metrics").error(
                "Error: Input CSV not found: %s", input_csv
            )
            sys.exit(1)

        try:
            input_rows = _load_input_csv(input_csv, default_end_date.date().isoformat())
        except ValueError as exc:
            logging.getLogger("dependency_metrics").error("Error: %s", exc)
            sys.exit(1)

        # Remove duplicate rows by ecosystem/package_name/end_date (case-insensitive)
        deduped_rows = []
        seen_keys = set()
        duplicates = 0
        for row in input_rows:
            key = (
                _normalize_ecosystem(row.get("ecosystem", "")),
                str(row.get("package_name", "")).strip().lower(),
                str(row.get("end_date", "")).strip(),
            )
            if key in seen_keys:
                duplicates += 1
                continue
            seen_keys.add(key)
            deduped_rows.append(row)
        input_rows = deduped_rows
        if duplicates:
            logging.getLogger("dependency_metrics").info(
                "Removed %s duplicate rows from input CSV.",
                duplicates,
            )

        input_label = input_csv.stem
        _depsdev_suffix = "_depsdev" if args.depsdev else ""
        if args.per_release:
            summary_file_path = (
                output_dir / f"{input_label}{_depsdev_suffix}_per_release_results.csv"
            )
            deps_file_path = (
                output_dir / f"{input_label}{_depsdev_suffix}_per_release_dependency_details.csv"
            )
            completed_file_path = (
                output_dir / f"{input_label}{_depsdev_suffix}_per_release_completed.csv"
            )
        else:
            summary_file_path = output_dir / f"{input_label}{_depsdev_suffix}_bulk_results.csv"
            deps_file_path = output_dir / f"{input_label}{_depsdev_suffix}_dependency_details.csv"
            completed_file_path = None

        existing_status = {}
        # For per-release resume: track
        # (ecosystem, package_name, window_start, package_version) already written
        existing_per_release: set = set()
        completed_input_rows: set = set()
        if args.resume and summary_file_path.exists():
            try:
                summary_df = pd.read_csv(summary_file_path, dtype=str, low_memory=False)
                if args.per_release:
                    for _, record in summary_df.iterrows():
                        ecosystem_r = _normalize_ecosystem(record.get("ecosystem", ""))
                        package_r = str(record.get("package_name", "")).strip().lower()
                        window_start_r = str(record.get("window_start", "")).strip()
                        pkg_ver_r = str(record.get("package_version", "")).strip()
                        status_r = str(record.get("status", "")).strip().lower()
                        if not ecosystem_r or not package_r or not window_start_r or not pkg_ver_r:
                            continue
                        existing_per_release.add(
                            (ecosystem_r, package_r, window_start_r, pkg_ver_r)
                        )
                        existing_status[(ecosystem_r, package_r, window_start_r, "")] = status_r
                else:
                    for _, record in summary_df.iterrows():
                        ecosystem_raw = str(record.get("ecosystem", "")).strip()
                        package_name_raw = str(record.get("package_name", "")).strip()
                        ecosystem = _normalize_ecosystem(ecosystem_raw)
                        package_name = package_name_raw.lower()
                        start_date = str(record.get("start_date", "")).strip()
                        end_date = str(record.get("end_date", "")).strip()
                        status = str(record.get("status", "")).strip().lower()
                        if not ecosystem or not package_name or not start_date or not end_date:
                            continue
                        key = (ecosystem, package_name, start_date, end_date)
                        existing_status[key] = status
            except Exception as exc:
                logging.getLogger("dependency_metrics").warning(
                    "Failed to read existing summary for resume: %s", exc
                )
                existing_status = {}
                existing_per_release = set()

        if args.per_release and completed_file_path is not None and completed_file_path.exists():
            try:
                ledger_df = pd.read_csv(completed_file_path)
                for _, rec in ledger_df.iterrows():
                    eco = _normalize_ecosystem(rec.get("ecosystem", ""))
                    pkg = str(rec.get("package_name", "")).strip().lower()
                    ws = str(rec.get("window_start", "")).strip()
                    we = str(rec.get("window_end", "")).strip()
                    if eco and pkg and ws and we:
                        completed_input_rows.add((eco, pkg, ws, we))
            except Exception as exc:
                logging.getLogger("dependency_metrics").warning(
                    "Failed to read per-release completion ledger: %s", exc
                )
                completed_input_rows = set()

        # Capture pre-filter totals so resume runs show cumulative progress.
        total_rows_all = len(input_rows)
        total_unique_packages_all = len(
            {
                (
                    _normalize_ecosystem(r.get("ecosystem", "")),
                    str(r.get("package_name", "")).strip().lower(),
                )
                for r in input_rows
            }
        )
        processed_before_resume = 0
        packages_done_before_resume = 0

        if args.resume and existing_status and not args.per_release:
            filtered_rows = []
            skipped = 0
            retried = 0
            new_rows = 0
            for row in input_rows:
                row_num = row.get("_row_num")
                ecosystem = _normalize_ecosystem(row.get("ecosystem", ""))
                package_name = str(row.get("package_name", "")).strip().lower()
                end_date_raw = str(row.get("end_date", "")).strip()
                start_date_raw = str(row.get("start_date", "")).strip()

                try:
                    start_date = default_start_date
                    if start_date_raw:
                        start_date = _parse_date(start_date_raw, "start_date", row_num)
                    end_date = _parse_date(end_date_raw, "end_date", row_num)
                except Exception:
                    filtered_rows.append(row)
                    new_rows += 1
                    continue

                key = (
                    ecosystem,
                    package_name,
                    start_date.date().isoformat(),
                    end_date.date().isoformat(),
                )
                status = existing_status.get(key)
                if status == "ok":
                    skipped += 1
                    continue
                if status == "error":
                    retried += 1
                else:
                    new_rows += 1
                filtered_rows.append(row)

            input_rows = filtered_rows
            processed_before_resume = skipped
            logging.getLogger("dependency_metrics").warning(
                "Resume enabled: skipping %s completed rows, "
                "retrying %s error rows, processing %s new rows.",
                skipped,
                retried,
                new_rows,
            )

            if not input_rows:
                logging.getLogger("dependency_metrics").warning(
                    "Resume enabled: no rows to process (all completed)."
                )
                sys.exit(0)

        elif args.resume and args.per_release and completed_input_rows:
            # The ledger has one entry per package (not per release row), so match
            # at the package level to correctly skip all rows for a completed package.
            completed_pkgs: set = {(eco, pkg) for eco, pkg, _ws, _we in completed_input_rows}
            filtered_rows = []
            skipped = 0
            skipped_pkg_keys: set = set()
            for row in input_rows:
                ecosystem = _normalize_ecosystem(row.get("ecosystem", ""))
                package_name = str(row.get("package_name", "")).strip().lower()
                if (ecosystem, package_name) in completed_pkgs:
                    skipped += 1
                    skipped_pkg_keys.add((ecosystem, package_name))
                else:
                    filtered_rows.append(row)
            input_rows = filtered_rows
            packages_done_before_resume = len(skipped_pkg_keys)
            remaining_packages = len(
                {
                    (
                        _normalize_ecosystem(r.get("ecosystem", "")),
                        str(r.get("package_name", "")).strip().lower(),
                    )
                    for r in input_rows
                }
            )
            logging.getLogger("dependency_metrics").warning(
                "Resume (per-release): skipping %s completed entries (%s unique packages), "
                "processing %s remaining entries (%s unique packages).",
                skipped,
                len(skipped_pkg_keys),
                len(input_rows),
                remaining_packages,
            )
            if not input_rows:
                logging.getLogger("dependency_metrics").warning(
                    "Resume (per-release): no packages to process (all completed)."
                )
                sys.exit(0)

        # Build OSV database automatically if missing
        osv_builder = OSVBuilder(output_dir)
        osv_df = osv_builder.build_database()
        if "severity" not in osv_df.columns:
            osv_df["severity"] = "None"
            logging.getLogger("dependency_metrics").warning(
                "OSV database missing 'severity' column — "
                "rebuild with --build-osv for severity support."
            )
        ecosystems = sorted(
            {_normalize_ecosystem(row["ecosystem"]) for row in input_rows if row.get("ecosystem")}
        )
        # Cargo vulnerabilities are stored under "crates.io" in the OSV dataset,
        # not "CARGO". All other ecosystems use their uppercased name.
        osv_by_ecosystem: Dict[str, Any] = {}
        for ecosystem in ecosystems:
            osv_filter = _OSV_ECOSYSTEM_NAMES.get(ecosystem, ecosystem.upper())
            if len(osv_df) > 0 and "ecosystem" in osv_df.columns:
                osv_by_ecosystem[ecosystem] = osv_df[osv_df["ecosystem"] == osv_filter].copy()
            else:
                osv_by_ecosystem[ecosystem] = osv_df

        # Build OSV index once per ecosystem (not once per package worker)
        osv_index_by_ecosystem: Dict[str, Dict] = {
            eco: build_osv_index(df) for eco, df in osv_by_ecosystem.items()
        }

        # Free the raw OSV DataFrame — the index is all we need from here on.
        # This reclaims 1-3 GB from the main process before worker processes start.
        del osv_df
        osv_by_ecosystem = {eco: pd.DataFrame() for eco in osv_by_ecosystem}
        gc.collect()

        resolver_cache = ResolverCache(cache_dir=output_dir / "cache")
        resolver_cache.warm_from_disk()
        _rss_mb = psutil.Process().memory_info().rss / 1024 / 1024
        logging.getLogger("dependency_metrics").warning(
            "Memory after cache warm-up: %.0f MB RSS", _rss_mb
        )

        # Pre-fetch all unique package metadata before workers start (eliminates thundering herd)
        from .resolvers import CratesResolver, NpmResolver, PyPIResolver as _PyPIResolverCls

        _registry_urls = {
            "npm": "https://registry.npmjs.org",
            "pypi": "https://pypi.org/pypi",
            "cargo": "https://crates.io",
        }

        # Shared deps.dev client (created once; thread-safe via ResolverCache session)
        _depsdev_client: Optional[DepsDevClient] = None
        if args.depsdev:
            _depsdev_client = DepsDevClient(cache=resolver_cache)

        _eco_to_system = {"npm": "NPM", "pypi": "PYPI", "cargo": "CARGO"}

        def _make_resolver(eco: str, pkg: str, start=None, end=None):
            _start = start or default_start_date
            _end = end or datetime.today()
            if args.depsdev and _depsdev_client is not None:
                return DepsDevResolver(
                    system=_eco_to_system.get(eco, eco.upper()),
                    package=pkg,
                    start_date=_start,
                    end_date=_end,
                    client=_depsdev_client,
                )
            if eco == "npm":
                return NpmResolver(
                    package=pkg,
                    start_date=_start,
                    end_date=_end,
                    registry_urls=_registry_urls,
                    cache=resolver_cache,
                )
            if eco == "cargo":
                return CratesResolver(
                    package=pkg,
                    start_date=_start,
                    end_date=_end,
                    registry_urls=_registry_urls,
                    cache=resolver_cache,
                )
            return _PyPIResolverCls(
                package=pkg,
                start_date=_start,
                end_date=_end,
                registry_urls=_registry_urls,
                cache=resolver_cache,
            )

        unique_packages = {
            (
                _normalize_ecosystem(r.get("ecosystem", "")),
                str(r.get("package_name", "")).strip(),
            )
            for r in input_rows
            if r.get("ecosystem") and r.get("package_name")
        }
        prefetch_workers = min(16, max(1, len(unique_packages)))
        _valid_prefetch_ecosystems = _VALID_ECOSYSTEMS
        with ThreadPoolExecutor(max_workers=prefetch_workers) as prefetch_exec:
            prefetch_futs = {
                prefetch_exec.submit(_make_resolver(eco, pkg).fetch_package_metadata, pkg): (
                    eco,
                    pkg,
                )
                for eco, pkg in unique_packages
                if eco in _valid_prefetch_ecosystems
            }
            _prefetch_pending = set(prefetch_futs)
            while _prefetch_pending:
                _pf_done, _prefetch_pending = wait(
                    _prefetch_pending, timeout=60, return_when=FIRST_COMPLETED
                )
                if not _pf_done:
                    logging.getLogger("dependency_metrics").warning(
                        "Prefetch: still waiting for %d package(s)...",
                        len(_prefetch_pending),
                    )
                    continue
                for f in _pf_done:
                    try:
                        f.result()
                    except Exception:
                        pass  # errors will surface again during analysis

        _rss_mb = psutil.Process().memory_info().rss / 1024 / 1024
        logging.getLogger("dependency_metrics").warning(
            "Memory after prefetch (%d packages): %.0f MB RSS", len(unique_packages), _rss_mb
        )

        total_rows = total_rows_all
        worker_count = args.workers
        if worker_count is None or worker_count <= 0:
            # ProcessPoolExecutor gives true CPU parallelism — no cap needed.
            worker_count = os.cpu_count() or 4

        def _process_group(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            error_results: List[Dict[str, Any]] = []
            valid_rows: List[Dict[str, Any]] = []

            for row in rows:
                row_num = row.get("_row_num")
                ecosystem = _normalize_ecosystem(row.get("ecosystem", ""))
                package_name = str(row.get("package_name", ""))
                end_date_raw = str(row.get("end_date", ""))
                start_date_raw = str(row.get("start_date", ""))
                first_release_date_raw = str(row.get("first_release_date", ""))

                try:
                    if not ecosystem or not package_name or not end_date_raw:
                        raise ValueError("ecosystem, package_name, and end_date are required.")
                    if ecosystem not in _VALID_ECOSYSTEMS:
                        raise ValueError(f"Unsupported ecosystem: {ecosystem}.")

                    start_date = default_start_date
                    if start_date_raw:
                        start_date = _parse_date(start_date_raw, "start_date", row_num)
                    elif first_release_date_raw:
                        start_date = _parse_date(
                            first_release_date_raw, "first_release_date", row_num
                        )
                    end_date = _parse_date(end_date_raw, "end_date", row_num)

                    valid_rows.append(
                        {
                            "row_num": row_num,
                            "start_date": start_date,
                            "end_date": end_date,
                        }
                    )
                except Exception as exc:
                    start_date = default_start_date
                    if start_date_raw:
                        try:
                            start_date = _parse_date(start_date_raw, "start_date", row_num)
                        except ValueError:
                            start_date = default_start_date
                    try:
                        end_date = _parse_date(end_date_raw, "end_date", row_num)
                    except ValueError:
                        end_date = datetime.today()

                    if args.severity_breakdown:
                        err_summary = {
                            "ecosystem": ecosystem,
                            "package_name": package_name,
                            "start_date": start_date.date().isoformat(),
                            "end_date": end_date.date().isoformat(),
                            "mttu": -1.0,
                            "mttr_critical": -1.0,
                            "mttr_high": -1.0,
                            "mttr_medium": -1.0,
                            "mttr_low": -1.0,
                            "mttr_all_severities": -1.0,
                            "num_dependencies": 0,
                            "status": "error",
                            "error": f'"{exc}"',
                        }
                    else:
                        err_summary = {
                            "ecosystem": ecosystem,
                            "package_name": package_name,
                            "start_date": start_date.date().isoformat(),
                            "end_date": end_date.date().isoformat(),
                            "mttu": -1.0,
                            "mttr": -1.0,
                            "num_dependencies": 0,
                            "status": "error",
                            "error": f'"{exc}"',
                        }
                    error_results.append(
                        {
                            "row_num": row_num,
                            "summary": err_summary,
                            "dependency_frames": [],
                        }
                    )

            if not valid_rows:
                return error_results

            ecosystem = _normalize_ecosystem(rows[0].get("ecosystem", ""))
            package_name = str(rows[0].get("package_name", ""))
            min_start = min(row["start_date"] for row in valid_rows)
            max_end = max(row["end_date"] for row in valid_rows)

            _injected_resolver = (
                _make_resolver(ecosystem, package_name, min_start, max_end)
                if args.depsdev
                else None
            )
            analyzer = DependencyAnalyzer(
                ecosystem=ecosystem,
                package=package_name,
                start_date=min_start,
                end_date=max_end,
                weighting_type=args.weighting_type,
                half_life=args.half_life,
                output_dir=output_dir,
                resolver_cache=resolver_cache,
                severity_breakdown=args.severity_breakdown,
                resolver=_injected_resolver,
            )
            analyzer._osv_index = osv_index_by_ecosystem.get(ecosystem, {})
            analyzer._osv_df = osv_by_ecosystem.get(ecosystem, pd.DataFrame())

            try:
                results = analyzer.analyze_bulk_rows(
                    valid_rows, osv_df=osv_by_ecosystem.get(ecosystem)
                )
            except Exception as exc:
                error = f'"{exc}"'
                for row in valid_rows:
                    if args.severity_breakdown:
                        exc_summary = {
                            "ecosystem": ecosystem,
                            "package_name": package_name,
                            "start_date": row["start_date"].date().isoformat(),
                            "end_date": row["end_date"].date().isoformat(),
                            "mttu": -1.0,
                            "mttr_critical": -1.0,
                            "mttr_high": -1.0,
                            "mttr_medium": -1.0,
                            "mttr_low": -1.0,
                            "mttr_all_severities": -1.0,
                            "num_dependencies": 0,
                            "status": "error",
                            "error": error,
                        }
                    else:
                        exc_summary = {
                            "ecosystem": ecosystem,
                            "package_name": package_name,
                            "start_date": row["start_date"].date().isoformat(),
                            "end_date": row["end_date"].date().isoformat(),
                            "mttu": -1.0,
                            "mttr": -1.0,
                            "num_dependencies": 0,
                            "status": "error",
                            "error": error,
                        }
                    error_results.append(
                        {
                            "row_num": row["row_num"],
                            "summary": exc_summary,
                            "dependency_frames": [],
                        }
                    )
                return error_results

            return results + error_results

        def _process_group_per_release(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            """Process a group of rows (same package) in per-release mode."""
            error_results: List[Dict[str, Any]] = []
            valid_rows: List[Dict[str, Any]] = []

            for row in rows:
                row_num = row.get("_row_num")
                ecosystem = _normalize_ecosystem(row.get("ecosystem", ""))
                package_name = str(row.get("package_name", ""))
                end_date_raw = str(row.get("end_date", ""))
                start_date_raw = str(row.get("start_date", ""))
                first_release_date_raw = str(row.get("first_release_date", ""))

                try:
                    if not ecosystem or not package_name or not end_date_raw:
                        raise ValueError("ecosystem, package_name, and end_date are required.")
                    if ecosystem not in _VALID_ECOSYSTEMS:
                        raise ValueError(f"Unsupported ecosystem: {ecosystem}.")

                    start_date = default_start_date
                    if start_date_raw:
                        start_date = _parse_date(start_date_raw, "start_date", row_num)
                    elif first_release_date_raw:
                        start_date = _parse_date(
                            first_release_date_raw, "first_release_date", row_num
                        )
                    end_date = _parse_date(end_date_raw, "end_date", row_num)

                    valid_rows.append(
                        {
                            "row_num": row_num,
                            "start_date": start_date,
                            "end_date": end_date,
                        }
                    )
                except Exception as exc:
                    start_date = default_start_date
                    if start_date_raw:
                        try:
                            start_date = _parse_date(start_date_raw, "start_date", row_num)
                        except ValueError:
                            start_date = default_start_date
                    try:
                        end_date = _parse_date(end_date_raw, "end_date", row_num)
                    except ValueError:
                        end_date = datetime.today()

                    if args.severity_breakdown:
                        pr_err_summary = {
                            "ecosystem": ecosystem,
                            "package_name": package_name,
                            "package_version": "",
                            "package_release_date": "",
                            "window_start": start_date.date().isoformat(),
                            "window_end": end_date.date().isoformat(),
                            "mttu": -1.0,
                            "mttr_critical": -1.0,
                            "mttr_high": -1.0,
                            "mttr_medium": -1.0,
                            "mttr_low": -1.0,
                            "mttr_all_severities": -1.0,
                            "num_dependencies": 0,
                            "status": "error",
                            "error": f'"{exc}"',
                        }
                    else:
                        pr_err_summary = {
                            "ecosystem": ecosystem,
                            "package_name": package_name,
                            "package_version": "",
                            "package_release_date": "",
                            "window_start": start_date.date().isoformat(),
                            "window_end": end_date.date().isoformat(),
                            "mttu": -1.0,
                            "mttr": -1.0,
                            "num_dependencies": 0,
                            "status": "error",
                            "error": f'"{exc}"',
                        }
                    error_results.append(
                        {
                            "row_num": row_num,
                            "summary": pr_err_summary,
                            "dependency_frames": [],
                        }
                    )

            if not valid_rows:
                return error_results

            ecosystem = _normalize_ecosystem(rows[0].get("ecosystem", ""))
            package_name = str(rows[0].get("package_name", ""))
            min_start = min(row["start_date"] for row in valid_rows)
            max_end = max(row["end_date"] for row in valid_rows)

            _injected_resolver_pr = (
                _make_resolver(ecosystem, package_name, min_start, max_end)
                if args.depsdev
                else None
            )
            analyzer = DependencyAnalyzer(
                ecosystem=ecosystem,
                package=package_name,
                start_date=min_start,
                end_date=max_end,
                weighting_type=args.weighting_type,
                half_life=args.half_life,
                output_dir=output_dir,
                resolver_cache=resolver_cache,
                severity_breakdown=args.severity_breakdown,
                resolver=_injected_resolver_pr,
            )
            analyzer._osv_index = osv_index_by_ecosystem.get(ecosystem, {})
            analyzer._osv_df = osv_by_ecosystem.get(ecosystem, pd.DataFrame())

            # All valid_rows for this package are merged into a single analysis window
            # [min_start, max_end]. This avoids redundant recomputation when the input
            # CSV contains multiple rows for the same package (e.g. duplicates or
            # overlapping date ranges that cover the same release points).
            merged_row = {
                "row_num": valid_rows[0]["row_num"],
                "start_date": min_start,
                "end_date": max_end,
            }
            all_results = []
            try:
                release_results = analyzer.analyze_at_release_points(
                    merged_row, osv_df=osv_by_ecosystem.get(ecosystem)
                )
                all_results.extend(release_results)
            except Exception as exc:
                error = f'"{exc}"'
                if args.severity_breakdown:
                    pr_exc_summary = {
                        "ecosystem": ecosystem,
                        "package_name": package_name,
                        "package_version": "",
                        "package_release_date": "",
                        "window_start": min_start.date().isoformat(),
                        "window_end": max_end.date().isoformat(),
                        "mttu": -1.0,
                        "mttr_critical": -1.0,
                        "mttr_high": -1.0,
                        "mttr_medium": -1.0,
                        "mttr_low": -1.0,
                        "mttr_all_severities": -1.0,
                        "num_dependencies": 0,
                        "status": "error",
                        "error": error,
                    }
                else:
                    pr_exc_summary = {
                        "ecosystem": ecosystem,
                        "package_name": package_name,
                        "package_version": "",
                        "package_release_date": "",
                        "window_start": min_start.date().isoformat(),
                        "window_end": max_end.date().isoformat(),
                        "mttu": -1.0,
                        "mttr": -1.0,
                        "num_dependencies": 0,
                        "status": "error",
                        "error": error,
                    }
                error_results.append(
                    {
                        "row_num": merged_row["row_num"],
                        "summary": pr_exc_summary,
                        "dependency_frames": [],
                    }
                )

            return all_results + error_results

        # Group rows by package to maximize cache reuse within a package
        grouped_rows: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
        for row in input_rows:
            key = (
                _normalize_ecosystem(row.get("ecosystem", "")),
                str(row.get("package_name", "")).strip().lower(),
            )
            grouped_rows.setdefault(key, []).append(row)

        total_unique_packages = total_unique_packages_all

        # Serialise the OSV index to a temp file so workers load it by path
        # (avoiding IPC-pipe pickling of a potentially large dict 35+ times).
        import pickle as _pickle
        import tempfile as _tempfile

        _osv_index_tmp = _tempfile.NamedTemporaryFile(
            delete=False, suffix="_osv_index.pkl", dir=output_dir / "cache"
        )
        try:
            _pickle.dump(osv_index_by_ecosystem, _osv_index_tmp)
            _osv_index_tmp.flush()
            _osv_index_tmp_path = _osv_index_tmp.name
        finally:
            _osv_index_tmp.close()

        _worker_init_args = (
            str(output_dir / "cache"),
            _osv_index_tmp_path,
            args.depsdev,
            args.severity_breakdown,
            args.weighting_type,
            args.half_life,
            str(output_dir),
            default_start_date,
            str(log_file_path),
            args.max_worker_memory_mb,
        )
        _mp_ctx = multiprocessing.get_context("spawn")

        with ProcessPoolExecutor(
            max_workers=worker_count,
            mp_context=_mp_ctx,
            initializer=_init_worker_process,
            initargs=_worker_init_args,
        ) as executor:
            futures = []
            future_to_pkg: Dict[Any, str] = {}
            future_to_rows: Dict[Any, List[Dict[str, Any]]] = {}
            future_submit_time: Dict[Any, float] = {}
            for (eco, pkg), rows in grouped_rows.items():
                task: Dict[str, Any] = {"rows": rows}
                if args.per_release:
                    f = executor.submit(_worker_run_group_per_release, task)
                else:
                    f = executor.submit(_worker_run_group, task)
                futures.append(f)
                future_to_pkg[f] = f"{eco}/{pkg}"
                future_to_rows[f] = rows
                future_submit_time[f] = time.monotonic()

            if args.write_dependency_details and deps_file_path.exists() and not args.resume:
                deps_file_path.unlink()

            if args.per_release and args.severity_breakdown:
                summary_columns = [
                    "ecosystem",
                    "package_name",
                    "package_version",
                    "package_release_date",
                    "window_start",
                    "window_end",
                    "mttu",
                    "mttr_critical",
                    "mttr_high",
                    "mttr_medium",
                    "mttr_low",
                    "mttr_all_severities",
                    "num_dependencies",
                    "status",
                    "error",
                ]
            elif args.per_release:
                summary_columns = [
                    "ecosystem",
                    "package_name",
                    "package_version",
                    "package_release_date",
                    "window_start",
                    "window_end",
                    "mttu",
                    "mttr",
                    "num_dependencies",
                    "status",
                    "error",
                ]
            elif args.severity_breakdown:
                summary_columns = [
                    "ecosystem",
                    "package_name",
                    "start_date",
                    "end_date",
                    "mttu",
                    "mttr_critical",
                    "mttr_high",
                    "mttr_medium",
                    "mttr_low",
                    "mttr_all_severities",
                    "num_dependencies",
                    "status",
                    "error",
                ]
            else:
                summary_columns = [
                    "ecosystem",
                    "package_name",
                    "start_date",
                    "end_date",
                    "mttu",
                    "mttr",
                    "num_dependencies",
                    "status",
                    "error",
                ]
            summary_file_path.parent.mkdir(parents=True, exist_ok=True)
            summary_writer = None
            deps_header_written = args.write_dependency_details and deps_file_path.exists()

            summary_exists = summary_file_path.exists()
            summary_mode = "a" if args.resume and summary_exists else "w"
            logging.getLogger("dependency_metrics").info(
                "Bulk summary output: %s (%s mode)",
                summary_file_path,
                "append" if summary_mode == "a" else "write",
            )
            summary_handle = summary_file_path.open(summary_mode, newline="")
            ledger_writer = None
            ledger_handle = None
            if args.per_release and completed_file_path is not None:
                ledger_exists = completed_file_path.exists()
                ledger_handle = completed_file_path.open("a", newline="")
                import csv as _csv

                ledger_writer = _csv.DictWriter(
                    ledger_handle,
                    fieldnames=[
                        "ecosystem",
                        "package_name",
                        "window_start",
                        "window_end",
                        "status",
                    ],
                )
                if not ledger_exists:
                    ledger_writer.writeheader()
            try:
                import csv as _csv

                summary_writer = _csv.DictWriter(summary_handle, fieldnames=summary_columns)
                if summary_mode == "w":
                    summary_writer.writeheader()

                processed = processed_before_resume
                packages_done = packages_done_before_resume
                _pool_broken = False
                _pending_futures = set(futures)
                _HEARTBEAT_SECS = 120
                if args.package_timeout is not None:
                    _HEARTBEAT_SECS = min(120, max(10, args.package_timeout // 2))
                while _pending_futures:
                    _done_futures, _pending_futures = wait(
                        _pending_futures, timeout=_HEARTBEAT_SECS, return_when=FIRST_COMPLETED
                    )
                    if args.package_timeout is not None:
                        _now = time.monotonic()
                        _timed_out = {
                            f
                            for f in _pending_futures
                            if _now - future_submit_time[f] > args.package_timeout
                        }
                        for f in _timed_out:
                            _pkg_label = future_to_pkg.get(f, "unknown")
                            logging.getLogger("dependency_metrics").warning(
                                "Package %s timed out after %ds — writing error row and skipping.",
                                _pkg_label,
                                args.package_timeout,
                            )
                            for trow in future_to_rows.get(f, []):
                                err_result = _make_timeout_result(
                                    trow,
                                    args.package_timeout,
                                    args.severity_breakdown,
                                    args.per_release,
                                )
                                summary_writer.writerow(err_result["summary"])
                                summary_handle.flush()
                                processed += 1
                        _pending_futures -= _timed_out
                    if not _done_futures and not _pending_futures:
                        break
                    if not _done_futures:
                        _in_flight = [
                            future_to_pkg[f] for f in _pending_futures if f in future_to_pkg
                        ]
                        logging.getLogger("dependency_metrics").warning(
                            "Still waiting for %d package(s): %s",
                            len(_pending_futures),
                            ", ".join(_in_flight[:20]),
                        )
                        continue
                    for future in _done_futures:
                        try:
                            results = future.result()
                        except BrokenProcessPool:
                            _pool_broken = True
                            logging.getLogger("dependency_metrics").error(
                                "Worker process killed (likely OOM). "
                                "Partial results flushed. Re-run with --resume to continue."
                            )
                            sys.stderr.write(
                                "\nERROR: A worker process was killed (likely out of memory).\n"
                                "Partial results saved. Re-run with --resume to continue.\n\n"
                            )
                            _pending_futures.clear()
                            break

                        if args.per_release:
                            packages_done += 1
                            if packages_done % 10 == 0:
                                _rss_mb = psutil.Process().memory_info().rss / 1024 / 1024
                                logging.getLogger("dependency_metrics").warning(
                                    "Memory after %d packages: %.0f MB RSS", packages_done, _rss_mb
                                )
                            if packages_done % 50 == 0:
                                gc.collect()
                            if results:
                                first_summary = results[0]["summary"]
                                first_row_num = results[0]["row_num"]
                                logging.getLogger("dependency_metrics").warning(
                                    "Completed %s/%s (CSV line %s): %s %s (%s release points)",
                                    packages_done,
                                    total_unique_packages,
                                    first_row_num,
                                    first_summary["ecosystem"],
                                    first_summary["package_name"],
                                    len(results),
                                )
                        else:
                            for result in results:
                                processed += 1
                                logging.getLogger("dependency_metrics").warning(
                                    "Processing row %s/%s (CSV line %s): %s %s",
                                    processed,
                                    total_rows,
                                    result["row_num"],
                                    result["summary"]["ecosystem"],
                                    result["summary"]["package_name"],
                                )
                                if processed % 10 == 0:
                                    _rss_mb = psutil.Process().memory_info().rss / 1024 / 1024
                                    logging.getLogger("dependency_metrics").warning(
                                        "Memory after %d rows: %.0f MB RSS", processed, _rss_mb
                                    )
                                if processed % 50 == 0:
                                    gc.collect()

                        pending_dep_frames: list = []
                        for result in results:
                            if result["summary"]["status"] == "error":
                                logging.getLogger("dependency_metrics").error(
                                    "Error (CSV line %s): %s",
                                    result["row_num"],
                                    result["summary"]["error"],
                                )
                            # Skip already-written per-release entries in resume mode
                            if args.per_release and args.resume and existing_per_release:
                                release_key = (
                                    result["summary"]["ecosystem"].lower(),
                                    result["summary"]["package_name"].lower(),
                                    result["summary"]["window_start"],
                                    result["summary"]["package_version"],
                                )
                                if release_key in existing_per_release:
                                    continue
                            summary_writer.writerow(result["summary"])
                            if args.write_dependency_details:
                                pending_dep_frames.extend(result["dependency_frames"])

                        # Flush once per package to preserve results on interruption
                        if pending_dep_frames:
                            import pandas as _pd

                            combined = _pd.concat(pending_dep_frames, ignore_index=True)
                            # Workaround: pyarrow ChunkedArray.to_numpy() raises "Unknown error:
                            # Wrapping" for nullable string columns. Convert via list() instead.
                            arrow_cols = [
                                c
                                for c in combined.columns
                                if isinstance(combined[c].dtype, _pd.ArrowDtype)
                            ]
                            if arrow_cols:
                                combined = combined.assign(
                                    **{c: list(combined[c]) for c in arrow_cols}
                                )
                            combined.to_csv(
                                deps_file_path,
                                mode="a",
                                header=not deps_header_written,
                                index=False,
                            )
                            deps_header_written = True
                        summary_handle.flush()
                        if args.per_release and ledger_writer is not None and results:
                            group_statuses = {r["summary"].get("status", "error") for r in results}
                            group_status = "ok" if "ok" in group_statuses else "error"
                            first = results[0]["summary"]
                            ledger_writer.writerow(
                                {
                                    "ecosystem": first.get("ecosystem", "").lower(),
                                    "package_name": first.get("package_name", "").lower(),
                                    "window_start": first.get("window_start", ""),
                                    "window_end": max(
                                        r["summary"].get("window_end", "") for r in results
                                    ),
                                    "status": group_status,
                                }
                            )
                            ledger_handle.flush()
            finally:
                summary_handle.close()
                if ledger_handle is not None:
                    ledger_handle.close()

        if _pool_broken:
            sys.exit(1)

        # Clean up the OSV index temp file now that all workers are done.
        try:
            os.unlink(_osv_index_tmp_path)
        except OSError:
            pass

        logging.getLogger("dependency_metrics").info("Bulk results saved to: %s", summary_file_path)
        if deps_header_written:
            logging.getLogger("dependency_metrics").info(
                "Dependency details saved to: %s", deps_file_path
            )

    else:
        if not args.ecosystem or not args.package:
            parser.error("--ecosystem and --package are required unless --input-csv is provided.")

        # Parse end date for single package
        if args.end_date:
            try:
                end_date = _parse_date(args.end_date, "end_date")
            except ValueError as exc:
                logging.getLogger("dependency_metrics").error("Error: %s", exc)
                sys.exit(1)
        else:
            end_date = datetime.today()

        start_date = default_start_date

        # Build OSV database if requested
        if args.build_osv:
            logging.getLogger("dependency_metrics").info("Building OSV vulnerability database...")
            osv_builder = OSVBuilder(output_dir)
            osv_df = osv_builder.build_database()
            logging.getLogger("dependency_metrics").info(
                "OSV database built with %s records", len(osv_df)
            )

        # Analyze package dependencies
        logging.getLogger("dependency_metrics").info(
            "Analyzing %s package: %s", args.ecosystem, args.package
        )
        _single_resolver = None
        if args.depsdev:
            _single_cache = ResolverCache(cache_dir=output_dir / "cache")
            _single_client = DepsDevClient(cache=_single_cache)
            _single_resolver = DepsDevResolver(
                system={"npm": "NPM", "pypi": "PYPI", "cargo": "CARGO"}.get(
                    args.ecosystem, args.ecosystem.upper()
                ),
                package=args.package,
                start_date=start_date,
                end_date=end_date,
                client=_single_client,
            )
        analyzer = DependencyAnalyzer(
            ecosystem=args.ecosystem,
            package=args.package,
            start_date=start_date,
            end_date=end_date,
            weighting_type=args.weighting_type,
            half_life=args.half_life,
            output_dir=output_dir,
            resolver=_single_resolver,
        )

        try:
            _logger = logging.getLogger("dependency_metrics")
            if args.per_release:
                row = {"start_date": start_date, "end_date": end_date}
                release_results = analyzer.analyze_at_release_points(
                    row, generate_dep_frames=args.get_worksheets
                )
                if not release_results:
                    _logger.info("No releases found in window for %s", args.package)
                else:
                    _logger.info("\n" + "=" * 60)
                    _logger.info("PER-RELEASE ANALYSIS RESULTS")
                    _logger.info("=" * 60)
                    _logger.info("Package: %s", args.package)
                    _logger.info("Ecosystem: %s", args.ecosystem)
                    _logger.info("Period: %s to %s", start_date.date(), end_date.date())
                    _logger.info("-" * 60)
                    for r in release_results:
                        s = r["summary"]
                        mttr = s.get("mttr", s.get("mttr_all_severities", 0.0))
                        _logger.info(
                            "  v%-20s  MTTU=%.2f  MTTR=%.2f  deps=%s  status=%s",
                            s.get("package_version", ""),
                            s.get("mttu", 0.0),
                            mttr,
                            s.get("num_dependencies", 0),
                            s.get("status", ""),
                        )
                    _logger.info("=" * 60)

                    summary_df = pd.DataFrame([r["summary"] for r in release_results])
                    csv_path = (
                        output_dir / f"{safe_filename_stem(args.package)}_per_release_results.csv"
                    )
                    summary_df.to_csv(csv_path, index=False)
                    _logger.info("Per-release results saved to: %s", csv_path)

                    if args.get_worksheets:
                        excel_file = export_per_release_worksheets(
                            release_results,
                            output_dir,
                            args.package,
                        )
                        if excel_file is not None:
                            _logger.info("Per-release worksheets saved to: %s", excel_file)
            else:
                results = analyzer.analyze()

                # Output results
                print_summary(
                    package=args.package,
                    ecosystem=args.ecosystem,
                    start_date=start_date,
                    end_date=end_date,
                    weighting_type=args.weighting_type,
                    half_life=args.half_life,
                    results=results,
                )

                results_file = save_results_json(results, output_dir, args.package)
                _logger.info("Results saved to: %s", results_file)

                # Export OSV data if requested
                if args.get_osv:
                    osv_file = export_osv_data(results, output_dir, args.package)
                    if osv_file is not None:
                        _logger.info("OSV data saved to: %s", osv_file)

                # Export worksheets if requested
                if args.get_worksheets:
                    excel_file = export_worksheets(results, output_dir, args.package)
                    if excel_file is not None:
                        _logger.info("Worksheets saved to: %s", excel_file)

        except Exception as e:
            logging.getLogger("dependency_metrics").error("Error during analysis: %s", e)
            import traceback

            traceback.print_exc()
            sys.exit(1)


if __name__ == "__main__":
    main()
