"""
Reporting and export utilities.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List
import logging

import pandas as pd


def safe_filename_stem(name: str) -> str:
    """Replace path separators in a name (e.g. scoped npm packages like
    "@scope/pkg") so it can be used as a single filename component instead of
    being interpreted as a (non-existent) subdirectory by Path / to_csv."""
    return name.replace("/", "__").replace("\\", "__")


def print_summary(
    package: str,
    ecosystem: str,
    start_date,
    end_date,
    weighting_type: str,
    half_life,
    results: Dict,
) -> None:
    logger.info("\n" + "=" * 60)
    logger.info("ANALYSIS RESULTS")
    logger.info("=" * 60)
    logger.info("Package: %s", package)
    logger.info("Ecosystem: %s", ecosystem)
    logger.info("Period: %s to %s", start_date.date(), end_date.date())
    logger.info("Weighting: %s", weighting_type)
    if weighting_type == "exponential":
        logger.info("Half-life: %s days", half_life)
    logger.info("-" * 60)
    logger.info("Average Time-to-Update (TTU): %.2f days", results["ttu"])
    logger.info("Average Time-to-Remediate (TTR): %.2f days", results["ttr"])
    logger.info("Number of dependencies: %s", results["num_dependencies"])
    logger.info("=" * 60)


def save_results_json(results: Dict, output_dir: Path, package: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    results_file = output_dir / f"{safe_filename_stem(package)}_results.json"
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2, default=str)
    return results_file


def export_osv_data(results: Dict, output_dir: Path, package: str) -> Path | None:
    if "osv_data" not in results:
        return None
    osv_file = output_dir / f"{safe_filename_stem(package)}_osv.csv"
    results["osv_data"].to_csv(osv_file, index=False)
    return osv_file


def export_worksheets(results: Dict, output_dir: Path, package: str) -> Path | None:
    if "dependency_data" not in results:
        return None
    excel_file = output_dir / f"{safe_filename_stem(package)}_worksheets.xlsx"
    with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
        for dep_name, dep_df in results["dependency_data"].items():
            sheet_name = dep_name[:31]

            df_copy = dep_df.copy()
            for col in df_copy.columns:
                if pd.api.types.is_datetime64tz_dtype(df_copy[col]):
                    df_copy[col] = df_copy[col].dt.tz_convert("UTC").dt.tz_localize(None)

            df_copy.to_excel(writer, sheet_name=sheet_name, index=False)
    return excel_file


def export_per_release_worksheets(
    release_results: List[Dict],
    output_dir: Path,
    package: str,
    regular_dep_data: "Dict | None" = None,
) -> "Path | None":
    """Export per-release results to Excel.

    Sheets:
    - "Summary": per-release metrics table
    - "<dep>": full-window dependency analysis (when regular_dep_data is supplied)
    - "<dep>" or "<dep>_PR": per-release dependency frames (suffix added on name clash)
    """
    summaries = [r["summary"] for r in release_results if r.get("summary")]
    if not summaries:
        return None

    per_release_frames: Dict[str, List] = {}
    for r in release_results:
        for df in r.get("dependency_frames", []):
            if df is None or df.empty:
                continue
            dep_name = df["dependency"].iloc[0] if "dependency" in df.columns else "unknown"
            per_release_frames.setdefault(dep_name, []).append(df)

    excel_file = output_dir / f"{safe_filename_stem(package)}_per_release_worksheets.xlsx"
    with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
        pd.DataFrame(summaries).to_excel(writer, sheet_name="Summary", index=False)

        if regular_dep_data:
            for dep_name, dep_df in regular_dep_data.items():
                df_copy = dep_df.copy()
                for col in df_copy.columns:
                    if pd.api.types.is_datetime64tz_dtype(df_copy[col]):
                        df_copy[col] = df_copy[col].dt.tz_convert("UTC").dt.tz_localize(None)
                df_copy.to_excel(writer, sheet_name=dep_name[:31], index=False)

        regular_names = set((regular_dep_data or {}).keys())
        for dep_name, frames in per_release_frames.items():
            combined = pd.concat(frames, ignore_index=True)
            for col in combined.columns:
                if pd.api.types.is_datetime64tz_dtype(combined[col]):
                    combined[col] = combined[col].dt.tz_convert("UTC").dt.tz_localize(None)
            if dep_name in regular_names:
                sheet_name = (dep_name[:28] + "_PR")[:31]
            else:
                sheet_name = dep_name[:31]
            combined.to_excel(writer, sheet_name=sheet_name, index=False)
    return excel_file


def export_bulk_summary_csv(
    rows: Iterable[Dict],
    output_dir: Path,
    input_csv: Path,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_file = output_dir / f"{input_csv.stem}_bulk_results.csv"
    df = pd.DataFrame(list(rows))
    columns = [
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
    df.to_csv(summary_file, index=False, columns=columns)
    return summary_file


def export_bulk_dependency_csv(
    dependency_frames: List[pd.DataFrame],
    output_dir: Path,
    input_csv: Path,
) -> Path | None:
    if not dependency_frames:
        return None
    output_dir.mkdir(parents=True, exist_ok=True)
    deps_file = output_dir / f"{input_csv.stem}_dependency_details.csv"
    df = pd.concat(dependency_frames, ignore_index=True)
    for col in df.columns:
        if pd.api.types.is_datetime64tz_dtype(df[col]):
            df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)
    df.to_csv(deps_file, index=False)
    return deps_file


def export_per_release_summary_csv(
    rows: Iterable[Dict],
    output_dir: Path,
    input_csv: Path,
) -> Path:
    """Write per-release summary CSV with extended columns."""
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_file = output_dir / f"{input_csv.stem}_per_release_results.csv"
    df = pd.DataFrame(list(rows))
    columns = [
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
    df.to_csv(summary_file, index=False, columns=columns)
    return summary_file


def export_per_release_dependency_csv(
    dependency_frames: List[pd.DataFrame],
    output_dir: Path,
    input_csv: Path,
) -> "Path | None":
    """Concatenate and write per-release dependency detail frames."""
    if not dependency_frames:
        return None
    output_dir.mkdir(parents=True, exist_ok=True)
    deps_file = output_dir / f"{input_csv.stem}_per_release_dependency_details.csv"
    df = pd.concat(dependency_frames, ignore_index=True)
    for col in df.columns:
        if pd.api.types.is_datetime64tz_dtype(df[col]):
            df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)
    df.to_csv(deps_file, index=False)
    return deps_file


logger = logging.getLogger(__name__)
