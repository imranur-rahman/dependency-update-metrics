"""
Reporting and export utilities.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd


def print_summary(
    package: str,
    ecosystem: str,
    start_date,
    end_date,
    weighting_type: str,
    half_life,
    results: Dict,
) -> None:
    print("\n" + "=" * 60)
    print("ANALYSIS RESULTS")
    print("=" * 60)
    print(f"Package: {package}")
    print(f"Ecosystem: {ecosystem}")
    print(f"Period: {start_date.date()} to {end_date.date()}")
    print(f"Weighting: {weighting_type}")
    if weighting_type == "exponential":
        print(f"Half-life: {half_life} days")
    print("-" * 60)
    print(f"Average Time-to-Update (TTU): {results['ttu']:.2f} days")
    print(f"Average Time-to-Remediate (TTR): {results['ttr']:.2f} days")
    print(f"Number of dependencies: {results['num_dependencies']}")
    print("=" * 60)


def save_results_json(results: Dict, output_dir: Path, package: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    results_file = output_dir / f"{package}_results.json"
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    return results_file


def export_osv_data(results: Dict, output_dir: Path, package: str) -> Path | None:
    if 'osv_data' not in results:
        return None
    osv_file = output_dir / f"{package}_osv.csv"
    results['osv_data'].to_csv(osv_file, index=False)
    return osv_file


def export_worksheets(results: Dict, output_dir: Path, package: str) -> Path | None:
    if 'dependency_data' not in results:
        return None
    excel_file = output_dir / f"{package}_worksheets.xlsx"
    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
        for dep_name, dep_df in results['dependency_data'].items():
            sheet_name = dep_name[:31]

            df_copy = dep_df.copy()
            for col in df_copy.columns:
                if pd.api.types.is_datetime64tz_dtype(df_copy[col]):
                    df_copy[col] = df_copy[col].dt.tz_convert('UTC').dt.tz_localize(None)

            df_copy.to_excel(writer, sheet_name=sheet_name, index=False)
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
