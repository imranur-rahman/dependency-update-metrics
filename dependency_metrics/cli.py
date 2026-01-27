"""
Command-line interface for the dependency metrics tool.
"""

import argparse
import csv
import io
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .analyzer import DependencyAnalyzer
from .osv_builder import OSVBuilder
from .resolvers import ResolverCache
from .reporting import (
    export_osv_data,
    export_bulk_dependency_csv,
    export_bulk_summary_csv,
    export_worksheets,
    print_summary,
    save_results_json,
)

def _parse_date(value: str, field: str, row_num: Optional[int] = None) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        location = f" on row {row_num}" if row_num is not None else ""
        raise ValueError(f"Invalid {field} format{location}. Use YYYY-MM-DD.")


def _load_input_csv(path: Path) -> List[Dict[str, object]]:
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
    required = {"ecosystem", "package_name", "end_date"}
    missing = required.difference(field_map.keys())
    if missing:
        available = ", ".join(normalized_fields) if normalized_fields else "none"
        raise ValueError(
            f"Input CSV missing required columns: {', '.join(sorted(missing))}. "
            f"Found columns: {available}."
        )

    rows: List[Dict[str, object]] = []
    for idx, record in df.iterrows():
        row_num = int(idx) + 2
        cleaned = {str(k).strip().lstrip("\ufeff"): ("" if pd.isna(v) else str(v).strip()) for k, v in record.items()}
        normalized = {
            "ecosystem": cleaned.get(field_map["ecosystem"], ""),
            "package_name": cleaned.get(field_map["package_name"], ""),
            "end_date": cleaned.get(field_map["end_date"], ""),
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
        choices=["npm", "pypi"],
        help="The ecosystem to analyze (npm or pypi)"
    )

    parser.add_argument(
        "--package",
        help="The name of the package to analyze"
    )

    parser.add_argument(
        "--input-csv",
        help="CSV file with columns: ecosystem, package_name, end_date, optional start_date"
    )
    
    parser.add_argument(
        "--start-date",
        default="1900-01-01",
        help="Start date for analysis (YYYY-MM-DD). Default: 1900-01-01"
    )
    
    parser.add_argument(
        "--end-date",
        default=None,
        help="End date for analysis (YYYY-MM-DD). Default: today"
    )
    
    parser.add_argument(
        "--weighting-type",
        choices=["linear", "exponential", "inverse", "disable"],
        default="disable",
        help="Type of weighting to apply. Default: disable"
    )
    
    parser.add_argument(
        "--half-life",
        type=float,
        default=None,
        help="Half-life in days (required for exponential weighting)"
    )
    
    parser.add_argument(
        "--build-osv",
        action="store_true",
        help="Build the OSV vulnerability database"
    )
    
    parser.add_argument(
        "--get-osv",
        action="store_true",
        help="Return the OSV dataset for the ecosystem and vulnerable dependencies"
    )
    
    parser.add_argument(
        "--get-worksheets",
        action="store_true",
        help="Export dependency dataframes to an Excel file with multiple sheets"
    )
    
    parser.add_argument(
        "--output-dir",
        default="./output",
        help="Output directory for results. Default: ./output"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging"
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.weighting_type == "exponential" and args.half_life is None:
        parser.error("--half-life is required when --weighting-type is exponential")
    
    # Parse default start date
    try:
        default_start_date = _parse_date(args.start_date, "start_date")
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Configure logging
    if args.verbose:
        import logging
        logging.basicConfig(level=logging.INFO)
        logging.getLogger("dependency_metrics").setLevel(logging.DEBUG)

    if args.input_csv:
        input_csv = Path(args.input_csv)
        if not input_csv.exists():
            print(f"Error: Input CSV not found: {input_csv}", file=sys.stderr)
            sys.exit(1)

        try:
            input_rows = _load_input_csv(input_csv)
        except ValueError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            sys.exit(1)

        # Build OSV database automatically if missing
        osv_builder = OSVBuilder(output_dir)
        osv_df = osv_builder.build_database()
        ecosystems = sorted({row["ecosystem"].lower() for row in input_rows if row.get("ecosystem")})
        osv_by_ecosystem: Dict[str, object] = {}
        for ecosystem in ecosystems:
            if len(osv_df) > 0 and "ecosystem" in osv_df.columns:
                osv_by_ecosystem[ecosystem] = osv_df[osv_df["ecosystem"] == ecosystem.upper()].copy()
            else:
                osv_by_ecosystem[ecosystem] = osv_df

        resolver_cache = ResolverCache()
        summary_rows: List[Dict[str, object]] = []
        dependency_frames = []

        for row in input_rows:
            row_num = row.get("_row_num")
            ecosystem = row.get("ecosystem", "").lower()
            package_name = row.get("package_name", "")
            end_date_raw = row.get("end_date", "")
            start_date_raw = row.get("start_date", "")

            status = "ok"
            error = ""
            mttu = 0.0
            mttr = 0.0
            num_dependencies = 0

            try:
                if not ecosystem or not package_name or not end_date_raw:
                    raise ValueError("ecosystem, package_name, and end_date are required.")
                if ecosystem not in {"npm", "pypi"}:
                    raise ValueError(f"Unsupported ecosystem: {ecosystem}.")

                start_date = default_start_date
                if start_date_raw:
                    start_date = _parse_date(start_date_raw, "start_date", row_num)
                end_date = _parse_date(end_date_raw, "end_date", row_num)

                analyzer = DependencyAnalyzer(
                    ecosystem=ecosystem,
                    package=package_name,
                    start_date=start_date,
                    end_date=end_date,
                    weighting_type=args.weighting_type,
                    half_life=args.half_life,
                    output_dir=output_dir,
                    resolver_cache=resolver_cache,
                )

                results = analyzer.analyze(osv_df=osv_by_ecosystem.get(ecosystem))
                mttu = results.get("ttu", 0.0)
                mttr = results.get("ttr", 0.0)
                num_dependencies = results.get("num_dependencies", 0)

                dep_data = results.get("dependency_data", {})
                for dep_df in dep_data.values():
                    dependency_frames.append(dep_df)

            except Exception as exc:
                status = "error"
                error = str(exc)

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

            summary_rows.append({
                "ecosystem": ecosystem,
                "package_name": package_name,
                "start_date": start_date.date().isoformat(),
                "end_date": end_date.date().isoformat(),
                "mttu": mttu,
                "mttr": mttr,
                "num_dependencies": num_dependencies,
                "status": status,
                "error": error,
            })

        summary_file = export_bulk_summary_csv(summary_rows, output_dir, input_csv)
        print(f"Bulk results saved to: {summary_file}")

        deps_file = export_bulk_dependency_csv(dependency_frames, output_dir, input_csv)
        if deps_file is not None:
            print(f"Dependency details saved to: {deps_file}")

    else:
        if not args.ecosystem or not args.package:
            parser.error("--ecosystem and --package are required unless --input-csv is provided.")

        # Parse end date for single package
        if args.end_date:
            try:
                end_date = _parse_date(args.end_date, "end_date")
            except ValueError as exc:
                print(f"Error: {exc}", file=sys.stderr)
                sys.exit(1)
        else:
            end_date = datetime.today()

        start_date = default_start_date

        # Build OSV database if requested
        if args.build_osv:
            print("Building OSV vulnerability database...")
            osv_builder = OSVBuilder(output_dir)
            osv_df = osv_builder.build_database()
            print(f"OSV database built with {len(osv_df)} records")

        # Analyze package dependencies
        print(f"Analyzing {args.ecosystem} package: {args.package}")
        analyzer = DependencyAnalyzer(
            ecosystem=args.ecosystem,
            package=args.package,
            start_date=start_date,
            end_date=end_date,
            weighting_type=args.weighting_type,
            half_life=args.half_life,
            output_dir=output_dir,
        )

        try:
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
            print(f"\nResults saved to: {results_file}")

            # Export OSV data if requested
            if args.get_osv:
                osv_file = export_osv_data(results, output_dir, args.package)
                if osv_file is not None:
                    print(f"OSV data saved to: {osv_file}")

            # Export worksheets if requested
            if args.get_worksheets:
                excel_file = export_worksheets(results, output_dir, args.package)
                if excel_file is not None:
                    print(f"Worksheets saved to: {excel_file}")

        except Exception as e:
            print(f"\nError during analysis: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)


if __name__ == "__main__":
    main()
