"""
Command-line interface for the dependency metrics tool.
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

from .analyzer import DependencyAnalyzer
from .osv_builder import OSVBuilder


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Analyze dependency update and remediation metrics for packages"
    )
    
    parser.add_argument(
        "--ecosystem",
        required=True,
        choices=["npm", "pypi"],
        help="The ecosystem to analyze (npm or pypi)"
    )
    
    parser.add_argument(
        "--package",
        required=True,
        help="The name of the package to analyze"
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
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.weighting_type == "exponential" and args.half_life is None:
        parser.error("--half-life is required when --weighting-type is exponential")
    
    # Parse dates
    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    except ValueError:
        print(f"Error: Invalid start-date format. Use YYYY-MM-DD", file=sys.stderr)
        sys.exit(1)
    
    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
        except ValueError:
            print(f"Error: Invalid end-date format. Use YYYY-MM-DD", file=sys.stderr)
            sys.exit(1)
    else:
        end_date = datetime.today()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
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
        output_dir=output_dir
    )
    
    try:
        results = analyzer.analyze()
        
        # Output results
        print("\n" + "="*60)
        print("ANALYSIS RESULTS")
        print("="*60)
        print(f"Package: {args.package}")
        print(f"Ecosystem: {args.ecosystem}")
        print(f"Period: {start_date.date()} to {end_date.date()}")
        print(f"Weighting: {args.weighting_type}")
        if args.weighting_type == "exponential":
            print(f"Half-life: {args.half_life} days")
        print("-"*60)
        print(f"Average Time-to-Update (TTU): {results['ttu']:.2f} days")
        print(f"Average Time-to-Remediate (TTR): {results['ttr']:.2f} days")
        print(f"Number of dependencies: {results['num_dependencies']}")
        print("="*60)
        
        # Save results to JSON
        results_file = output_dir / f"{args.package}_results.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nResults saved to: {results_file}")
        
        # Export OSV data if requested
        if args.get_osv and 'osv_data' in results:
            osv_file = output_dir / f"{args.package}_osv.csv"
            results['osv_data'].to_csv(osv_file, index=False)
            print(f"OSV data saved to: {osv_file}")
        
        # Export worksheets if requested
        if args.get_worksheets and 'dependency_data' in results:
            excel_file = output_dir / f"{args.package}_worksheets.xlsx"
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                for dep_name, dep_df in results['dependency_data'].items():
                    # Excel sheet names have a 31 character limit
                    sheet_name = dep_name[:31]
                    dep_df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"Worksheets saved to: {excel_file}")
        
    except Exception as e:
        print(f"\nError during analysis: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
