#!/usr/bin/env python3
"""
Example script showing how to use the dependency-metrics tool.
"""

from datetime import datetime
from pathlib import Path
from dependency_metrics.analyzer import DependencyAnalyzer
from dependency_metrics.osv_builder import OSVBuilder


def example_basic_analysis():
    """Example: Basic analysis without weighting."""
    print("="*60)
    print("Example 1: Basic Analysis")
    print("="*60)
    
    analyzer = DependencyAnalyzer(
        ecosystem="npm",
        package="express",
        start_date=datetime(2022, 1, 1),
        end_date=datetime(2023, 12, 31),
        weighting_type="disable",
        output_dir=Path("./output/example1")
    )
    
    results = analyzer.analyze()
    
    print(f"\nPackage: {results['package']}")
    print(f"Version analyzed: {results['version']}")
    print(f"Number of dependencies: {results['num_dependencies']}")
    print(f"Average Time-to-Update: {results['ttu']:.2f} days")
    print(f"Average Time-to-Remediate: {results['ttr']:.2f} days")


def example_weighted_analysis():
    """Example: Analysis with exponential weighting."""
    print("\n" + "="*60)
    print("Example 2: Weighted Analysis (Exponential)")
    print("="*60)
    
    analyzer = DependencyAnalyzer(
        ecosystem="npm",
        package="express",
        start_date=datetime(2022, 1, 1),
        end_date=datetime(2023, 12, 31),
        weighting_type="exponential",
        half_life=180,  # 6 months
        output_dir=Path("./output/example2")
    )
    
    results = analyzer.analyze()
    
    print(f"\nPackage: {results['package']}")
    print(f"Version analyzed: {results['version']}")
    print(f"Weighting: {results['weighting_type']} (half-life: {results['half_life']} days)")
    print(f"Number of dependencies: {results['num_dependencies']}")
    print(f"Average Time-to-Update: {results['ttu']:.2f} days")
    print(f"Average Time-to-Remediate: {results['ttr']:.2f} days")


def example_build_osv():
    """Example: Build OSV vulnerability database."""
    print("\n" + "="*60)
    print("Example 3: Build OSV Database")
    print("="*60)
    
    builder = OSVBuilder(Path("./output"))
    
    print("\nBuilding OSV database (this may take several minutes)...")
    osv_df = builder.build_database()
    
    print(f"OSV database built with {len(osv_df)} vulnerability records")
    print(f"\nEcosystems in database:")
    print(osv_df['ecosystem'].value_counts())


def example_pypi_analysis():
    """Example: Analyze a PyPI package."""
    print("\n" + "="*60)
    print("Example 4: PyPI Package Analysis")
    print("="*60)
    
    analyzer = DependencyAnalyzer(
        ecosystem="pypi",
        package="requests",
        start_date=datetime(2022, 1, 1),
        end_date=datetime(2023, 12, 31),
        weighting_type="linear",
        output_dir=Path("./output/example4")
    )
    
    results = analyzer.analyze()
    
    print(f"\nPackage: {results['package']}")
    print(f"Ecosystem: {results['ecosystem']}")
    print(f"Version analyzed: {results['version']}")
    print(f"Number of dependencies: {results['num_dependencies']}")
    print(f"Average Time-to-Update: {results['ttu']:.2f} days")
    print(f"Average Time-to-Remediate: {results['ttr']:.2f} days")


if __name__ == "__main__":
    import sys
    
    print("Dependency Metrics - Example Usage")
    print("="*60)
    print("\nNOTE: These examples require network access and may take several minutes.")
    print("Make sure you have npm CLI installed for npm ecosystem analysis.")
    
    # Run examples
    try:
        # Example 1: Basic analysis
        example_basic_analysis()
        
        # Example 2: Weighted analysis
        example_weighted_analysis()
        
        # Example 3: Build OSV database (commented out by default as it's time-consuming)
        # example_build_osv()
        
        # Example 4: PyPI analysis
        example_pypi_analysis()
        
        print("\n" + "="*60)
        print("Examples completed successfully!")
        print("Check the ./output directory for detailed results.")
        print("="*60)
        
    except Exception as e:
        print(f"\nError running examples: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
