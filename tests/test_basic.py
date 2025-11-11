"""Tests for the dependency_metrics package."""

import pytest
from datetime import datetime
from pathlib import Path
import tempfile


def test_package_import():
    """Test that the package can be imported."""
    import dependency_metrics
    assert dependency_metrics.__version__ == "0.1.0"


def test_cli_import():
    """Test that CLI module can be imported."""
    from dependency_metrics.cli import main
    assert callable(main)


def test_analyzer_import():
    """Test that analyzer module can be imported."""
    from dependency_metrics.analyzer import DependencyAnalyzer
    assert DependencyAnalyzer is not None


def test_osv_builder_import():
    """Test that OSV builder module can be imported."""
    from dependency_metrics.osv_builder import OSVBuilder
    assert OSVBuilder is not None


def test_analyzer_initialization():
    """Test that analyzer can be initialized."""
    from dependency_metrics.analyzer import DependencyAnalyzer
    
    with tempfile.TemporaryDirectory() as tmpdir:
        analyzer = DependencyAnalyzer(
            ecosystem="npm",
            package="test-package",
            start_date=datetime(2020, 1, 1),
            end_date=datetime(2023, 12, 31),
            output_dir=Path(tmpdir)
        )
        
        assert analyzer.ecosystem == "npm"
        assert analyzer.package == "test-package"
        assert analyzer.weighting_type == "disable"


def test_osv_builder_initialization():
    """Test that OSV builder can be initialized."""
    from dependency_metrics.osv_builder import OSVBuilder
    
    with tempfile.TemporaryDirectory() as tmpdir:
        builder = OSVBuilder(Path(tmpdir))
        
        assert builder.output_dir == Path(tmpdir)
        assert builder.osv_dir == Path(tmpdir) / "osv-data"


def test_semver_transformation():
    """Test semver transformation function."""
    from dependency_metrics.osv_builder import OSVBuilder
    
    with tempfile.TemporaryDirectory() as tmpdir:
        builder = OSVBuilder(Path(tmpdir))
        
        assert builder.transformation_semver("0") == "0.0.0"
        assert builder.transformation_semver("1") == "1.0.0"
        assert builder.transformation_semver("1.2") == "1.2.0"
        assert builder.transformation_semver("1.2.3") == "1.2.3"


def test_weight_calculation_disable():
    """Test weight calculation with disable mode."""
    from dependency_metrics.analyzer import DependencyAnalyzer
    
    with tempfile.TemporaryDirectory() as tmpdir:
        analyzer = DependencyAnalyzer(
            ecosystem="npm",
            package="test",
            start_date=datetime(2020, 1, 1),
            end_date=datetime(2023, 12, 31),
            weighting_type="disable",
            output_dir=Path(tmpdir)
        )
        
        assert analyzer.calculate_weight(0) == 1.0
        assert analyzer.calculate_weight(100) == 1.0


def test_weight_calculation_exponential():
    """Test weight calculation with exponential mode."""
    from dependency_metrics.analyzer import DependencyAnalyzer
    import math
    
    with tempfile.TemporaryDirectory() as tmpdir:
        analyzer = DependencyAnalyzer(
            ecosystem="npm",
            package="test",
            start_date=datetime(2020, 1, 1),
            end_date=datetime(2023, 12, 31),
            weighting_type="exponential",
            half_life=180,
            output_dir=Path(tmpdir)
        )
        
        # At half-life, weight should be 0.5
        weight = analyzer.calculate_weight(180)
        assert abs(weight - 0.5) < 0.01
        
        # At 0 days, weight should be 1.0
        assert analyzer.calculate_weight(0) == 1.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
