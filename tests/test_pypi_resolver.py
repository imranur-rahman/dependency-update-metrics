from datetime import datetime


def test_extract_dependencies_parses_requires_dist():
    from dependency_metrics.analyzer import DependencyAnalyzer

    analyzer = DependencyAnalyzer(
        ecosystem="pypi",
        package="demo",
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 2),
    )
    version_data = {
        "requires_dist": [
            "requests>=2.0",
            "urllib3 (>=1.26); python_version < '4'",
            "numpy[extra]>=1.0",
            "pandas",
        ]
    }

    deps = analyzer.extract_dependencies(version_data)

    assert deps == {"requests": ">=2.0", "urllib3": ">=1.26", "pandas": "*"}


def test_pypi_resolver_returns_best_candidate(monkeypatch):
    from dependency_metrics import pypi_resolver

    class FakeCandidate:
        def __init__(self, version: str) -> None:
            self.version = version

    class FakeResult:
        def __init__(self, version: str) -> None:
            self.best_candidate = FakeCandidate(version)

    class FakeFinder:
        def __init__(self) -> None:
            self.calls = []

        def find_best_candidate(self, project_name, specifier=None, hashes=None):
            self.calls.append((project_name, specifier, hashes))
            return FakeResult("2.0.0")

    captured = {}

    def fake_get_finder(self, before):
        captured["before"] = before
        return FakeFinder()

    monkeypatch.setattr(pypi_resolver.PyPIResolver, "_get_finder", fake_get_finder)

    resolver = pypi_resolver.PyPIResolver()
    version = resolver.resolve("demo", ">=1.0", datetime(2020, 1, 1))

    assert version == "2.0.0"
    assert captured["before"].tzinfo is not None


def test_pypi_analyzer_produces_intervals_with_stubbed_versions(monkeypatch):
    from dependency_metrics.analyzer import DependencyAnalyzer

    analyzer = DependencyAnalyzer(
        ecosystem="pypi",
        package="demo",
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 10),
    )

    pkg_metadata = {
        "releases": {
            "1.0.0": [{"upload_time": "2020-01-01T00:00:00Z"}],
            "1.1.0": [{"upload_time": "2020-01-05T00:00:00Z"}],
        }
    }
    dep_metadata = {
        "releases": {
            "0.9.0": [{"upload_time": "2019-12-15T00:00:00Z"}],
            "1.0.0": [{"upload_time": "2020-01-03T00:00:00Z"}],
        }
    }

    def fake_get_deps(package, version):
        return {"dep": ">=0.9.0"}

    from datetime import timezone
    threshold = datetime(2020, 1, 3, tzinfo=timezone.utc)

    def fake_highest(dep, at_date, metadata=None):
        return "1.0.0" if at_date >= threshold else "0.9.0"

    # Patch the resolver method that analyze_dependency actually calls for PyPI
    monkeypatch.setattr(analyzer.resolver, "get_version_dependencies", fake_get_deps)
    monkeypatch.setattr(analyzer, "get_highest_semver_version_at_date", fake_highest)

    df = analyzer.analyze_dependency("dep", pkg_metadata, dep_metadata, osv_df=[])

    assert not df.empty


def test_resolve_pypi_version_locally_basic():
    from datetime import timezone
    from dependency_metrics.resolvers import resolve_pypi_version_locally

    dep_metadata = {
        "releases": {
            "1.0.0": [{"upload_time": "2020-01-01T00:00:00Z"}],
            "2.0.0": [{"upload_time": "2020-06-01T00:00:00Z"}],
            "3.0.0": [{"upload_time": "2021-01-01T00:00:00Z"}],
        }
    }

    # Only versions uploaded on or before 2020-07-01 and matching >=1.0.0,<3.0.0
    before = datetime(2020, 7, 1, tzinfo=timezone.utc)
    result = resolve_pypi_version_locally(dep_metadata, ">=1.0.0,<3.0.0", before)
    assert result == "2.0.0"

    # Before any release
    before_any = datetime(2019, 1, 1, tzinfo=timezone.utc)
    assert resolve_pypi_version_locally(dep_metadata, ">=1.0.0", before_any) is None

    # Wildcard constraint: return highest available before date
    before_all = datetime(2022, 1, 1, tzinfo=timezone.utc)
    assert resolve_pypi_version_locally(dep_metadata, "*", before_all) == "3.0.0"


def test_build_intervals_uses_unique_sorted_dates():
    from dependency_metrics.time_utils import build_intervals

    start = datetime(2020, 1, 1)
    end = datetime(2020, 1, 5)
    dates = [
        datetime(2020, 1, 3),
        datetime(2020, 1, 2),
        datetime(2020, 1, 2),
    ]

    intervals = build_intervals(dates, start, end)

    assert intervals == [
        (datetime(2020, 1, 1), datetime(2020, 1, 2)),
        (datetime(2020, 1, 2), datetime(2020, 1, 3)),
        (datetime(2020, 1, 3), datetime(2020, 1, 5)),
    ]
