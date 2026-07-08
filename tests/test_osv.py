from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from dependency_metrics.analyzer import DependencyAnalyzer
from dependency_metrics.osv_service import OSVService


def test_check_remediation_false_when_fix_available_before_interval(tmp_path: Path):
    analyzer = DependencyAnalyzer(
        ecosystem="pypi",
        package="demo",
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 10),
        output_dir=tmp_path,
    )

    osv_df = pd.DataFrame(
        [
            {
                "package": "dep",
                "vul_introduced": "1.0.0",
                "vul_fixed": "2.0.0",
            }
        ]
    )
    dep_metadata = {"releases": {"2.0.0": [{"upload_time": "2020-01-02T00:00:00Z"}]}}

    remediated = analyzer._check_remediation(
        dependency="dep",
        dep_version="1.5.0",
        interval_start=datetime(2020, 1, 5, tzinfo=timezone.utc),
        osv_df=osv_df,
        dep_metadata=dep_metadata,
    )

    assert remediated is False


def test_check_remediation_true_when_fix_after_interval(tmp_path: Path):
    analyzer = DependencyAnalyzer(
        ecosystem="pypi",
        package="demo",
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 10),
        output_dir=tmp_path,
    )

    osv_df = pd.DataFrame(
        [
            {
                "package": "dep",
                "vul_introduced": "1.0.0",
                "vul_fixed": "2.0.0",
            }
        ]
    )
    dep_metadata = {"releases": {"2.0.0": [{"upload_time": "2020-01-09T00:00:00Z"}]}}

    remediated = analyzer._check_remediation(
        dependency="dep",
        dep_version="1.5.0",
        interval_start=datetime(2020, 1, 5, tzinfo=timezone.utc),
        osv_df=osv_df,
        dep_metadata=dep_metadata,
    )

    assert remediated is True


def _depsdev_metadata(version_to_published: dict) -> dict:
    """Build metadata shaped like deps.dev's GetPackage response: "versions"
    is a LIST of {"versionKey": {"version": ...}, "publishedAt": ...} entries —
    structurally different from npm-registry/PyPI-native metadata, where
    "versions"/"releases" is a dict keyed by version string."""
    return {
        "packageKey": {"system": "NPM", "name": "dep"},
        "versions": [
            {"versionKey": {"system": "NPM", "name": "dep", "version": ver}, "publishedAt": ts}
            for ver, ts in version_to_published.items()
        ],
    }


def test_check_remediation_false_with_depsdev_shaped_metadata(tmp_path: Path):
    """Regression test: --depsdev analyses must also detect "fixed but not
    adopted" — get_version_release_date previously only understood the
    npm-registry dict-of-versions shape, silently returned None for deps.dev's
    list-of-versions shape (an AttributeError swallowed by a blanket except),
    and so is_remediated always reported True regardless of how vulnerable the
    in-use version was (e.g. minimist@1.2.0 under GHSA-vh95-rmgr-6w4m)."""
    analyzer = DependencyAnalyzer(
        ecosystem="npm",
        package="demo",
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 10),
        output_dir=tmp_path,
    )

    osv_df = pd.DataFrame(
        [
            {
                "package": "dep",
                "vul_introduced": "1.0.0",
                "vul_fixed": "2.0.0",
            }
        ]
    )
    dep_metadata = _depsdev_metadata({"2.0.0": "2020-01-02T00:00:00Z"})

    remediated = analyzer._check_remediation(
        dependency="dep",
        dep_version="1.5.0",
        interval_start=datetime(2020, 1, 5, tzinfo=timezone.utc),
        osv_df=osv_df,
        dep_metadata=dep_metadata,
    )

    assert remediated is False


def test_check_remediation_true_with_depsdev_shaped_metadata_when_fix_after_interval(
    tmp_path: Path,
):
    analyzer = DependencyAnalyzer(
        ecosystem="npm",
        package="demo",
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 10),
        output_dir=tmp_path,
    )

    osv_df = pd.DataFrame(
        [
            {
                "package": "dep",
                "vul_introduced": "1.0.0",
                "vul_fixed": "2.0.0",
            }
        ]
    )
    dep_metadata = _depsdev_metadata({"2.0.0": "2020-01-09T00:00:00Z"})

    remediated = analyzer._check_remediation(
        dependency="dep",
        dep_version="1.5.0",
        interval_start=datetime(2020, 1, 5, tzinfo=timezone.utc),
        osv_df=osv_df,
        dep_metadata=dep_metadata,
    )

    assert remediated is True


def test_get_version_release_date_handles_both_npm_registry_and_depsdev_shapes():
    service = OSVService()

    npm_registry_metadata = {
        "versions": {"2.0.0": {"dist": {"published": "2020-01-02T00:00:00.000Z"}}}
    }
    assert service.get_version_release_date(
        "npm", "dep", "2.0.0", npm_registry_metadata
    ) == datetime(2020, 1, 2, tzinfo=timezone.utc)

    depsdev_metadata = _depsdev_metadata({"2.0.0": "2020-01-02T00:00:00Z"})
    assert service.get_version_release_date("npm", "dep", "2.0.0", depsdev_metadata) == datetime(
        2020, 1, 2, tzinfo=timezone.utc
    )

    # Unknown version in either shape -> None, not an exception
    assert service.get_version_release_date("npm", "dep", "9.9.9", npm_registry_metadata) is None
    assert service.get_version_release_date("npm", "dep", "9.9.9", depsdev_metadata) is None


def test_get_version_release_date_handles_native_crates_metadata():
    service = OSVService()
    crates_metadata = {
        "versions": [{"num": "1.0.0", "created_at": "2020-01-02T00:00:00Z", "yanked": False}]
    }

    assert service.get_version_release_date("cargo", "dep", "1.0.0", crates_metadata) == datetime(
        2020, 1, 2, tzinfo=timezone.utc
    )
