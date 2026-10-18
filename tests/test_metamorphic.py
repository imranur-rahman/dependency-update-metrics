"""Metamorphic tests for MTTU and MTTR.

A metric tool has no test oracle: nobody knows the "true" MTTU of a real
package. Metamorphic testing (Kanewala and Bieman, IST 2014) works around this.
Each test runs the analyzer on a source input and on a follow-up input that is
changed in a known way, and checks the relation that must hold between the two
outputs.

All tests are fully stubbed; no network calls.
"""

import socket
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from unittest.mock import patch

from dependency_metrics.analyzer import DependencyAnalyzer
from dependency_metrics.resolvers import ResolverCache


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """Fail loudly if any test in this module tries to reach the network."""

    def _blocked(*args, **kwargs):
        raise RuntimeError("network access is not allowed in metamorphic tests")

    monkeypatch.setattr(socket.socket, "connect", _blocked)


def _utc(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


# Source scenario (PyPI):
#   mypkg 1.0.0 (2021-01-01) and 1.1.0 (2022-01-01) both declare depA "<2.0".
#   depA 1.0.0 (2020-06-01) and 2.0.0 (2021-06-01); the constraint always
#   selects 1.0.0, so depA is outdated from 2021-06-01 on.
#   VULN-A affects depA < 2.0.0 and is fixed in 2.0.0 (published 2021-06-01),
#   so depA is also unremediated from 2021-06-01 on.
#   At release 1.1.0: MTTU = MTTR = 214 days (2021-06-01 .. 2022-01-01).
PKG_RELEASES = [("1.0.0", _utc(2021, 1, 1)), ("1.1.0", _utc(2022, 1, 1))]
DEP_RELEASES = [("1.0.0", _utc(2020, 6, 1)), ("2.0.0", _utc(2021, 6, 1))]
VULN_A = {
    "vul_id": "VULN-A",
    "ecosystem": "PYPI",
    "package": "depA",
    "vul_introduced": "0",
    "vul_fixed": "2.0.0",
    "severity": "High",
}


def _run(
    tmp_path,
    *,
    shift=timedelta(0),
    dep_releases=None,
    vulns=None,
    weighting_type="disable",
    half_life=None,
):
    """Run analyze_at_release_points on the scenario and return the summary
    of the last release (1.1.0)."""
    dep_releases = DEP_RELEASES if dep_releases is None else dep_releases
    vulns = [VULN_A] if vulns is None else vulns
    pkg = [(v, t + shift) for v, t in PKG_RELEASES]
    dep = [(v, t + shift) for v, t in dep_releases]
    newest_at = {v: t for v, t in dep if "rc" not in v}
    start, end = _utc(2020, 1, 1) + shift, _utc(2023, 1, 1) + shift

    analyzer = DependencyAnalyzer(
        ecosystem="pypi",
        package="mypkg",
        start_date=start,
        end_date=end,
        weighting_type=weighting_type,
        half_life=half_life,
        output_dir=tmp_path,
        resolver_cache=ResolverCache(cache_dir=tmp_path / "cache"),
    )
    dep_metadata = {
        "releases": {v: [{"upload_time": t.strftime("%Y-%m-%dT%H:%M:%S")}] for v, t in dep}
    }

    def _versions(meta, *, package_name=None):
        return pkg if package_name == "mypkg" else dep

    def _highest(package_name, at_date, metadata=None):
        released = [(t, v) for v, t in newest_at.items() if t <= at_date]
        return max(released)[1] if released else None

    with (
        patch.object(analyzer, "fetch_package_metadata", return_value=dep_metadata),
        patch.object(analyzer, "get_all_versions_with_dates", side_effect=_versions),
        patch.object(analyzer.resolver, "get_version_dependencies", return_value={"depA": "<2.0"}),
        patch.object(analyzer, "get_highest_semver_version_at_date", side_effect=_highest),
        patch.object(analyzer, "resolve_dependency_version", return_value="1.0.0"),
        patch.object(analyzer.resolver, "resolve_constraint_at_date", return_value="1.0.0"),
    ):
        results = analyzer.analyze_at_release_points(
            {"row_num": 1, "start_date": start, "end_date": end},
            osv_df=pd.DataFrame(vulns),
        )
    return next(r for r in results if r["summary"]["package_version"] == "1.1.0")["summary"]


def test_source_scenario_has_nonzero_metrics(tmp_path):
    """Baseline: the source input is non-trivial, so the relations below compare
    real values, not two zeros."""
    s = _run(tmp_path)
    assert s["mttu"] == pytest.approx(214.0)
    assert s["mttr"] == pytest.approx(214.0)


def test_mr1_unrelated_vulnerability_changes_nothing(tmp_path):
    """MR1: a vulnerability in a package that is not a dependency leaves MTTU
    and MTTR unchanged."""
    source = _run(tmp_path / "s")
    other = dict(VULN_A, vul_id="VULN-OTHER", package="unrelated-pkg")
    follow = _run(tmp_path / "f", vulns=[VULN_A, other])
    assert follow["mttu"] == pytest.approx(source["mttu"])
    assert follow["mttr"] == pytest.approx(source["mttr"])


def test_mr2_vulnerability_without_published_fix_does_not_count(tmp_path):
    """MR2 (fix gate): a vulnerability whose fixed version was never released
    leaves MTTR unchanged, because no project could have adopted the fix."""
    source = _run(tmp_path / "s")
    unfixed = dict(VULN_A, vul_id="VULN-UNFIXED", vul_fixed="9.9.9")
    follow = _run(tmp_path / "f", vulns=[VULN_A, unfixed])
    assert follow["mttu"] == pytest.approx(source["mttu"])
    assert follow["mttr"] == pytest.approx(source["mttr"])


@pytest.mark.parametrize("days", [1, 365, 1000])
def test_mr3_time_shift_invariance(tmp_path, days):
    """MR3: shifting every date by the same amount leaves unweighted MTTU and
    MTTR unchanged; the metrics depend on durations, not on calendar dates."""
    source = _run(tmp_path / "s")
    follow = _run(tmp_path / "f", shift=timedelta(days=days))
    assert follow["mttu"] == pytest.approx(source["mttu"])
    assert follow["mttr"] == pytest.approx(source["mttr"])


def test_mr4_extra_boundary_splits_but_does_not_change_totals(tmp_path):
    """MR4: a dependency release that changes neither the selected nor the
    newest stable version (here a pre-release) only splits an interval, so
    unweighted MTTU and MTTR stay the same."""
    source = _run(tmp_path / "s")
    extra = DEP_RELEASES + [("2.1.0rc1", _utc(2021, 9, 1))]
    follow = _run(tmp_path / "f", dep_releases=extra)
    assert follow["mttu"] == pytest.approx(source["mttu"])
    assert follow["mttr"] == pytest.approx(source["mttr"])
