"""Tests for analyze_at_release_points and _calculate_weight_with_window."""

import math
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from dependency_metrics.analyzer import DependencyAnalyzer
from dependency_metrics.resolvers import ResolverCache


def _make_analyzer(tmp_path: Path, ecosystem: str = "pypi") -> DependencyAnalyzer:
    return DependencyAnalyzer(
        ecosystem=ecosystem,
        package="mypackage",
        start_date=datetime(2020, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2021, 1, 1, tzinfo=timezone.utc),
        weighting_type="disable",
        output_dir=tmp_path,
        resolver_cache=ResolverCache(cache_dir=tmp_path / "cache"),
    )


def _utc(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# 1. One result row per release in window
# ---------------------------------------------------------------------------


def test_analyze_at_release_points_returns_one_row_per_release(tmp_path):
    analyzer = _make_analyzer(tmp_path)
    row = {
        "row_num": 1,
        "start_date": _utc(2020, 1, 1),
        "end_date": _utc(2021, 1, 1),
    }

    releases = [
        ("1.0.0", _utc(2020, 3, 1)),
        ("1.1.0", _utc(2020, 6, 1)),
        ("2.0.0", _utc(2020, 9, 1)),
    ]

    with (
        patch.object(analyzer, "fetch_package_metadata", return_value={}),
        patch.object(analyzer, "get_all_versions_with_dates", return_value=releases),
        patch.object(analyzer.resolver, "get_version_dependencies", return_value={}),
    ):
        results = analyzer.analyze_at_release_points(row, osv_df=pd.DataFrame())

    assert len(results) == len(releases)


# ---------------------------------------------------------------------------
# 2. window_end equals release date for each result
# ---------------------------------------------------------------------------


def test_window_end_equals_release_date(tmp_path):
    analyzer = _make_analyzer(tmp_path)
    row = {
        "row_num": 2,
        "start_date": _utc(2020, 1, 1),
        "end_date": _utc(2021, 1, 1),
    }

    releases = [
        ("1.0.0", _utc(2020, 4, 1)),
        ("1.1.0", _utc(2020, 8, 1)),
    ]

    with (
        patch.object(analyzer, "fetch_package_metadata", return_value={}),
        patch.object(analyzer, "get_all_versions_with_dates", return_value=releases),
        patch.object(analyzer.resolver, "get_version_dependencies", return_value={}),
    ):
        results = analyzer.analyze_at_release_points(row, osv_df=pd.DataFrame())

    release_dates = [r[1].date().isoformat() for r in releases]
    window_ends = [r["summary"]["window_end"] for r in results]
    assert window_ends == release_dates


# ---------------------------------------------------------------------------
# 3. No releases in window → empty list
# ---------------------------------------------------------------------------


def test_no_releases_returns_empty(tmp_path):
    analyzer = _make_analyzer(tmp_path)
    row = {
        "row_num": 3,
        "start_date": _utc(2020, 1, 1),
        "end_date": _utc(2021, 1, 1),
    }

    with (
        patch.object(analyzer, "fetch_package_metadata", return_value={}),
        patch.object(analyzer, "get_all_versions_with_dates", return_value=[]),
    ):
        results = analyzer.analyze_at_release_points(row, osv_df=pd.DataFrame())

    assert results == []


# ---------------------------------------------------------------------------
# 4. Package version with no deps → mttu=0, mttr=0, num_dependencies=0
# ---------------------------------------------------------------------------


def test_no_deps_returns_zero_metrics(tmp_path):
    analyzer = _make_analyzer(tmp_path)
    row = {
        "row_num": 4,
        "start_date": _utc(2020, 1, 1),
        "end_date": _utc(2021, 1, 1),
    }

    releases = [("1.0.0", _utc(2020, 6, 1))]

    with (
        patch.object(analyzer, "fetch_package_metadata", return_value={}),
        patch.object(analyzer, "get_all_versions_with_dates", return_value=releases),
        patch.object(analyzer.resolver, "get_version_dependencies", return_value={}),
    ):
        results = analyzer.analyze_at_release_points(row, osv_df=pd.DataFrame())

    assert len(results) == 1
    summary = results[0]["summary"]
    assert summary["mttu"] == 0.0
    assert summary["mttr"] == 0.0
    assert summary["num_dependencies"] == 0


# ---------------------------------------------------------------------------
# 5. _calculate_weight_with_window linear formula
# ---------------------------------------------------------------------------


def test_calculate_weight_with_window_linear(tmp_path):
    analyzer = _make_analyzer(tmp_path)
    analyzer.weighting_type = "linear"

    window_start = _utc(2020, 1, 1)
    window_end = _utc(2020, 1, 11)  # 10-day window
    # age=0 → weight 1.0
    assert analyzer._calculate_weight_with_window(0, window_start, window_end) == pytest.approx(1.0)
    # age=5 → weight 0.5
    assert analyzer._calculate_weight_with_window(5, window_start, window_end) == pytest.approx(0.5)
    # age=10 → weight 0.0
    assert analyzer._calculate_weight_with_window(10, window_start, window_end) == pytest.approx(
        0.0
    )

    # disable weighting always returns 1.0
    analyzer.weighting_type = "disable"
    assert analyzer._calculate_weight_with_window(999, window_start, window_end) == pytest.approx(
        1.0
    )

    # inverse weighting
    analyzer.weighting_type = "inverse"
    assert analyzer._calculate_weight_with_window(0, window_start, window_end) == pytest.approx(1.0)
    assert analyzer._calculate_weight_with_window(1, window_start, window_end) == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# 6. MTTU and MTTR correctness for exponential weighting (cafeteria-like scenario)
# ---------------------------------------------------------------------------


def test_mttu_mttr_exponential_two_releases(tmp_path):
    """Per-release MTTU/MTTR and dep-frame intervals are bound by the first release
    that actually DECLARES the dependency — not by the package's first release ever.

    Package mypkg has 3 releases in the analysis window:
      0.20.0 (2022-01-01) — declares NO dependencies (mirrors import-meta-resolve@0.1.0)
      0.21.0 (2022-07-01) — first release to declare depA, constraint ">=0.9.0,<1.1.0"
      0.22.0 (2024-01-01) — also declares depA with the same constraint
    depA release history: 1.0.0 (2021-06-01) and 1.1.0 (2023-01-01).
    The resolver always returns dep_version="1.0.0" (satisfies constraint).
    Highest dep version at date:
      - before 2023-01-01: "1.0.0"  → updated=True   (dep is on latest)
      - from   2023-01-01: "1.1.0"  → updated=False  (dep lags behind)

    `first_use_date["depA"]` must be 0.21.0's date (2022-07-01) — NOT mypkg's
    first-ever release (0.20.0 @ 2022-01-01, which declares nothing) and NOT
    depA's own first release (1.0.0 @ 2021-06-01, which predates mypkg's adoption
    of it). Interval structure (built from dep_dates_cache, bound at 2022-07-01):
        i=0  2022-07-01 .. 2023-01-01  (184 d)  updated=True
        i=1  2023-01-01 .. 2024-01-01  (365 d)  updated=False
        i=2  2024-01-01 .. 2024-03-05  ( 64 d)  updated=False

    Expected MTTU (exponential half-life=80):
      v0.20.0 — declares no deps → empty/zeroed summary, no dep frame, MTTU=0
      v0.21.0 — bisect k=0 (its release date IS the first interval_start, no prior
        history exists yet) → no dep frame, MTTU=0
      v0.22.0 — bisect k=2: intervals i=0..1; only i=1 non-updated (365 d) → MTTU=365

    MTTR=0 for all releases (empty osv_df → no vulnerabilities).

    dependency_frames is a SINGLE continuous timeline per dependency, attached
    only to the LAST release's result (0.22.0) — see analyze_at_release_points'
    dependency_frames contract. It spans [first_use("depA")=2022-07-01, end_date],
    each interval tagged with whichever release actually governed it (the
    highest available at that point) — NOT the release whose summary the frame
    happens to be attached to. age_of_interval/weight are end_date-relative
    (2024-03-05), since the timeline isn't tied to any one release's window.
    """
    lam = math.log(2) / 80.0

    analyzer = DependencyAnalyzer(
        ecosystem="npm",
        package="mypkg",
        start_date=_utc(2021, 1, 1),
        end_date=_utc(2024, 3, 5),
        weighting_type="exponential",
        half_life=80.0,
        output_dir=tmp_path,
        resolver_cache=ResolverCache(cache_dir=tmp_path / "cache"),
    )

    row = {
        "row_num": 1,
        "start_date": _utc(2021, 1, 1),
        "end_date": _utc(2024, 3, 5),
    }

    pkg_releases = [
        ("0.20.0", _utc(2022, 1, 1)),
        ("0.21.0", _utc(2022, 7, 1)),
        ("0.22.0", _utc(2024, 1, 1)),
    ]
    dep_releases = [("1.0.0", _utc(2021, 6, 1)), ("1.1.0", _utc(2023, 1, 1))]

    def _versions(meta, *, package_name=None):
        return pkg_releases if package_name == "mypkg" else dep_releases

    def _highest(package_name, at_date, metadata=None):
        return "1.1.0" if at_date >= _utc(2023, 1, 1) else "1.0.0"

    def _deps(package, ver):
        return {} if ver == "0.20.0" else {"depA": ">=0.9.0,<1.1.0"}

    with (
        patch.object(analyzer, "fetch_package_metadata", return_value={}),
        patch.object(analyzer, "get_all_versions_with_dates", side_effect=_versions),
        patch.object(analyzer.resolver, "get_version_dependencies", side_effect=_deps),
        patch.object(analyzer, "get_highest_semver_version_at_date", side_effect=_highest),
        patch.object(analyzer, "resolve_dependency_version", return_value="1.0.0"),
    ):
        results = analyzer.analyze_at_release_points(
            row, osv_df=pd.DataFrame(), generate_dep_frames=True
        )

    assert len(results) == 3
    r20 = next(r for r in results if r["summary"]["package_version"] == "0.20.0")
    r21 = next(r for r in results if r["summary"]["package_version"] == "0.21.0")
    r22 = next(r for r in results if r["summary"]["package_version"] == "0.22.0")

    # --- v0.20.0: declares no deps at all → zeroed summary, no dep frame ---
    assert r20["summary"]["mttu"] == pytest.approx(0.0)
    assert r20["summary"]["mttr"] == pytest.approx(0.0)
    assert r20["summary"]["num_dependencies"] == 0
    assert r20["dependency_frames"] == []

    # --- MTTU / MTTR ---
    # v0.21.0: k=0 (its own release date is the first interval_start — no history yet
    # to look back on) → no dep frame contributed → MTTU=0
    assert r21["summary"]["mttu"] == pytest.approx(0.0)
    assert r21["summary"]["mttr"] == pytest.approx(0.0)

    # v0.22.0: k=2, only interval i=1 is non-updated (365 d); weight cancels → MTTU=365
    assert r22["summary"]["mttu"] == pytest.approx(365.0, rel=1e-6)
    assert r22["summary"]["mttr"] == pytest.approx(0.0)

    # --- Dep frames: a SINGLE continuous timeline per dependency, attached only
    # to the LAST release's result (0.22.0) — r20/r21 get nothing, regardless of
    # whether they themselves declare/use the dep, because frames are global. ---
    assert r20["dependency_frames"] == []
    assert r21["dependency_frames"] == []
    assert len(r22["dependency_frames"]) == 1
    dfA = r22["dependency_frames"][0]
    assert len(dfA) == 3

    # Spans [first_use("depA")=2022-07-01, end_date=2024-03-05] — 3 intervals,
    # each tagged with whichever release actually GOVERNED it (highest semver
    # available at that point):
    #   i=0  2022-07-01..2023-01-01  pkg=0.21.0 (first to declare depA)
    #   i=1  2023-01-01..2024-01-01  pkg=0.21.0 (0.22.0 doesn't exist yet)
    #   i=2  2024-01-01..2024-03-05  pkg=0.22.0
    expected_starts = [_utc(2022, 7, 1), _utc(2023, 1, 1), _utc(2024, 1, 1)]
    assert dfA["interval_start"].tolist() == expected_starts
    assert dfA["package_version"].tolist() == ["0.21.0", "0.21.0", "0.22.0"]
    assert dfA["dependency_constraint"].tolist() == [">=0.9.0,<1.1.0"] * 3

    # age_of_interval/weight are end_date-relative (2024-03-05) — the timeline
    # isn't bound to any single release's window_end.
    expected_ages = [(_utc(2024, 3, 5) - s).days for s in expected_starts]
    assert dfA["age_of_interval"].tolist() == expected_ages
    assert dfA["weight"].tolist() == [
        pytest.approx(math.exp(-lam * age), rel=1e-6) for age in expected_ages
    ]
    assert "interval_duration" not in dfA.columns

    # resolve_dependency_version is mocked to always return "1.0.0"; only from
    # 2023-01-01 onward does "1.1.0" become the highest available → non-updated.
    non_updated = dfA[~dfA["updated"]]
    assert non_updated["interval_start"].tolist() == [_utc(2023, 1, 1), _utc(2024, 1, 1)]


# ---------------------------------------------------------------------------
# 7. dependency_frames is a SINGLE continuous timeline per dependency: each
#    interval is tagged with whichever package version actually GOVERNED it
#    (the highest available at that point) and that SAME version's own
#    declared constraint — so package_version and dependency_constraint can
#    never disagree (a single manifest's constraint is immutable). Each
#    (dependency, interval) appears exactly once — not replayed inside every
#    release's retrospective view.
# ---------------------------------------------------------------------------


def test_dependency_frame_is_single_timeline_with_consistent_package_version(tmp_path):
    """Mirrors the real npm/import-meta-resolve scenario that prompted this
    redesign: `builtins`'s declared constraint changes partway through the
    package's history (^1.0.0 -> ^2.0.0 at 2.0.0), and a worksheet reader saw
    rows tagged `package_version=2.0.1` with BOTH ^1.0.0 and ^2.0.0 — looking
    contradictory, since one manifest's constraint never changes. The fix:
    `package_version` must be whichever release actually GOVERNED each interval
    (the highest available at that point), not the release whose retrospective
    replay the row used to live inside — so it always matches that interval's
    `dependency_constraint` by construction, and each interval is emitted once.

    mypkg releases (constraint declared for depA):
      1.0.0 (2021-01-01) — introduces depA, constraint ^1.0.0
      1.1.0 (2021-06-01) — still ^1.0.0
      2.0.0 (2022-01-01) — constraint changes to ^2.0.0
      2.0.1 (2022-06-01) — still ^2.0.0
    depA releases: 1.0.0 (2020-06-01), 2.0.0 (2021-12-01).
    resolve_dependency_version maps each constraint straight to its matching
    version (^1.0.0 -> "1.0.0", ^2.0.0 -> "2.0.0"); highest is always "2.0.0".

    Single timeline for depA (dates = union of pkg/dep release dates from
    first-use 2021-01-01 onward), each interval tagged with whichever release
    actually governed it (highest semver available at that interval's start):
      i=0  2021-01-01..2021-06-01  pkg=1.0.0  ^1.0.0 -> 1.0.0  (not updated)
      i=1  2021-06-01..2021-12-01  pkg=1.1.0  ^1.0.0 -> 1.0.0  (not updated)
      i=2  2021-12-01..2022-01-01  pkg=1.1.0  ^1.0.0 -> 1.0.0  (not updated)
      i=3  2022-01-01..2022-06-01  pkg=2.0.0  ^2.0.0 -> 2.0.0  (updated)
      i=4  2022-06-01..2022-09-01  pkg=2.0.1  ^2.0.0 -> 2.0.0  (updated)

    Note i=1/i=2 are governed by 1.1.0 — the highest release that EXISTED at
    those points — not 2.0.0, which wasn't out yet despite being semver-higher
    overall. And the whole timeline is attached only once, to the LAST
    release's result (2.0.1); every earlier release gets `[]`.
    """
    analyzer = DependencyAnalyzer(
        ecosystem="npm",
        package="mypkg",
        start_date=_utc(2021, 1, 1),
        end_date=_utc(2022, 9, 1),
        weighting_type="disable",
        output_dir=tmp_path,
        resolver_cache=ResolverCache(cache_dir=tmp_path / "cache"),
    )

    row = {
        "row_num": 1,
        "start_date": _utc(2021, 1, 1),
        "end_date": _utc(2022, 9, 1),
    }

    pkg_releases = [
        ("1.0.0", _utc(2021, 1, 1)),
        ("1.1.0", _utc(2021, 6, 1)),
        ("2.0.0", _utc(2022, 1, 1)),
        ("2.0.1", _utc(2022, 6, 1)),
    ]
    dep_releases = [("1.0.0", _utc(2020, 6, 1)), ("2.0.0", _utc(2021, 12, 1))]
    deps_by_version = {
        "1.0.0": {"depA": "^1.0.0"},
        "1.1.0": {"depA": "^1.0.0"},
        "2.0.0": {"depA": "^2.0.0"},
        "2.0.1": {"depA": "^2.0.0"},
    }
    resolved_by_constraint = {"^1.0.0": "1.0.0", "^2.0.0": "2.0.0"}

    def _versions(meta, *, package_name=None):
        return pkg_releases if package_name == "mypkg" else dep_releases

    def _deps(package, ver):
        return deps_by_version[ver]

    def _resolve(dep_name, constraint, date):
        return resolved_by_constraint[constraint]

    with (
        patch.object(analyzer, "fetch_package_metadata", return_value={}),
        patch.object(analyzer, "get_all_versions_with_dates", side_effect=_versions),
        patch.object(analyzer.resolver, "get_version_dependencies", side_effect=_deps),
        patch.object(analyzer, "get_highest_semver_version_at_date", return_value="2.0.0"),
        patch.object(analyzer, "resolve_dependency_version", side_effect=_resolve),
    ):
        results = analyzer.analyze_at_release_points(
            row, osv_df=pd.DataFrame(), generate_dep_frames=True
        )

    by_version = {r["summary"]["package_version"]: r for r in results}

    # The global timeline is attached only to the LAST release's result —
    # every earlier release gets nothing.
    assert by_version["1.0.0"]["dependency_frames"] == []
    assert by_version["1.1.0"]["dependency_frames"] == []
    assert by_version["2.0.0"]["dependency_frames"] == []
    frames = by_version["2.0.1"]["dependency_frames"]
    assert len(frames) == 1
    dfA = frames[0]

    expected_starts = [
        _utc(2021, 1, 1),
        _utc(2021, 6, 1),
        _utc(2021, 12, 1),
        _utc(2022, 1, 1),
        _utc(2022, 6, 1),
    ]
    assert dfA["interval_start"].tolist() == expected_starts
    assert dfA["interval_end"].tolist() == expected_starts[1:] + [_utc(2022, 9, 1)]

    # The crux: package_version is whichever release actually GOVERNED each
    # interval (highest available at that point) — 1.1.0 governs i=1/i=2 even
    # though 2.0.0 exists later in mypkg's history; 2.0.0 only takes over from
    # its own release date onward.
    assert dfA["package_version"].tolist() == ["1.0.0", "1.1.0", "1.1.0", "2.0.0", "2.0.1"]
    assert dfA["dependency_constraint"].tolist() == [
        "^1.0.0",
        "^1.0.0",
        "^1.0.0",
        "^2.0.0",
        "^2.0.0",
    ]
    assert dfA["dependency_version"].tolist() == ["1.0.0", "1.0.0", "1.0.0", "2.0.0", "2.0.0"]
    assert dfA["updated"].tolist() == [False, False, False, True, True]

    # The user's exact complaint made structurally impossible: package_version
    # and dependency_constraint always come from the SAME governing manifest —
    # cross-check every row against an independent {version: constraint} map.
    for _, r in dfA.iterrows():
        assert deps_by_version[r["package_version"]]["depA"] == r["dependency_constraint"]

    # Each (dependency, interval_start) pair appears exactly once.
    assert not dfA.duplicated(subset=["dependency", "interval_start"]).any()

    # age_of_interval/weight are end_date-relative (2022-09-01), and
    # interval_duration no longer exists (dropped — meaningless without a
    # per-release window to cap against).
    expected_ages = [(_utc(2022, 9, 1) - s).days for s in expected_starts]
    assert dfA["age_of_interval"].tolist() == expected_ages
    assert dfA["weight"].tolist() == [1.0] * 5  # weighting_type="disable"
    assert "interval_duration" not in dfA.columns


def test_dependency_frame_prefers_highest_semver_over_most_recent_release(tmp_path):
    """An out-of-order release — e.g. a 1.x backport published AFTER 2.0.0 is
    already out — must NOT steal governance back from 2.0.0. "The highest
    available version" means highest by SEMVER, not most-recently-published
    (mirrors `analyze_dependency`'s `pkg_version_at_interval`,
    analyzer.py:1364-1394, and is exactly what the user described: "once 2.0.2
    exists we don't care about 2.0.1 anymore" — the comparison is by version
    number, not publish order).

    mypkg releases (constraint declared for depA), note 1.5.2 is published
    AFTER 2.0.0 despite being a lower semver version:
      1.0.0 (2021-01-01) — introduces depA, ^1.0.0
      2.0.0 (2021-06-01) — bumps to ^2.0.0
      1.5.2 (2021-09-01) — late 1.x backport, declares ^1.5.0 (must NOT govern
                           — 2.0.0 remains "the highest available")
    """
    analyzer = DependencyAnalyzer(
        ecosystem="npm",
        package="mypkg",
        start_date=_utc(2021, 1, 1),
        end_date=_utc(2021, 12, 1),
        weighting_type="disable",
        output_dir=tmp_path,
        resolver_cache=ResolverCache(cache_dir=tmp_path / "cache"),
    )

    row = {"row_num": 1, "start_date": _utc(2021, 1, 1), "end_date": _utc(2021, 12, 1)}

    pkg_releases = [
        ("1.0.0", _utc(2021, 1, 1)),
        ("2.0.0", _utc(2021, 6, 1)),
        ("1.5.2", _utc(2021, 9, 1)),
    ]
    dep_releases = [("1.0.0", _utc(2020, 6, 1))]
    deps_by_version = {
        "1.0.0": {"depA": "^1.0.0"},
        "2.0.0": {"depA": "^2.0.0"},
        "1.5.2": {"depA": "^1.5.0"},
    }
    resolved_by_constraint = {"^1.0.0": "1.0.0", "^2.0.0": "2.0.0"}

    def _versions(meta, *, package_name=None):
        return pkg_releases if package_name == "mypkg" else dep_releases

    def _deps(package, ver):
        return deps_by_version[ver]

    def _resolve(dep_name, constraint, date):
        return resolved_by_constraint[constraint]

    with (
        patch.object(analyzer, "fetch_package_metadata", return_value={}),
        patch.object(analyzer, "get_all_versions_with_dates", side_effect=_versions),
        patch.object(analyzer.resolver, "get_version_dependencies", side_effect=_deps),
        patch.object(analyzer, "get_highest_semver_version_at_date", return_value="1.0.0"),
        patch.object(analyzer, "resolve_dependency_version", side_effect=_resolve),
    ):
        results = analyzer.analyze_at_release_points(
            row, osv_df=pd.DataFrame(), generate_dep_frames=True
        )

    by_version = {r["summary"]["package_version"]: r for r in results}
    frames = by_version["1.5.2"]["dependency_frames"]
    assert len(frames) == 1
    dfA = frames[0]

    # From 2.0.0's release date onward, 2.0.0 — NOT the later-published-but
    # lower-semver 1.5.2 — must remain "the highest available version" and
    # keep governing every subsequent interval.
    governed_after_200 = dfA[dfA["interval_start"] >= _utc(2021, 6, 1)]
    assert not governed_after_200.empty
    assert (governed_after_200["package_version"] == "2.0.0").all()
    assert (governed_after_200["dependency_constraint"] == "^2.0.0").all()
    assert "1.5.2" not in dfA["package_version"].tolist()
