"""Additional edge-case tests for resolve_pypi_version_locally()."""

from datetime import datetime, timezone

from dependency_metrics.resolvers import resolve_pypi_version_locally


def _utc(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


def _metadata(*versions) -> dict:
    """Build a minimal PyPI metadata dict with the given (version, upload_date) pairs."""
    releases = {}
    for ver, upload_time in versions:
        releases[ver] = [{"upload_time": upload_time}]
    return {"releases": releases}


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_wildcard_constraint_returns_latest_before_date():
    meta = _metadata(
        ("1.0.0", "2020-01-01T00:00:00"),
        ("2.0.0", "2021-01-01T00:00:00"),
        ("3.0.0", "2022-01-01T00:00:00"),
    )
    result = resolve_pypi_version_locally(meta, "*", _utc(2021, 6, 1))
    assert result == "2.0.0"


def test_multiple_matching_returns_highest():
    meta = _metadata(
        ("1.0.0", "2020-01-01T00:00:00"),
        ("1.1.0", "2020-06-01T00:00:00"),
        ("1.2.0", "2020-11-01T00:00:00"),
    )
    result = resolve_pypi_version_locally(meta, ">=1.0.0", _utc(2021, 1, 1))
    assert result == "1.2.0"


def test_exact_boundary_version_is_included():
    """Version uploaded on the exact cutoff date should be included (pub_date <= cmp_date)."""
    meta = _metadata(("1.5.0", "2021-06-01T00:00:00"))
    result = resolve_pypi_version_locally(meta, ">=1.0.0", _utc(2021, 6, 1))
    assert result == "1.5.0"


# ---------------------------------------------------------------------------
# Exclusion / filtering
# ---------------------------------------------------------------------------


def test_no_versions_before_date_returns_none():
    meta = _metadata(
        ("1.0.0", "2023-01-01T00:00:00"),
        ("2.0.0", "2024-01-01T00:00:00"),
    )
    result = resolve_pypi_version_locally(meta, ">=1.0.0", _utc(2020, 1, 1))
    assert result is None


def test_version_one_day_after_cutoff_excluded():
    meta = _metadata(
        ("1.0.0", "2021-01-01T00:00:00"),
        ("2.0.0", "2021-06-02T00:00:00"),  # one day after cutoff
    )
    result = resolve_pypi_version_locally(meta, ">=1.0.0", _utc(2021, 6, 1))
    assert result == "1.0.0"


def test_specifier_excludes_non_matching_versions():
    meta = _metadata(
        ("1.0.0", "2020-01-01T00:00:00"),
        ("2.0.0", "2021-01-01T00:00:00"),
        ("3.0.0", "2022-01-01T00:00:00"),
    )
    # Only versions < 3.0.0 should match
    result = resolve_pypi_version_locally(meta, ">=1.0.0,<3.0.0", _utc(2023, 1, 1))
    assert result == "2.0.0"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_releases_dict_returns_none():
    result = resolve_pypi_version_locally({"releases": {}}, ">=1.0.0", _utc(2021, 1, 1))
    assert result is None


def test_releases_with_empty_file_list_skipped():
    meta = {
        "releases": {
            "1.0.0": [],  # no files → no upload_time
            "2.0.0": [{"upload_time": "2021-01-01T00:00:00"}],
        }
    }
    result = resolve_pypi_version_locally(meta, ">=1.0.0", _utc(2022, 1, 1))
    assert result == "2.0.0"


def test_empty_constraint_string_treated_as_wildcard():
    meta = _metadata(
        ("1.0.0", "2020-01-01T00:00:00"),
        ("2.0.0", "2021-01-01T00:00:00"),
    )
    result = resolve_pypi_version_locally(meta, "", _utc(2022, 1, 1))
    assert result == "2.0.0"


def test_naive_before_date_is_handled():
    """before_date without tzinfo should not raise."""
    meta = _metadata(("1.0.0", "2020-01-01T00:00:00"))
    naive_date = datetime(2021, 1, 1)  # no tzinfo
    result = resolve_pypi_version_locally(meta, ">=1.0.0", naive_date)
    assert result == "1.0.0"
