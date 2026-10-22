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


# ---------------------------------------------------------------------------
# OSV build from the per-ecosystem zips (issue #13)
# ---------------------------------------------------------------------------


def _osv_range(*events):
    return {"type": "SEMVER", "events": list(events)}


# Small OSV records. Each key is the folder name of one OSV ecosystem zip.
_SHARED_RECORD = {
    # Affects npm and PyPI, so OSV puts it in both zips.
    "id": "GHSA-shared",
    "severity": [{"type": "CVSS_V3", "score": "9.8"}],
    "affected": [
        {
            "package": {"name": "shared-npm", "ecosystem": "npm"},
            "ranges": [_osv_range({"introduced": "0"}, {"fixed": "2.0.0"})],
        },
        {
            "package": {"name": "shared-py", "ecosystem": "PyPI"},
            "ranges": [_osv_range({"introduced": "1.0"}, {"fixed": "1.5"})],
        },
    ],
}
_OSV_FIXTURE_RECORDS = {
    "npm": [
        {
            "id": "GHSA-npm-1",
            "affected": [
                {
                    "package": {"name": "lodash", "ecosystem": "npm"},
                    "database_specific": {"severity": "HIGH"},
                    "ranges": [_osv_range({"introduced": "0"}, {"fixed": "4.17.21"})],
                }
            ],
        },
        {
            "id": "GHSA-npm-2",
            "database_specific": {"severity": "MODERATE"},
            "affected": [
                {
                    "package": {"name": "minimist", "ecosystem": "npm"},
                    "ranges": [
                        _osv_range(
                            {"introduced": "1.0"},
                            {"fixed": "1.2"},
                            {"introduced": "1.2.3"},
                            {"fixed": "1.2.6"},
                        ),
                        _osv_range({"introduced": "2"}),
                    ],
                }
            ],
        },
        _SHARED_RECORD,
    ],
    "PyPI": [
        {
            "id": "PYSEC-1",
            "severity": [{"type": "CVSS_V3", "score": "CVSS:3.1/AV:N/AC:L"}],
            "affected": [
                {
                    "package": {"name": "django", "ecosystem": "PyPI"},
                    "ranges": [_osv_range({"introduced": "3.0"}, {"fixed": "3.2.5"})],
                }
            ],
        },
        {
            "id": "PYSEC-2",
            "severity": [{"type": "X", "score": "5.0"}],
            "affected": [
                {
                    "package": {"name": "requests", "ecosystem": "PyPI"},
                    "ranges": [_osv_range({"introduced": "0"}, {"fixed": "2.31"})],
                },
                {"package": {"name": "no-ranges", "ecosystem": "PyPI"}},
            ],
        },
        _SHARED_RECORD,
    ],
    "crates.io": [
        {
            "id": "RUSTSEC-1",
            "database_specific": {"severity": "low"},
            "affected": [
                {
                    "package": {"name": "smallvec", "ecosystem": "crates.io"},
                    "ranges": [_osv_range({"introduced": "0.6.3"}, {"fixed": "1.6.1"})],
                }
            ],
        },
        {
            "id": "RUSTSEC-2",
            "affected": [
                {
                    "package": {"name": "nofix", "ecosystem": "crates.io"},
                    "ranges": [_osv_range({"introduced": "0"})],
                }
            ],
        },
    ],
}

# The rows that the old build (download all.zip, extract it, parse each
# extracted JSON file) gave for the same records, with each id once and one
# file that is not valid JSON. Recorded from the old code before the change.
_EXPECTED_INDEX_ROWS = sorted(
    [
        ("GHSA-npm-1", "NPM", "lodash", "0.0.0", "4.17.21", "High"),
        ("GHSA-npm-2", "NPM", "minimist", "1.0.0", "1.2.0", "Medium"),
        ("GHSA-npm-2", "NPM", "minimist", "1.2.3", "1.2.6", "Medium"),
        ("GHSA-shared", "NPM", "shared-npm", "0.0.0", "2.0.0", "Critical"),
        ("GHSA-shared", "PYPI", "shared-py", "1.0.0", "1.5.0", "Critical"),
        ("PYSEC-1", "PYPI", "django", "3.0.0", "3.2.5", "None"),
        ("PYSEC-2", "PYPI", "requests", "0.0.0", "2.31.0", "Medium"),
        ("RUSTSEC-1", "CRATES.IO", "smallvec", "0.6.3", "1.6.1", "Low"),
    ]
)
_INDEX_COLUMNS = ["vul_id", "ecosystem", "package", "vul_introduced", "vul_fixed", "severity"]
_OSV_ZIP_URLS = sorted(
    [
        "https://storage.googleapis.com/osv-vulnerabilities/npm/all.zip",
        "https://storage.googleapis.com/osv-vulnerabilities/PyPI/all.zip",
        "https://storage.googleapis.com/osv-vulnerabilities/crates.io/all.zip",
    ]
)


def _write_fixture_zip(path: Path, records: list) -> None:
    import json
    import zipfile

    with zipfile.ZipFile(path, "w") as zf:
        for record in records:
            zf.writestr(f"{record['id']}.json", json.dumps(record))
        zf.writestr("BROKEN-1.json", "{not json")


def _fixture_fetcher(downloaded_urls: list):
    """Return a fetch function that writes the fixture zip for each OSV URL."""

    def fetch(url: str, dest: Path) -> None:
        downloaded_urls.append(url)
        folder = url.rsplit("/", 2)[-2]
        _write_fixture_zip(dest, _OSV_FIXTURE_RECORDS[folder])

    return fetch


def _index_rows(df: pd.DataFrame) -> list:
    return sorted(tuple(row) for row in df[_INDEX_COLUMNS].itertuples(index=False))


def test_osv_build_from_ecosystem_zips_matches_old_extracted_build(tmp_path: Path):
    from dependency_metrics.osv_builder import OSVBuilder

    urls: list = []
    builder = OSVBuilder(tmp_path, fetch_zip=_fixture_fetcher(urls))

    df = builder.build_database()

    assert sorted(urls) == _OSV_ZIP_URLS
    assert _index_rows(df) == _EXPECTED_INDEX_ROWS
    assert _index_rows(pd.read_parquet(tmp_path / "osv_database.parquet")) == _EXPECTED_INDEX_ROWS


def test_osv_build_does_not_extract_files_and_leaves_only_the_index(tmp_path: Path, monkeypatch):
    import zipfile

    from dependency_metrics.osv_builder import OSVBuilder

    def no_extract(*args, **kwargs):
        raise AssertionError("the OSV build must not extract files")

    monkeypatch.setattr(zipfile.ZipFile, "extract", no_extract)
    monkeypatch.setattr(zipfile.ZipFile, "extractall", no_extract)

    OSVBuilder(tmp_path, fetch_zip=_fixture_fetcher([])).build_database()

    leftovers = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*"))
    assert leftovers == ["osv_database.parquet"]


def _build_old_index(tmp_path: Path) -> list:
    from dependency_metrics.osv_builder import OSVBuilder

    OSVBuilder(tmp_path, fetch_zip=_fixture_fetcher([])).build_database()
    return _index_rows(pd.read_parquet(tmp_path / "osv_database.parquet"))


def test_failed_osv_download_keeps_old_index(tmp_path: Path):
    from dependency_metrics.osv_builder import OSVBuilder

    old_rows = _build_old_index(tmp_path)
    good_fetch = _fixture_fetcher([])

    def fetch_fails_on_last_zip(url: str, dest: Path) -> None:
        if "crates.io" in url:
            raise RuntimeError("network down")
        good_fetch(url, dest)

    builder = OSVBuilder(tmp_path, fetch_zip=fetch_fails_on_last_zip)
    try:
        builder.build_database(force=True)
    except RuntimeError:
        pass
    else:
        raise AssertionError("the build must raise when a download fails")

    assert _index_rows(pd.read_parquet(tmp_path / "osv_database.parquet")) == old_rows
    leftovers = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*"))
    assert leftovers == ["osv_database.parquet"]


def test_osv_index_write_that_fails_midway_keeps_old_index(tmp_path: Path, monkeypatch):
    """The new index goes to a temporary path and replaces the old one in one
    rename, so a write that stops partway never touches the old index."""
    import pyarrow.parquet as pq

    from dependency_metrics.osv_builder import OSVBuilder

    old_rows = _build_old_index(tmp_path)

    def partial_write(table, where, *args, **kwargs):
        Path(where).write_bytes(b"PAR1 partial")
        raise OSError("disk full")

    monkeypatch.setattr(pq, "write_table", partial_write)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", partial_write)
    builder = OSVBuilder(tmp_path, fetch_zip=_fixture_fetcher([]))
    try:
        builder.build_database(force=True)
    except OSError:
        pass
    else:
        raise AssertionError("the build must raise when the write fails")

    assert _index_rows(pd.read_parquet(tmp_path / "osv_database.parquet")) == old_rows
    leftovers = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*"))
    assert leftovers == ["osv_database.parquet"]


def test_osv_build_records_index_date(tmp_path: Path):
    from datetime import date

    from dependency_metrics.osv_builder import OSVBuilder

    assert OSVBuilder(tmp_path).index_date() is None

    OSVBuilder(
        tmp_path,
        fetch_zip=_fixture_fetcher([]),
        clock=lambda: datetime(2026, 10, 21, 23, 30, tzinfo=timezone.utc),
    ).build_database()

    # A new builder (for example, in the Host) reads the date from the index file.
    assert OSVBuilder(tmp_path).index_date() == date(2026, 10, 21)


def test_cli_build_osv_alone_rebuilds_existing_index(tmp_path: Path, monkeypatch):
    """The Host rebuilds the index every 7 days with `--build-osv` only, so the
    flag must replace an index that exists and must not need a package."""
    from unittest.mock import patch

    import pytest

    from dependency_metrics import osv_builder
    from dependency_metrics.cli import main

    stale = pd.DataFrame([dict(zip(_INDEX_COLUMNS, ("OLD-1", "NPM", "old", "0.0.0", "1.0.0", "Low")))])
    stale.to_parquet(tmp_path / "osv_database.parquet", index=False)
    urls: list = []
    monkeypatch.setattr(osv_builder, "download_file", _fixture_fetcher(urls))

    with patch("sys.argv", ["cli", "--build-osv", "--output-dir", str(tmp_path)]):
        with pytest.raises(SystemExit) as exc_info:
            main()

    assert exc_info.value.code == 0
    assert sorted(urls) == _OSV_ZIP_URLS
    assert _index_rows(pd.read_parquet(tmp_path / "osv_database.parquet")) == _EXPECTED_INDEX_ROWS
    assert osv_builder.OSVBuilder(tmp_path).index_date() is not None


def test_osv_index_date_is_none_for_index_from_old_build(tmp_path: Path):
    from dependency_metrics.osv_builder import OSVBuilder

    pd.DataFrame([dict(zip(_INDEX_COLUMNS, _EXPECTED_INDEX_ROWS[0]))]).to_parquet(
        tmp_path / "osv_database.parquet", index=False
    )

    assert OSVBuilder(tmp_path).index_date() is None
