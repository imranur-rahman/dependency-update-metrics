"""Tests for ResolverCache in dependency_metrics/resolvers.py."""

import json
import sqlite3
import threading
from collections import OrderedDict
from pathlib import Path

import pytest

from dependency_metrics.cache_config import METADATA_CACHE_MAX
from dependency_metrics.resolvers import ResolverCache


def _make_cache(tmp_path: Path) -> ResolverCache:
    return ResolverCache(cache_dir=tmp_path / "cache")


# ---------------------------------------------------------------------------
# _capped_set — eviction logic
# ---------------------------------------------------------------------------


def test_capped_set_evicts_oldest_when_full(tmp_path):
    cache = _make_cache(tmp_path)
    od: OrderedDict = OrderedDict()
    cap = 10

    # Fill to cap+1 → triggers eviction of oldest 10% (≥1 entry)
    for i in range(cap + 1):
        cache._capped_set(od, f"key{i}", i, cap)

    assert "key0" not in od  # oldest evicted
    assert f"key{cap}" in od  # newest kept
    assert len(od) <= cap


def test_capped_set_none_cap_is_unlimited(tmp_path):
    cache = _make_cache(tmp_path)
    od: OrderedDict = OrderedDict()

    for i in range(1000):
        cache._capped_set(od, f"k{i}", i, None)

    assert len(od) == 1000


def test_capped_set_fifo_order(tmp_path):
    cache = _make_cache(tmp_path)
    od: OrderedDict = OrderedDict()
    cap = 5

    for i in range(20):
        cache._capped_set(od, f"k{i}", i, cap)

    # All remaining keys should be contiguous high-numbered entries
    remaining = [int(k[1:]) for k in od]
    assert remaining == sorted(remaining)
    assert max(remaining) == 19


def test_capped_set_single_entry_cap(tmp_path):
    cache = _make_cache(tmp_path)
    od: OrderedDict = OrderedDict()

    cache._capped_set(od, "a", 1, 1)
    cache._capped_set(od, "b", 2, 1)

    assert "a" not in od
    assert "b" in od


# ---------------------------------------------------------------------------
# Typed setters delegate to _capped_set
# ---------------------------------------------------------------------------


def test_metadata_set_enforces_cap(tmp_path):
    cache = _make_cache(tmp_path)
    cap = METADATA_CACHE_MAX
    if cap is None:
        pytest.skip("METADATA_CACHE_MAX is unlimited")

    for i in range(cap + 1):
        cache.metadata_set(("npm", f"pkg{i}"), {"name": f"pkg{i}"})

    assert len(cache.metadata_cache) <= cap


def test_npm_resolve_set_enforces_cap(tmp_path):
    cache = _make_cache(tmp_path)
    small_cap = 10
    for i in range(small_cap + 1):
        cache._capped_set(
            cache.npm_resolve_cache,
            (f"dep{i}", "^1.0.0", "2024-01-01"),
            f"1.0.{i}",
            small_cap,
        )

    assert len(cache.npm_resolve_cache) <= small_cap


def test_all_typed_setters_write_to_correct_cache(tmp_path):
    cache = _make_cache(tmp_path)

    cache.pypi_version_metadata_set("pkg@1.0.0", {"info": {}})
    assert "pkg@1.0.0" in cache.pypi_version_metadata_cache

    cache.pypi_version_deps_set("pkg@1.0.0", {"requests": ">=2.0"})
    assert "pkg@1.0.0" in cache.pypi_version_deps_cache

    cache.npm_time_set("lodash", {"1.0.0": "2020-01-01T00:00:00Z"})
    assert "lodash" in cache.npm_time_cache

    cache.version_prefix_set(("npm", "lodash"), ([], [], []))
    assert ("npm", "lodash") in cache.version_prefix_cache


# ---------------------------------------------------------------------------
# Disk cache: save_json / load_json
# ---------------------------------------------------------------------------


def test_save_and_load_json_roundtrip(tmp_path):
    cache = _make_cache(tmp_path)
    data = {"versions": {"1.0.0": {"dist": {"published": "2020-01-01"}}}}

    cache.save_json("metadata", "npm:lodash", data)
    loaded = cache.load_json("metadata", "npm:lodash")

    assert loaded == data


def test_load_json_missing_key_returns_none(tmp_path):
    cache = _make_cache(tmp_path)
    assert cache.load_json("metadata", "npm:does-not-exist") is None


def test_load_json_no_cache_dir_returns_none():
    cache = ResolverCache(cache_dir=None)
    assert cache.load_json("metadata", "any-key") is None


def test_load_json_serves_disk_preload(tmp_path):
    """load_json() returns from _disk_preload without hitting SQLite."""
    cache = _make_cache(tmp_path)
    preloaded = {"preloaded": True}
    # Write a different value to SQLite
    cache.save_json("npm_time", "npm:lodash", {"preloaded": False})
    # Inject into _disk_preload — this is what warm_from_disk populates
    cache._disk_preload[("npm_time", "npm:lodash")] = preloaded

    result = cache.load_json("npm_time", "npm:lodash")
    assert result == {"preloaded": True}


# ---------------------------------------------------------------------------
# Invalid versions: record / load
# ---------------------------------------------------------------------------


def test_record_and_load_invalid_versions(tmp_path):
    cache = _make_cache(tmp_path)

    cache.record_invalid_version("npm", "lodash", "bad-version")
    cache.record_invalid_version("npm", "lodash", "also-bad")

    # Clear in-memory copy to force a disk read
    cache.invalid_version_strings.clear()

    result = cache.load_invalid_versions("npm", "lodash")
    assert "bad-version" in result
    assert "also-bad" in result


def test_record_invalid_version_is_idempotent(tmp_path):
    cache = _make_cache(tmp_path)

    cache.record_invalid_version("npm", "lodash", "bad")
    cache.record_invalid_version("npm", "lodash", "bad")  # duplicate

    result = cache.load_invalid_versions("npm", "lodash")
    assert sorted(result) == ["bad"]


# ---------------------------------------------------------------------------
# warm_from_disk
# ---------------------------------------------------------------------------


def test_warm_from_disk_skips_metadata_namespace(tmp_path):
    cache = ResolverCache(cache_dir=tmp_path / "cache")
    cache.save_json("metadata", "npm:pkg", {"name": "pkg"})
    cache.save_json("npm_time", "npm:lodash", {"1.0.0": "2020-01-01"})
    cache._disk_preload.clear()

    cache.warm_from_disk()

    assert not any(k[0] == "metadata" for k in cache._disk_preload)
    assert ("npm_time", "npm:lodash") in cache._disk_preload


def test_warm_from_disk_loads_non_metadata_files(tmp_path):
    cache = ResolverCache(cache_dir=tmp_path / "cache")
    data = {"1.0.0": "2020-01-01T00:00:00Z"}
    cache.save_json("npm_time", "npm:somekey", data)
    cache._disk_preload.clear()

    cache.warm_from_disk()

    assert cache._disk_preload.get(("npm_time", "npm:somekey")) == data


def test_warm_from_disk_no_cache_dir_is_noop():
    cache = ResolverCache(cache_dir=None)
    cache.warm_from_disk()  # should not raise
    assert cache._disk_preload == {}


# ---------------------------------------------------------------------------
# SQLite backend — storage, schema, and WAL mode
# ---------------------------------------------------------------------------


def test_sqlite_db_created_at_expected_path(tmp_path):
    cache = _make_cache(tmp_path)
    cache.save_json("depsdev_package", "NPM:lodash", {"versions": []})

    db_path = tmp_path / "cache" / "cache.db"
    assert db_path.exists(), "cache.db should be created on first save_json call"


def test_sqlite_wal_mode_is_enabled(tmp_path):
    cache = _make_cache(tmp_path)
    cache.save_json("depsdev_package", "NPM:lodash", {"versions": []})

    db_path = tmp_path / "cache" / "cache.db"
    conn = sqlite3.connect(str(db_path))
    row = conn.execute("PRAGMA journal_mode").fetchone()
    conn.close()
    assert row[0] == "wal"


def test_sqlite_table_has_expected_schema(tmp_path):
    cache = _make_cache(tmp_path)
    cache.save_json("ns", "k", {})

    db_path = tmp_path / "cache" / "cache.db"
    conn = sqlite3.connect(str(db_path))
    cols = {row[1] for row in conn.execute("PRAGMA table_info(cache)").fetchall()}
    conn.close()
    assert cols == {"namespace", "key", "value"}


def test_save_json_overwrites_existing_key(tmp_path):
    cache = _make_cache(tmp_path)
    cache.save_json("ns", "k", {"v": 1})
    cache.save_json("ns", "k", {"v": 2})

    result = cache.load_json("ns", "k")
    assert result == {"v": 2}


def test_namespaces_are_isolated(tmp_path):
    cache = _make_cache(tmp_path)
    cache.save_json("ns_a", "key", {"source": "a"})
    cache.save_json("ns_b", "key", {"source": "b"})

    assert cache.load_json("ns_a", "key") == {"source": "a"}
    assert cache.load_json("ns_b", "key") == {"source": "b"}


def test_many_keys_across_namespaces_all_retrievable(tmp_path):
    cache = _make_cache(tmp_path)
    written = {}
    for ns in ("depsdev_package", "depsdev_req", "npm_time"):
        for i in range(10):
            key = f"{ns}:pkg{i}"
            data = {"ns": ns, "i": i}
            cache.save_json(ns, key, data)
            written[(ns, key)] = data

    for (ns, key), expected in written.items():
        assert cache.load_json(ns, key) == expected


def test_concurrent_reads_from_multiple_threads(tmp_path):
    """Multiple threads can read the same key from SQLite simultaneously."""
    cache = _make_cache(tmp_path)
    cache.save_json("depsdev_package", "NPM:lodash", {"versions": ["1.0.0"]})

    results = {}
    errors = []

    def reader(tid):
        try:
            results[tid] = cache.load_json("depsdev_package", "NPM:lodash")
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=reader, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    for i in range(8):
        assert results[i] == {"versions": ["1.0.0"]}


def test_concurrent_writes_from_multiple_threads_all_persist(tmp_path):
    """Each thread writes a unique key; all keys survive after all threads finish."""
    cache = _make_cache(tmp_path)
    errors = []

    def writer(tid):
        try:
            cache.save_json("depsdev_package", f"NPM:pkg{tid}", {"tid": tid})
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    for i in range(8):
        assert cache.load_json("depsdev_package", f"NPM:pkg{i}") == {"tid": i}


def test_each_thread_gets_its_own_sqlite_connection(tmp_path):
    """_get_sqlite_conn() returns different Connection objects per thread."""
    cache = _make_cache(tmp_path)
    connections = {}

    def grab_conn(tid):
        connections[tid] = id(cache._get_sqlite_conn())

    threads = [threading.Thread(target=grab_conn, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # All thread-local connection IDs should be distinct
    assert len(set(connections.values())) == 4


# ---------------------------------------------------------------------------
# warm_from_disk — SQLite-backed behaviour
# ---------------------------------------------------------------------------


def test_warm_from_disk_noop_when_db_file_absent(tmp_path):
    """warm_from_disk() does nothing when cache.db does not exist yet."""
    cache = ResolverCache(cache_dir=tmp_path / "cache")
    cache.warm_from_disk()
    assert cache._disk_preload == {}


def test_warm_from_disk_loads_multiple_target_namespaces(tmp_path):
    cache = ResolverCache(cache_dir=tmp_path / "cache")
    cache.save_json("depsdev_package", "NPM:express", {"versions": []})
    cache.save_json("depsdev_req", "NPM:express:4.0.0", {"nodes": []})
    cache.save_json("npm_time", "npm:express", {"4.0.0": "2020-01-01"})
    cache._disk_preload.clear()

    cache.warm_from_disk()

    assert ("depsdev_package", "NPM:express") in cache._disk_preload
    assert ("depsdev_req", "NPM:express:4.0.0") in cache._disk_preload
    assert ("npm_time", "npm:express") in cache._disk_preload


def test_warm_from_disk_values_match_saved_data(tmp_path):
    data = {"versions": [{"versionKey": {"version": "1.0.0"}}]}
    cache = ResolverCache(cache_dir=tmp_path / "cache")
    cache.save_json("depsdev_package", "NPM:lodash", data)
    cache._disk_preload.clear()

    cache.warm_from_disk()

    assert cache._disk_preload[("depsdev_package", "NPM:lodash")] == data


def test_warm_from_disk_preloaded_entry_bypasses_sqlite_on_load(tmp_path):
    """After warm_from_disk, load_json returns the preloaded value without a SQLite query."""
    cache = ResolverCache(cache_dir=tmp_path / "cache")
    original = {"versions": ["1.0.0"]}
    cache.save_json("depsdev_package", "NPM:lodash", original)
    cache._disk_preload.clear()
    cache.warm_from_disk()

    # Overwrite the DB entry so a live SQLite read would return something different
    cache._get_sqlite_conn().execute(
        "UPDATE cache SET value=? WHERE namespace=? AND key=?",
        (json.dumps({"versions": ["9.9.9"]}), "depsdev_package", "NPM:lodash"),
    )
    cache._get_sqlite_conn().commit()

    # load_json should serve the preloaded (original) value, not the updated one
    result = cache.load_json("depsdev_package", "NPM:lodash")
    assert result == original
