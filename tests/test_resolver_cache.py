"""Tests for ResolverCache in dependency_metrics/resolvers.py."""

import json
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
    """load_json() returns from _disk_preload without reading the file."""
    cache = _make_cache(tmp_path)
    preloaded = {"preloaded": True}
    # Write a different value to disk
    cache.save_json("npm_time", "npm:lodash", {"preloaded": False})
    # Inject into _disk_preload — this is what warm_from_disk populates
    cache_path = cache._cache_path("npm_time", "npm:lodash")
    cache._disk_preload.setdefault("npm_time", {})[cache_path.stem] = preloaded

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
    cache_dir = tmp_path / "cache"
    (cache_dir / "metadata").mkdir(parents=True)
    (cache_dir / "metadata" / "abc.json").write_text('{"name": "pkg"}')

    (cache_dir / "npm_time").mkdir(parents=True)
    (cache_dir / "npm_time" / "def.json").write_text('{"1.0.0": "2020-01-01"}')

    cache = ResolverCache(cache_dir=cache_dir)
    cache.warm_from_disk()

    assert "metadata" not in cache._disk_preload
    assert "npm_time" in cache._disk_preload


def test_warm_from_disk_loads_non_metadata_files(tmp_path):
    cache_dir = tmp_path / "cache"
    npm_dir = cache_dir / "npm_time"
    npm_dir.mkdir(parents=True)
    data = {"1.0.0": "2020-01-01T00:00:00Z"}
    (npm_dir / "somekey.json").write_text(json.dumps(data))

    cache = ResolverCache(cache_dir=cache_dir)
    cache.warm_from_disk()

    assert cache._disk_preload.get("npm_time", {}).get("somekey") == data


def test_warm_from_disk_no_cache_dir_is_noop():
    cache = ResolverCache(cache_dir=None)
    cache.warm_from_disk()  # should not raise
    assert cache._disk_preload == {}
