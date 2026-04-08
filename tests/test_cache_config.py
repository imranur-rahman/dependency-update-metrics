"""Tests for dependency_metrics/cache_config.py."""

from unittest.mock import patch

import dependency_metrics.cache_config as cfg


def test_warm_disk_max_bytes_is_correct_fraction():
    total_ram = 32 * 1024**3  # 32 GB
    with patch("dependency_metrics.cache_config.psutil.virtual_memory") as mock_vm:
        mock_vm.return_value.total = total_ram
        result = cfg.warm_disk_max_bytes()
    assert result == int(total_ram * cfg.WARM_DISK_FRACTION)


def test_warm_disk_fraction_is_conservative():
    """Fraction must be > 0 and ≤ 0.5 to leave headroom for analysis."""
    assert 0 < cfg.WARM_DISK_FRACTION <= 0.5


def test_warm_skip_namespaces_contains_metadata():
    assert "metadata" in cfg.WARM_SKIP_NAMESPACES


def test_warm_skip_namespaces_is_frozenset():
    assert isinstance(cfg.WARM_SKIP_NAMESPACES, frozenset)


def test_resolve_cache_max_has_npm_and_pypi():
    assert "npm" in cfg.RESOLVE_CACHE_MAX
    assert "pypi" in cfg.RESOLVE_CACHE_MAX


def test_all_cache_caps_are_positive_or_none():
    caps = [
        cfg.METADATA_CACHE_MAX,
        cfg.PYPI_VERSION_METADATA_CACHE_MAX,
        cfg.PYPI_VERSION_DEPS_CACHE_MAX,
        cfg.VERSION_PREFIX_CACHE_MAX,
        cfg.NPM_TIME_CACHE_MAX,
    ] + list(cfg.RESOLVE_CACHE_MAX.values())

    for cap in caps:
        assert cap is None or (isinstance(cap, int) and cap > 0), f"Invalid cap value: {cap!r}"


def test_npm_cap_larger_than_pypi_cap():
    """npm resolve entries are tiny (~1 KB); pypi entries are large (~1-5 MB)."""
    npm_cap = cfg.RESOLVE_CACHE_MAX.get("npm")
    pypi_cap = cfg.RESOLVE_CACHE_MAX.get("pypi")
    if npm_cap is not None and pypi_cap is not None:
        assert npm_cap > pypi_cap
