"""Tests for DepsDevClient in dependency_metrics/depsdev_client.py."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from dependency_metrics.depsdev_client import DepsDevClient
from dependency_metrics.resolvers import ResolverCache


def _make_client(tmp_path: Path) -> DepsDevClient:
    cache = ResolverCache(cache_dir=tmp_path / "cache")
    return DepsDevClient(cache=cache)


def _mock_response(data: dict, status: int = 200) -> MagicMock:
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status
    resp.json.return_value = data
    resp.raise_for_status = MagicMock()
    if status >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
    return resp


# ---------------------------------------------------------------------------
# get_package — URL construction
# ---------------------------------------------------------------------------


def test_get_package_builds_correct_url(tmp_path):
    client = _make_client(tmp_path)
    expected_data = {"versions": []}

    with patch.object(client._cache, "get", return_value=_mock_response(expected_data)) as mock_get:
        result = client.get_package("NPM", "express")

    called_url = mock_get.call_args[0][0]
    assert called_url == "https://api.deps.dev/v3/systems/NPM/packages/express"
    assert result == expected_data


def test_get_package_url_encodes_scoped_npm_package(tmp_path):
    client = _make_client(tmp_path)

    with patch.object(client._cache, "get", return_value=_mock_response({})) as mock_get:
        client.get_package("NPM", "@scope/pkg")

    called_url = mock_get.call_args[0][0]
    assert "%40scope%2Fpkg" in called_url
    assert "@" not in called_url.split("/packages/")[1]


def test_get_package_url_encodes_pypi_package(tmp_path):
    client = _make_client(tmp_path)

    with patch.object(client._cache, "get", return_value=_mock_response({})) as mock_get:
        client.get_package("PYPI", "my-package")

    called_url = mock_get.call_args[0][0]
    assert "my-package" in called_url


# ---------------------------------------------------------------------------
# get_requirements — URL construction
# ---------------------------------------------------------------------------


def test_get_requirements_builds_correct_url(tmp_path):
    client = _make_client(tmp_path)
    expected_data = {"nodes": []}

    with patch.object(client._cache, "get", return_value=_mock_response(expected_data)) as mock_get:
        result = client.get_requirements("CARGO", "serde", "1.0.196")

    called_url = mock_get.call_args[0][0]
    assert called_url == (
        "https://api.deps.dev/v3/systems/CARGO/packages/serde/versions/1.0.196:requirements"
    )
    assert result == expected_data


def test_get_requirements_url_encodes_version_with_plus(tmp_path):
    client = _make_client(tmp_path)

    with patch.object(client._cache, "get", return_value=_mock_response({})) as mock_get:
        client.get_requirements("NPM", "pkg", "1.0.0+build")

    called_url = mock_get.call_args[0][0]
    assert "+" not in called_url
    assert "%2B" in called_url or "1.0.0" in called_url


# ---------------------------------------------------------------------------
# Disk cache — get_package
# ---------------------------------------------------------------------------


def test_get_package_disk_cache_hit_skips_http(tmp_path):
    client = _make_client(tmp_path)
    cached = {
        "versions": [{"versionKey": {"version": "1.0.0"}, "publishedAt": "2020-01-01T00:00:00Z"}]
    }
    client._cache.save_json("depsdev_package", "NPM:express", cached)

    with patch.object(client._cache, "get") as mock_get:
        result = client.get_package("NPM", "express")
        mock_get.assert_not_called()

    assert result == cached


def test_get_package_response_written_to_disk_cache(tmp_path):
    client = _make_client(tmp_path)
    data = {"versions": [{"versionKey": {"version": "2.0.0"}}]}

    with patch.object(client._cache, "get", return_value=_mock_response(data)):
        client.get_package("PYPI", "requests")

    # Second call must not hit HTTP
    with patch.object(client._cache, "get") as mock_get:
        result = client.get_package("PYPI", "requests")
        mock_get.assert_not_called()

    assert result == data


def test_get_requirements_disk_cache_hit_skips_http(tmp_path):
    client = _make_client(tmp_path)
    cached = {"nodes": []}
    client._cache.save_json("depsdev_req", "NPM:lodash:4.17.21", cached)

    with patch.object(client._cache, "get") as mock_get:
        result = client.get_requirements("NPM", "lodash", "4.17.21")
        mock_get.assert_not_called()

    assert result == cached


# ---------------------------------------------------------------------------
# HTTP errors
# ---------------------------------------------------------------------------


def test_get_package_raises_on_404(tmp_path):
    client = _make_client(tmp_path)
    resp = _mock_response({}, status=404)

    with patch.object(client._cache, "get", return_value=resp):
        with pytest.raises(requests.HTTPError):
            client.get_package("NPM", "no-such-package")


def test_get_requirements_raises_on_500(tmp_path):
    client = _make_client(tmp_path)
    resp = _mock_response({}, status=500)

    with patch.object(client._cache, "get", return_value=resp):
        with pytest.raises(requests.HTTPError):
            client.get_requirements("CARGO", "serde", "1.0.0")


# ---------------------------------------------------------------------------
# Cache namespace isolation
# ---------------------------------------------------------------------------


def test_get_package_and_requirements_use_separate_namespaces(tmp_path):
    client = _make_client(tmp_path)
    pkg_data = {"versions": []}
    req_data = {"nodes": []}

    with patch.object(client._cache, "get", return_value=_mock_response(pkg_data)):
        client.get_package("NPM", "express")

    with patch.object(client._cache, "get", return_value=_mock_response(req_data)):
        client.get_requirements("NPM", "express", "4.18.0")

    # Both namespaces should be populated independently
    loaded_pkg = client._cache.load_json("depsdev_package", "NPM:express")
    loaded_req = client._cache.load_json("depsdev_req", "NPM:express:4.18.0")
    assert loaded_pkg == pkg_data
    assert loaded_req == req_data
