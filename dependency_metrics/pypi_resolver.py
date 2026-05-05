"""
Resolve PyPI versions using patched pip's --before support.
"""

from __future__ import annotations

import subprocess
import sys
import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .cache_config import RESOLVE_CACHE_MAX as _RESOLVE_CACHE_MAX

_FINDER_CACHE_MAX_SIZE: int | None = _RESOLVE_CACHE_MAX.get("pypi", 500)

_PIP_REPO_URL = "https://github.com/imranur-rahman/pip"

# Primary: vendor/pip in the source tree (git submodule / editable install)
_LOCAL_VENDOR_PATH = Path(__file__).resolve().parents[1] / "vendor" / "pip" / "src"

# Fallback: user home dir (PyPI-installed users, or source users without submodule)
_USER_VENDOR_PATH = Path.home() / ".dependency_metrics" / "vendor" / "pip" / "src"


def _ensure_pip_on_path() -> None:
    """Ensure the patched pip is importable, auto-cloning it on first use if needed."""
    # Fast path: already inserted by a prior call
    local_str = str(_LOCAL_VENDOR_PATH)
    user_str = str(_USER_VENDOR_PATH)
    if local_str in sys.path or user_str in sys.path:
        return

    if _LOCAL_VENDOR_PATH.exists():
        sys.path.insert(0, local_str)
        return

    if _USER_VENDOR_PATH.exists():
        sys.path.insert(0, user_str)
        return

    # Neither location exists — clone once to the user cache dir
    clone_target = _USER_VENDOR_PATH.parent  # ~/.dependency_metrics/vendor/pip
    print(
        f"[dependency-metrics] Patched pip not found. Cloning from {_PIP_REPO_URL} "
        f"to {clone_target} (one-time setup)..."
    )
    clone_target.parent.mkdir(parents=True, exist_ok=True)
    try:
        subprocess.run(
            ["git", "clone", "--depth=1", _PIP_REPO_URL, str(clone_target)],
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"Failed to clone patched pip from {_PIP_REPO_URL}. "
            "Ensure git is installed and you have internet access, or clone manually: "
            f"git clone {_PIP_REPO_URL} vendor/pip"
        ) from exc

    if not _USER_VENDOR_PATH.exists():
        raise RuntimeError(f"Cloned pip repo but expected src/ not found at {_USER_VENDOR_PATH}")
    sys.path.insert(0, user_str)
    print("[dependency-metrics] Patched pip installed successfully.")


@dataclass
class PyPIResolver:
    """Resolve versions from PyPI using pip's PackageFinder."""

    _finder_cache: OrderedDict
    _finder_lock: threading.Lock

    def __init__(self) -> None:
        self._finder_cache = OrderedDict()
        self._finder_lock = threading.Lock()

    def resolve(self, package: str, constraint: str, before: datetime) -> Optional[str]:
        """Resolve highest version before date that satisfies constraint."""
        _ensure_pip_on_path()

        if before.tzinfo is None:
            before = before.replace(tzinfo=timezone.utc)
        else:
            before = before.astimezone(timezone.utc)

        finder = self._get_finder(before)
        specifier = self._build_specifier(constraint)
        result = finder.find_best_candidate(project_name=package, specifier=specifier)
        if result.best_candidate is None:
            return None
        return str(result.best_candidate.version)

    def _build_specifier(self, constraint: str):
        if not constraint or constraint == "*":
            return None
        # Use pip's vendored packaging to match candidate versions.
        from pip._vendor.packaging.specifiers import SpecifierSet  # type: ignore

        return SpecifierSet(constraint)

    def _get_finder(self, before: datetime):
        with self._finder_lock:
            if before in self._finder_cache:
                self._finder_cache.move_to_end(before)
                return self._finder_cache[before]

        # Local import after sys.path injection.
        from pip._internal.index.package_finder import PackageFinder
        from pip._internal.models.search_scope import SearchScope
        from pip._internal.models.selection_prefs import SelectionPreferences
        from pip._internal.network.session import PipSession
        from pip._internal.index.collector import LinkCollector

        session = PipSession()
        search_scope = SearchScope.create(
            find_links=[],
            index_urls=["https://pypi.org/simple"],
            no_index=False,
        )
        link_collector = LinkCollector(session=session, search_scope=search_scope)
        selection_prefs = SelectionPreferences(  # type: ignore[call-arg]
            allow_yanked=True,
            allow_all_prereleases=False,
            format_control=None,
            prefer_binary=False,
            ignore_requires_python=False,
            before=before,
        )
        finder = PackageFinder.create(
            link_collector=link_collector,
            selection_prefs=selection_prefs,
            target_python=None,
        )
        with self._finder_lock:
            self._finder_cache[before] = finder
            self._finder_cache.move_to_end(before)
            # Evict oldest entries beyond the size cap; None = unlimited
            if _FINDER_CACHE_MAX_SIZE is not None:
                while len(self._finder_cache) > _FINDER_CACHE_MAX_SIZE:
                    self._finder_cache.popitem(last=False)
        return finder


_DEFAULT_RESOLVER = PyPIResolver()


def resolve_pypi_version(package: str, constraint: str, before: datetime) -> Optional[str]:
    """Resolve version using a shared resolver instance."""
    return _DEFAULT_RESOLVER.resolve(package, constraint, before)
