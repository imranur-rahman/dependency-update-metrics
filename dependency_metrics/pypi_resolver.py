"""
Resolve PyPI versions using patched pip's --before support.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from packaging.specifiers import SpecifierSet


_PIP_VENDOR_PATH = Path(__file__).resolve().parents[1] / "vendor" / "pip" / "src"


def _ensure_pip_on_path() -> None:
    """Ensure the vendored pip is available for imports."""
    if _PIP_VENDOR_PATH.exists():
        sys.path.insert(0, str(_PIP_VENDOR_PATH))
        return
    raise FileNotFoundError(f"Vendored pip not found at {_PIP_VENDOR_PATH}")


@dataclass
class PyPIResolver:
    """Resolve versions from PyPI using pip's PackageFinder."""

    _finder_cache: dict[datetime, "PackageFinder"]

    def __init__(self) -> None:
        self._finder_cache = {}

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

    def _build_specifier(self, constraint: str) -> Optional[SpecifierSet]:
        if not constraint or constraint == "*":
            return None
        return SpecifierSet(constraint)

    def _get_finder(self, before: datetime) -> "PackageFinder":
        if before in self._finder_cache:
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
        selection_prefs = SelectionPreferences(
            allow_yanked=True,
            allow_all_prereleases=False,
            format_control=None,
            prefer_binary=False,
            ignore_requires_python=None,
            before=before,
        )
        finder = PackageFinder.create(
            link_collector=link_collector,
            selection_prefs=selection_prefs,
            target_python=None,
        )
        self._finder_cache[before] = finder
        return finder


_DEFAULT_RESOLVER = PyPIResolver()


def resolve_pypi_version(package: str, constraint: str, before: datetime) -> Optional[str]:
    """Resolve version using a shared resolver instance."""
    return _DEFAULT_RESOLVER.resolve(package, constraint, before)
