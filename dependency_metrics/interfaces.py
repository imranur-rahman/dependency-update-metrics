"""
Interfaces for resolvers and vulnerability services.
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, Optional, Protocol, Tuple

import pandas as pd

from .models import PackageVersion


class PackageResolver(Protocol):
    """Resolve package versions and dependencies for an ecosystem."""

    ecosystem: str

    def fetch_package_metadata(self, package_name: str) -> Dict:
        ...

    def get_package_version_at_date(self, metadata: Dict) -> Tuple[str, Dict]:
        ...

    def get_all_versions_with_dates(
        self, metadata: Dict, package_name: Optional[str] = None
    ) -> Iterable[PackageVersion]:
        ...

    def resolve_dependency_version(
        self, dependency: str, constraint: str, before_date: datetime
    ) -> Optional[str]:
        ...

    def get_highest_semver_version_at_date(
        self, package_name: str, at_date: datetime, metadata: Optional[Dict] = None
    ) -> Optional[str]:
        ...

    def extract_dependencies(self, version_data: Dict) -> Dict[str, str]:
        ...

    def get_version_dependencies(self, package: str, version: str) -> Dict[str, str]:
        ...


class VulnerabilityService(Protocol):
    """Provide vulnerability data and remediation checks."""

    def get_vulnerabilities(self, ecosystem: str, package: str) -> pd.DataFrame:
        ...

    def is_remediated(
        self,
        dependency: str,
        dependency_version: Optional[str],
        interval_start: datetime,
        osv_df: pd.DataFrame,
        dependency_metadata: Dict,
    ) -> bool:
        ...
