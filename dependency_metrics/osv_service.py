"""
OSV remediation logic wrapper.
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional

import pandas as pd
from packaging import version as pkg_version

from .time_utils import ensure_utc, parse_timestamp


class OSVService:
    """Provide remediation checks for vulnerabilities."""

    def is_remediated(
        self,
        dependency: str,
        dependency_version: Optional[str],
        interval_start: datetime,
        osv_df: pd.DataFrame,
        dependency_metadata: Dict,
        ecosystem: str,
    ) -> bool:
        if dependency_version is None:
            return False
        interval_start = ensure_utc(interval_start)

        if len(osv_df) == 0 or 'package' not in osv_df.columns:
            return True

        dep_vulns = osv_df[osv_df['package'] == dependency]
        if len(dep_vulns) == 0:
            return True

        try:
            current_ver = pkg_version.parse(dependency_version)
        except Exception:
            return False

        for _, vuln in dep_vulns.iterrows():
            try:
                intro_ver = pkg_version.parse(vuln['vul_introduced'])
                fixed_ver = pkg_version.parse(vuln['vul_fixed'])

                if intro_ver <= current_ver < fixed_ver:
                    fixed_date = self.get_version_release_date(
                        ecosystem, dependency, vuln['vul_fixed'], dependency_metadata
                    )

                    if fixed_date and fixed_date <= interval_start:
                        return False
            except Exception:
                continue

        return True


    def get_version_release_date(
        self,
        ecosystem: str,
        package: str,
        version: str,
        metadata: Dict,
    ) -> Optional[datetime]:
        try:
            if ecosystem == "npm":
                versions = metadata.get('versions', {})
                ver_data = versions.get(version)
                if ver_data:
                    published = ver_data.get('dist', {}).get('published')
                    if published:
                        return parse_timestamp(published)

            elif ecosystem == "pypi":
                releases = metadata.get('releases', {})
                release_files = releases.get(version, [])
                if release_files:
                    upload_time = release_files[0].get('upload_time')
                    if upload_time:
                        return parse_timestamp(upload_time)
        except Exception:
            return None

        return None
