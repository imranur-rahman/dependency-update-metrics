"""
OSV remediation logic wrapper.
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from packaging import version as pkg_version

from .time_utils import ensure_utc, parse_timestamp

SEVERITY_LEVELS = ["Critical", "High", "Medium", "Low"]


class OSVService:
    """Provide remediation checks for vulnerabilities."""

    def is_remediated_by_severity(
        self,
        dependency: str,
        dependency_version: Optional[str],
        interval_start: datetime,
        osv_df: pd.DataFrame,
        dependency_metadata: Dict,
        ecosystem: str,
        osv_index: Optional[Dict[str, List[Dict]]] = None,
    ) -> Dict[str, bool]:
        """Like is_remediated(), but returns a dict keyed by severity level + 'all_severities'.

        All values start as True (remediated). For each vuln that applies, set the
        corresponding severity key and 'all_severities' to False.
        """
        result: Dict[str, bool] = {level: True for level in SEVERITY_LEVELS}
        result["all_severities"] = True

        if dependency_version is None:
            # Version resolution failed — we can't determine which specific CVEs apply,
            # so only penalise the aggregate bucket, not individual severity levels.
            result["all_severities"] = False
            return result

        from .time_utils import ensure_utc

        interval_start = ensure_utc(interval_start)

        if osv_index is not None:
            dep_vulns_list = osv_index.get(dependency)
            if not dep_vulns_list:
                return result

            try:
                current_ver = pkg_version.parse(dependency_version)
            except Exception:
                for k in result:
                    result[k] = False
                return result

            for vuln in dep_vulns_list:
                try:
                    intro_ver = pkg_version.parse(vuln["vul_introduced"])
                    fixed_ver = pkg_version.parse(vuln["vul_fixed"])

                    if intro_ver <= current_ver < fixed_ver:
                        fixed_date = self.get_version_release_date(
                            ecosystem, dependency, vuln["vul_fixed"], dependency_metadata
                        )

                        if fixed_date and fixed_date <= interval_start:
                            severity_of_vuln = vuln.get("severity", "None")
                            if severity_of_vuln in SEVERITY_LEVELS:
                                result[severity_of_vuln] = False
                            result["all_severities"] = False
                except Exception:
                    continue

            return result

        # Fallback: DataFrame linear scan
        if len(osv_df) == 0 or "package" not in osv_df.columns:
            return result

        dep_vulns = osv_df[osv_df["package"] == dependency]
        if len(dep_vulns) == 0:
            return result

        try:
            current_ver = pkg_version.parse(dependency_version)
        except Exception:
            for k in result:
                result[k] = False
            return result

        for vuln in dep_vulns.itertuples(index=False):
            try:
                intro_ver = pkg_version.parse(vuln.vul_introduced)
                fixed_ver = pkg_version.parse(vuln.vul_fixed)

                if intro_ver <= current_ver < fixed_ver:
                    fixed_date = self.get_version_release_date(
                        ecosystem, dependency, vuln.vul_fixed, dependency_metadata
                    )

                    if fixed_date and fixed_date <= interval_start:
                        severity_of_vuln = getattr(vuln, "severity", "None")
                        if severity_of_vuln in SEVERITY_LEVELS:
                            result[severity_of_vuln] = False
                        result["all_severities"] = False
            except Exception:
                continue

        return result

    def is_remediated(
        self,
        dependency: str,
        dependency_version: Optional[str],
        interval_start: datetime,
        osv_df: pd.DataFrame,
        dependency_metadata: Dict,
        ecosystem: str,
        osv_index: Optional[Dict[str, List[Dict]]] = None,
    ) -> bool:
        if dependency_version is None:
            return False
        interval_start = ensure_utc(interval_start)

        # Fast path: use pre-built index (O(1) lookup) when available
        if osv_index is not None:
            dep_vulns_list = osv_index.get(dependency)
            if not dep_vulns_list:
                return True

            try:
                current_ver = pkg_version.parse(dependency_version)
            except Exception:
                return False

            for vuln in dep_vulns_list:
                try:
                    intro_ver = pkg_version.parse(vuln["vul_introduced"])
                    fixed_ver = pkg_version.parse(vuln["vul_fixed"])

                    if intro_ver <= current_ver < fixed_ver:
                        fixed_date = self.get_version_release_date(
                            ecosystem, dependency, vuln["vul_fixed"], dependency_metadata
                        )

                        if fixed_date and fixed_date <= interval_start:
                            return False
                except Exception:
                    continue

            return True

        # Fallback: DataFrame linear scan (legacy path)
        if len(osv_df) == 0 or "package" not in osv_df.columns:
            return True

        dep_vulns = osv_df[osv_df["package"] == dependency]
        if len(dep_vulns) == 0:
            return True

        try:
            current_ver = pkg_version.parse(dependency_version)
        except Exception:
            return False

        for vuln in dep_vulns.itertuples(index=False):
            try:
                intro_ver = pkg_version.parse(vuln.vul_introduced)
                fixed_ver = pkg_version.parse(vuln.vul_fixed)

                if intro_ver <= current_ver < fixed_ver:
                    fixed_date = self.get_version_release_date(
                        ecosystem, dependency, vuln.vul_fixed, dependency_metadata
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
                versions = metadata.get("versions", {})
                ver_data = versions.get(version)
                if ver_data:
                    published = ver_data.get("dist", {}).get("published")
                    if published:
                        return parse_timestamp(published)

            elif ecosystem == "pypi":
                releases = metadata.get("releases", {})
                release_files = releases.get(version, [])
                if release_files:
                    upload_time = release_files[0].get("upload_time")
                    if upload_time:
                        return parse_timestamp(upload_time)
        except Exception:
            return None

        return None
