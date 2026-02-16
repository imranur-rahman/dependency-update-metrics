"""
Core dependency analyzer for calculating TTU and TTR metrics.
"""

import logging
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd
from packaging import version as pkg_version

from .osv_builder import OSVBuilder
from .osv_service import OSVService
from .resolvers import NpmResolver, PyPIResolver, ResolverCache, npm_semver_key
from .time_utils import build_intervals, parse_timestamp


logger = logging.getLogger(__name__)


class DependencyAnalyzer:
    """Analyze dependency update and remediation metrics."""
    
    def __init__(
        self,
        ecosystem: str,
        package: str,
        start_date: datetime,
        end_date: datetime,
        weighting_type: str = "disable",
        half_life: Optional[float] = None,
        output_dir: Path = Path("./output"),
        resolver_cache: Optional[ResolverCache] = None,
    ):
        """Initialize dependency analyzer.
        
        Args:
            ecosystem: Ecosystem name (npm, pypi)
            package: Package name to analyze
            start_date: Start date for analysis
            end_date: End date for analysis
            weighting_type: Type of weighting (linear, exponential, inverse, disable)
            half_life: Half-life in days (for exponential weighting)
            output_dir: Output directory for results
            resolver_cache: Shared resolver cache for minimizing network calls
        """
        self.ecosystem = ecosystem.lower()
        self.package = package
        # Ensure dates are timezone-aware (UTC)
        if start_date.tzinfo is None:
            self.start_date = start_date.replace(tzinfo=timezone.utc)
        else:
            self.start_date = start_date.astimezone(timezone.utc)
            
        if end_date.tzinfo is None:
            self.end_date = end_date.replace(tzinfo=timezone.utc)
        else:
            self.end_date = end_date.astimezone(timezone.utc)
            
        self.weighting_type = weighting_type
        self.half_life = half_life
        self.output_dir = Path(output_dir)
        
        self.osv_builder = OSVBuilder(output_dir)
        self.osv_service = OSVService()
        self._resolver_cache = resolver_cache or ResolverCache(cache_dir=self.output_dir / "cache")
        
        # Registry URLs
        self.registry_urls = {
            "npm": "https://registry.npmjs.org",
            "pypi": "https://pypi.org/pypi"
        }
        if self.ecosystem == "npm":
            self.resolver = NpmResolver(
                package=self.package,
                start_date=self.start_date,
                end_date=self.end_date,
                registry_urls=self.registry_urls,
                cache=self._resolver_cache,
            )
        elif self.ecosystem == "pypi":
            self.resolver = PyPIResolver(
                package=self.package,
                start_date=self.start_date,
                end_date=self.end_date,
                registry_urls=self.registry_urls,
                cache=self._resolver_cache,
            )
        else:
            raise ValueError(f"Unsupported ecosystem: {self.ecosystem}")
    
    def fetch_package_metadata(self, package_name: str) -> Dict:
        """Fetch package metadata from registry.
        
        Args:
            package_name: Name of the package
            
        Returns:
            Package metadata as dictionary
        """
        return self.resolver.fetch_package_metadata(package_name)
    
    def get_package_version_at_date(self, metadata: Dict) -> Tuple[str, Dict]:
        """Get package version closest to end_date but before it.
        
        Args:
            metadata: Package metadata
            
        Returns:
            Tuple of (version string, version data)
        """
        return self.resolver.get_package_version_at_date(metadata)
    
    def extract_dependencies(self, version_data: Dict) -> Dict[str, str]:
        """Extract dependencies from version data.
        
        Args:
            version_data: Version data from metadata
            
        Returns:
            Dictionary mapping dependency names to constraints
        """
        return self.resolver.extract_dependencies(version_data)

    def _get_latest_package_version_data(self, metadata: Dict) -> Tuple[str, Dict]:
        """Get latest package version and its metadata (regardless of end_date)."""
        if self.ecosystem == "npm":
            latest_version = None
            latest_date = None
            versions = metadata.get("versions", {})
            for ver, ver_data in versions.items():
                published = ver_data.get("dist", {}).get("published")
                if not published:
                    continue
                pub_date = parse_timestamp(published)
                if pub_date is None:
                    continue
                if latest_date is None or pub_date > latest_date:
                    latest_date = pub_date
                    latest_version = ver
            if latest_version is None:
                raise ValueError("No versions found in npm metadata.")
            return latest_version, versions.get(latest_version, {})

        if self.ecosystem == "pypi":
            latest_version = None
            latest_date = None
            releases = metadata.get("releases", {})
            for ver, release_files in releases.items():
                if not release_files:
                    continue
                for file_info in release_files:
                    upload_time = file_info.get("upload_time")
                    if not upload_time:
                        continue
                    pub_date = parse_timestamp(upload_time)
                    if pub_date is None:
                        continue
                    if latest_date is None or pub_date > latest_date:
                        latest_date = pub_date
                        latest_version = ver
            if latest_version is None:
                raise ValueError("No versions found in PyPI metadata.")
            try:
                version_metadata = self.resolver._get_pypi_version_metadata(self.package, latest_version)
                version_data = {
                    "upload_time": latest_date.isoformat() if latest_date else None,
                    "requires_dist": version_metadata.get("info", {}).get("requires_dist", []),
                }
            except Exception:
                version_data = {
                    "upload_time": latest_date.isoformat() if latest_date else None,
                    "requires_dist": [],
                }
            return latest_version, version_data

        raise ValueError(f"Unsupported ecosystem: {self.ecosystem}")

    def analyze_bulk_rows(
        self,
        rows: List[Dict[str, Any]],
        osv_df: Optional[pd.DataFrame] = None,
    ) -> List[Dict[str, Any]]:
        """Analyze multiple rows for a single package using latest dependencies."""
        if not rows:
            return []

        # Parse and normalize dates
        parsed_rows = []
        for row in rows:
            start_date = row["start_date"]
            end_date = row["end_date"]
            if start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=timezone.utc)
            else:
                start_date = start_date.astimezone(timezone.utc)
            if end_date.tzinfo is None:
                end_date = end_date.replace(tzinfo=timezone.utc)
            else:
                end_date = end_date.astimezone(timezone.utc)
            parsed_rows.append({**row, "start_date": start_date, "end_date": end_date})

        min_start = min(row["start_date"] for row in parsed_rows)
        max_end = max(row["end_date"] for row in parsed_rows)

        # Override analyzer range for bulk timeline computation
        original_start = self.start_date
        original_end = self.end_date
        self.start_date = min_start
        self.end_date = max_end
        self.resolver.start_date = min_start
        self.resolver.end_date = max_end

        # Fetch package metadata and latest dependencies
        pkg_metadata = self.fetch_package_metadata(self.package)
        latest_pkg_version, latest_version_data = self._get_latest_package_version_data(pkg_metadata)
        dependencies = self.extract_dependencies(latest_version_data)

        if osv_df is None:
            osv_db_file = self.output_dir / "osv_database.parquet"
            if osv_db_file.exists():
                osv_df = pd.read_parquet(osv_db_file)
            else:
                osv_df = pd.DataFrame()
        if len(osv_df) > 0:
            osv_df = osv_df[osv_df["ecosystem"] == self.ecosystem.upper()].copy()

        pkg_versions = self.get_all_versions_with_dates(pkg_metadata, package_name=self.package)
        pkg_versions = [(ver, date) for ver, date in pkg_versions]
        pkg_versions.sort(key=lambda item: item[1])

        pkg_version_at_date = {}
        for ver, date in pkg_versions:
            pkg_version_at_date[date] = ver

        # Precompute package version available at each interval start
        def _package_version_for_date(at_date: datetime) -> Optional[str]:
            available = [(ver, d) for ver, d in pkg_versions if d <= at_date]
            if not available:
                return None
            if self.ecosystem == "npm":
                semver_candidates = []
                for ver, _ in available:
                    key = npm_semver_key(ver)
                    if key is not None:
                        semver_candidates.append((key, ver))
                if semver_candidates:
                    semver_candidates.sort(key=lambda item: item[0])
                    return semver_candidates[-1][1]
                return available[-1][0]
            try:
                available.sort(key=lambda item: pkg_version.parse(item[0]))
                return available[-1][0]
            except Exception:
                return available[-1][0]

        dep_cache: Dict[str, Dict[datetime, Dict[str, Any]]] = {}
        dep_metadata_cache: Dict[str, Dict] = {}
        dep_dates_cache: Dict[str, List[datetime]] = {}

        # Precompute dependency timelines and per-date resolution
        for dep_name, dep_constraint in dependencies.items():
            dep_metadata = self.fetch_package_metadata(dep_name)
            dep_metadata_cache[dep_name] = dep_metadata
            dep_versions = self.get_all_versions_with_dates(dep_metadata, package_name=dep_name)
            dep_versions = [(ver, date) for ver, date in dep_versions]

            dates = []
            for _, date in pkg_versions:
                if min_start <= date <= max_end:
                    dates.append(date)
            for _, date in dep_versions:
                if min_start <= date <= max_end:
                    dates.append(date)

            dates = sorted(set(dates))
            dep_dates_cache[dep_name] = dates

            per_date = {}
            for date in dates:
                dep_version = self.resolve_dependency_version(dep_name, dep_constraint, date)
                highest_dep_version = self.get_highest_semver_version_at_date(
                    dep_name, date, metadata=dep_metadata
                )
                updated = (
                    dep_version == highest_dep_version
                    if dep_version and highest_dep_version
                    else False
                )
                remediated = self._check_remediation(
                    dep_name,
                    dep_version,
                    date,
                    osv_df if osv_df is not None else pd.DataFrame(),
                    dep_metadata,
                )
                per_date[date] = {
                    "dependency_version": dep_version,
                    "dependency_highest_version": highest_dep_version,
                    "updated": updated,
                    "remediated": remediated,
                }
            dep_cache[dep_name] = per_date

        results = []
        for row in parsed_rows:
            start_date = row["start_date"]
            end_date = row["end_date"]
            ttu_values = []
            ttr_values = []
            dep_frames = []

            original_start_row = self.start_date
            original_end_row = self.end_date
            self.start_date = start_date
            self.end_date = end_date

            for dep_name, dep_constraint in dependencies.items():
                dates = [d for d in dep_dates_cache[dep_name] if start_date <= d <= end_date]
                intervals = build_intervals(dates, start_date, end_date)
                if not intervals:
                    continue
                records = []
                for interval_start, interval_end in intervals:
                    info = dep_cache[dep_name].get(interval_start)
                    if info is None:
                        continue
                    age_of_interval = (end_date - interval_start).days
                    weight = self.calculate_weight(age_of_interval)
                    pkg_version_at_interval = _package_version_for_date(interval_start)
                    records.append({
                        "ecosystem": self.ecosystem,
                        "package": self.package,
                        "package_version": pkg_version_at_interval or latest_pkg_version,
                        "dependency": dep_name,
                        "dependency_constraint": dep_constraint,
                        "dependency_version": info["dependency_version"],
                        "dependency_highest_version": info["dependency_highest_version"],
                        "interval_start": interval_start,
                        "interval_end": interval_end,
                        "updated": info["updated"],
                        "remediated": info["remediated"],
                        "age_of_interval": age_of_interval,
                        "weight": weight,
                        "analysis_end_date": end_date,
                    })
                dep_df = pd.DataFrame(records)
                if len(dep_df) == 0:
                    continue
                ttu, ttr = self.calculate_ttu_ttr(dep_df)
                ttu_values.append(ttu)
                ttr_values.append(ttr)
                dep_frames.append(dep_df)

            self.start_date = original_start_row
            self.end_date = original_end_row

            avg_ttu = sum(ttu_values) / len(ttu_values) if ttu_values else 0.0
            avg_ttr = sum(ttr_values) / len(ttr_values) if ttr_values else 0.0

            results.append({
                "row_num": row["row_num"],
                "summary": {
                    "ecosystem": self.ecosystem,
                    "package_name": self.package,
                    "start_date": start_date.date().isoformat(),
                    "end_date": end_date.date().isoformat(),
                    "mttu": avg_ttu,
                    "mttr": avg_ttr,
                    "num_dependencies": len(dependencies),
                    "status": "ok",
                    "error": "",
                },
                "dependency_frames": dep_frames,
            })

        self.start_date = original_start
        self.end_date = original_end
        self.resolver.start_date = original_start
        self.resolver.end_date = original_end

        return results
    
    def get_all_versions_with_dates(self, metadata: Dict, package_name: Optional[str] = None) -> List[Tuple[str, datetime]]:
        """Get all versions and their release dates within the date range.
        
        Args:
            metadata: Package metadata
            package_name: Package name (for npm view time command)
            
        Returns:
            List of (version, date) tuples
        """
        versions = self.resolver.get_all_versions_with_dates(metadata, package_name=package_name)
        return [(item.version, item.released_at) for item in versions]
    
    def resolve_dependency_version(
        self, 
        dependency: str, 
        constraint: str, 
        before_date: datetime
    ) -> Optional[str]:
        """Resolve dependency version based on constraint and date.
        
        Args:
            dependency: Dependency name
            constraint: Version constraint
            before_date: Resolve version available before this date
            
        Returns:
            Resolved version or None
        """
        return self.resolver.resolve_dependency_version(dependency, constraint, before_date)
    
    def get_highest_semver_version_at_date(
        self, 
        package_name: str, 
        at_date: datetime,
        metadata: Optional[Dict] = None
    ) -> Optional[str]:
        """Get highest SEMVER version available at a specific date.
        
        Args:
            package_name: Package name to check
            at_date: Date to check versions
            metadata: Optional pre-fetched package metadata
            
        Returns:
            Highest SEMVER version or None
        """
        return self.resolver.get_highest_semver_version_at_date(
            package_name, at_date, metadata=metadata
        )
    
    def calculate_weight(self, age_of_interval: float) -> float:
        """Calculate weight based on age and weighting type.
        
        Args:
            age_of_interval: Age in days
            
        Returns:
            Weight value
        """
        if self.weighting_type == "disable":
            return 1.0
        elif self.weighting_type == "linear":
            max_age = (self.end_date - self.start_date).days
            return 1.0 - (age_of_interval / max_age) if max_age > 0 else 1.0
        elif self.weighting_type == "exponential":
            if self.half_life is None:
                raise ValueError("Half-life required for exponential weighting")
            lambda_val = math.log(2) / self.half_life
            return math.exp(-lambda_val * age_of_interval)
        elif self.weighting_type == "inverse":
            return 1.0 / (1.0 + age_of_interval)
        
        return 1.0
    
    def analyze_dependency(
        self, 
        dependency: str, 
        pkg_metadata: Dict,
        dep_metadata: Dict,
        osv_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Analyze a single dependency across all intervals.
        
        For each interval (defined by unique release dates of both package and dependency),
        we find the highest available package version, get its constraint for this dependency,
        and resolve the dependency version using npm --before.
        
        Args:
            dependency: Dependency name
            pkg_metadata: Parent package metadata
            dep_metadata: Dependency metadata
            osv_df: OSV vulnerability dataframe
            
        Returns:
            DataFrame with analysis results
        """
        logger.info(f"Analyzing dependency: {dependency}")
        
        # Get all version release dates for the package (parent)
        pkg_versions = self.get_all_versions_with_dates(pkg_metadata, package_name=self.package)
        
        # Get all version release dates for the dependency
        dep_versions = self.get_all_versions_with_dates(dep_metadata, package_name=dependency)
        
        # Determine effective start date: max of analysis start_date and first release dates
        effective_start = self.start_date
        if pkg_versions:
            first_pkg_date = pkg_versions[0][1]
            effective_start = max(effective_start, first_pkg_date)
        if dep_versions:
            first_dep_date = dep_versions[0][1]
            effective_start = max(effective_start, first_dep_date)
        
        dates = []
        for _, date in pkg_versions:
            if effective_start <= date <= self.end_date:
                dates.append(date)
        for _, date in dep_versions:
            if effective_start <= date <= self.end_date:
                dates.append(date)

        intervals = build_intervals(dates, effective_start, self.end_date)
        if not intervals:
            return pd.DataFrame()
        
        # Build lookup for package versions: date -> (version, constraint for this dependency)
        pkg_version_info = []  # List of (version, date, constraint_for_dep)
        for ver, date in pkg_versions:
            # Get dependencies for this version
            if self.ecosystem == "npm":
                ver_data = pkg_metadata.get('versions', {}).get(ver, {})
                deps = ver_data.get('dependencies', {})
            elif self.ecosystem == "pypi":
                deps = self.resolver.get_version_dependencies(self.package, ver)
            else:
                deps = {}
            
            constraint = deps.get(dependency, None)
            if constraint is not None:
                pkg_version_info.append((ver, date, constraint))
        
        # Sort by date
        pkg_version_info.sort(key=lambda x: x[1])
        
        records = []
        for interval_start, interval_end in intervals:
            
            # Find highest SEMVER package version available at interval_start
            # Collect all versions released before or at interval_start
            available_versions = []
            for ver, date, constraint in pkg_version_info:
                if date <= interval_start:
                    available_versions.append((ver, constraint))
            
            if not available_versions:
                continue
            
            # Sort by semantic version and pick the highest
            if self.ecosystem == "npm":
                semver_candidates = []
                for ver, constraint in available_versions:
                    key = npm_semver_key(ver)
                    if key is not None:
                        semver_candidates.append((key, ver, constraint))
                if semver_candidates:
                    semver_candidates.sort(key=lambda item: item[0])
                    _, pkg_version_at_interval, constraint_at_interval = semver_candidates[-1]
                else:
                    pkg_version_at_interval, constraint_at_interval = available_versions[-1]
            else:
                try:
                    available_versions.sort(key=lambda x: pkg_version.parse(x[0]))
                    pkg_version_at_interval, constraint_at_interval = available_versions[-1]
                except Exception:
                    # Fallback to last by date if semver parsing fails
                    pkg_version_at_interval, constraint_at_interval = available_versions[-1]
            
            # Skip if no constraint for this dependency
            if constraint_at_interval is None:
                continue
            
            # Resolve dependency version at this interval using the constraint
            dep_version = self.resolve_dependency_version(
                dependency, constraint_at_interval, interval_start
            )
            
            # Get highest SEMVER dependency version available at interval_start
            highest_dep_version = self.get_highest_semver_version_at_date(
                dependency, interval_start, metadata=dep_metadata
            )
            
            # Check if updated (dependency is at highest available version)
            updated = (dep_version == highest_dep_version) if dep_version and highest_dep_version else False
            
            # Calculate age
            age_of_interval = (self.end_date - interval_start).days
            
            # Calculate weight
            weight = self.calculate_weight(age_of_interval)
            
            # Check remediation status
            remediated = self._check_remediation(
                dependency, dep_version, interval_start, osv_df, dep_metadata
            )
            
            records.append({
                'ecosystem': self.ecosystem,
                'package': self.package,
                'package_version': pkg_version_at_interval,
                'dependency': dependency,
                'dependency_constraint': constraint_at_interval,
                'dependency_version': dep_version,
                'dependency_highest_version': highest_dep_version,
                'interval_start': interval_start,
                'interval_end': interval_end,
                'updated': updated,
                'remediated': remediated,
                'age_of_interval': age_of_interval,
                'weight': weight
            })
        
        return pd.DataFrame(records)

    def _get_pypi_version_dependencies(self, package: str, version: str) -> Dict[str, str]:
        """Backward-compatible wrapper for resolver access."""
        return self.resolver.get_version_dependencies(package, version)
    
    def _check_remediation(
        self,
        dependency: str,
        dep_version: Optional[str],
        interval_start: datetime,
        osv_df: pd.DataFrame,
        dep_metadata: Dict
    ) -> bool:
        """Check if dependency version is remediated from vulnerabilities.
        
        Args:
            dependency: Dependency name
            dep_version: Current dependency version
            interval_start: Start of interval
            osv_df: OSV vulnerability dataframe
            dep_metadata: Dependency metadata
            
        Returns:
            True if remediated, False otherwise
        """
        return self.osv_service.is_remediated(
            dependency=dependency,
            dependency_version=dep_version,
            interval_start=interval_start,
            osv_df=osv_df,
            dependency_metadata=dep_metadata,
            ecosystem=self.ecosystem,
        )
    
    def _get_version_release_date(
        self, 
        package: str, 
        version: str, 
        metadata: Dict
    ) -> Optional[datetime]:
        """Get release date for a specific version.
        
        Args:
            package: Package name
            version: Version string
            metadata: Package metadata
            
        Returns:
            Release date or None
        """
        return self.osv_service.get_version_release_date(self.ecosystem, package, version, metadata)
    
    def calculate_ttu_ttr(self, df: pd.DataFrame) -> Tuple[float, float]:
        """Calculate TTU and TTR metrics.
        
        Args:
            df: DataFrame with dependency analysis
            
        Returns:
            Tuple of (TTU, TTR) in days
        """
        if len(df) == 0:
            return 0.0, 0.0
        
        # Calculate interval duration
        df['interval_duration'] = (df['interval_end'] - df['interval_start']).dt.total_seconds() / 86400
        
        # Calculate TTU
        not_updated = df[df['updated'] == False]
        if self.weighting_type != "disable" and len(not_updated) > 0:
            ttu = (not_updated['weight'] * not_updated['interval_duration']).sum() / not_updated['weight'].sum()
        else:
            ttu = not_updated['interval_duration'].sum() if len(not_updated) > 0 else 0.0
        
        # Calculate TTR
        not_remediated = df[df['remediated'] == False]
        if self.weighting_type != "disable" and len(not_remediated) > 0:
            ttr = (not_remediated['weight'] * not_remediated['interval_duration']).sum() / not_remediated['weight'].sum()
        else:
            ttr = not_remediated['interval_duration'].sum() if len(not_remediated) > 0 else 0.0
        
        return ttu, ttr
    
    def analyze(self, osv_df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """Run complete analysis.
        
        Returns:
            Dictionary with analysis results
        """
        # Fetch package metadata
        print(f"Fetching metadata for {self.package}...")
        pkg_metadata = self.fetch_package_metadata(self.package)
        
        # Get package version at end_date
        pkg_version, version_data = self.get_package_version_at_date(pkg_metadata)
        print(f"Analyzing version {pkg_version}")
        
        # Extract dependencies
        dependencies = self.extract_dependencies(version_data)
        print(f"Found {len(dependencies)} dependencies")
        
        if len(dependencies) == 0:
            return {
                'package': self.package,
                'version': pkg_version,
                'ttu': 0.0,
                'ttr': 0.0,
                'num_dependencies': 0
            }
        
        # Load or build OSV database
        if osv_df is None:
            osv_db_file = self.output_dir / "osv_database.parquet"
            if osv_db_file.exists():
                osv_df = pd.read_parquet(osv_db_file)
            else:
                osv_df = pd.DataFrame()

        if len(osv_df) > 0:
            osv_df = osv_df[osv_df['ecosystem'] == self.ecosystem.upper()].copy()
        
        # Analyze each dependency
        all_deps_data = {}
        ttu_values = []
        ttr_values = []
        
        for dep_name, dep_constraint in dependencies.items():
            print(f"  Analyzing {dep_name}...")
            
            try:
                # Fetch dependency metadata
                dep_metadata = self.fetch_package_metadata(dep_name)
                
                # Analyze dependency (pass package metadata for interval creation)
                dep_df = self.analyze_dependency(
                    dep_name, 
                    pkg_metadata,
                    dep_metadata,
                    osv_df
                )
                
                # Calculate metrics
                ttu, ttr = self.calculate_ttu_ttr(dep_df)
                ttu_values.append(ttu)
                ttr_values.append(ttr)
                
                all_deps_data[dep_name] = dep_df
                
            except Exception as e:
                import traceback
                logger.error(f"Error analyzing {dep_name}: {e}")
                logger.error(traceback.format_exc())
                print(f"    Error: {e}")
                if "Error getting highest semver version for" in str(e):
                    raise
                continue
        
        # Calculate averages
        avg_ttu = sum(ttu_values) / len(ttu_values) if ttu_values else 0.0
        avg_ttr = sum(ttr_values) / len(ttr_values) if ttr_values else 0.0
        
        # Prepare results
        results = {
            'package': self.package,
            'ecosystem': self.ecosystem,
            'version': pkg_version,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'weighting_type': self.weighting_type,
            'half_life': self.half_life,
            'ttu': avg_ttu,
            'ttr': avg_ttr,
            'num_dependencies': len(dependencies),
            'dependency_data': all_deps_data
        }
        
        # Add OSV data if available
        if len(osv_df) > 0:
            dep_names = list(dependencies.keys())
            osv_filtered = osv_df[osv_df['package'].isin(dep_names)]
            results['osv_data'] = osv_filtered
        
        return results
