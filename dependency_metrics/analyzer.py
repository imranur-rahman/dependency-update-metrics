"""
Core dependency analyzer for calculating TTU and TTR metrics.
"""

import json
import logging
import math
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd
import requests
from packaging import version as pkg_version
from packaging.specifiers import SpecifierSet

from .osv_builder import OSVBuilder


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
        output_dir: Path = Path("./output")
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
        
        # Registry URLs
        self.registry_urls = {
            "npm": "https://registry.npmjs.org",
            "pypi": "https://pypi.org/pypi"
        }
    
    def fetch_package_metadata(self, package_name: str) -> Dict:
        """Fetch package metadata from registry.
        
        Args:
            package_name: Name of the package
            
        Returns:
            Package metadata as dictionary
        """
        if self.ecosystem == "npm":
            url = f"{self.registry_urls['npm']}/{package_name}"
        elif self.ecosystem == "pypi":
            url = f"{self.registry_urls['pypi']}/{package_name}/json"
        else:
            raise ValueError(f"Unsupported ecosystem: {self.ecosystem}")
        
        logger.info(f"Fetching metadata for {package_name}")
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    def get_package_version_at_date(self, metadata: Dict) -> Tuple[str, Dict]:
        """Get package version closest to end_date but before it.
        
        Args:
            metadata: Package metadata
            
        Returns:
            Tuple of (version string, version data)
        """
        if self.ecosystem == "npm":
            # Use npm view time command for more reliable date fetching
            return self._get_npm_version_at_date(metadata)
        elif self.ecosystem == "pypi":
            return self._get_pypi_version_at_date(metadata)
        else:
            raise ValueError(f"Unsupported ecosystem: {self.ecosystem}")
    
    def _get_npm_version_at_date(self, metadata: Dict) -> Tuple[str, Dict]:
        """Get npm package version at end_date using npm view time.
        
        Args:
            metadata: Package metadata
            
        Returns:
            Tuple of (version string, version data)
        """
        try:
            # Use npm view to get all version timestamps
            cmd = ['npm', 'view', self.package, 'time', '--json']
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=30
            )
            
            if result.returncode == 0:
                time_data = json.loads(result.stdout)
                # Remove metadata entries
                time_data.pop('modified', None)
                time_data.pop('created', None)
                
                valid_versions = []
                for ver, timestamp in time_data.items():
                    try:
                        pub_date = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        if pub_date <= self.end_date:
                            valid_versions.append((ver, pub_date))
                    except (ValueError, AttributeError):
                        continue
                
                if valid_versions:
                    # Sort by date and get the latest
                    valid_versions.sort(key=lambda x: x[1], reverse=True)
                    latest_version = valid_versions[0][0]
                    
                    # Get version data from metadata
                    versions = metadata.get('versions', {})
                    version_data = versions.get(latest_version, {})
                    return latest_version, version_data
        except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as e:
            logger.warning(f"npm view time failed, falling back to metadata parsing: {e}")
        
        # Fallback to parsing metadata directly
        return self._parse_versions_from_metadata(metadata)
    
    def _get_pypi_version_at_date(self, metadata: Dict) -> Tuple[str, Dict]:
        """Get PyPI package version at end_date.
        
        Args:
            metadata: Package metadata
            
        Returns:
            Tuple of (version string, version data)
        """
        releases = metadata.get('releases', {})
        valid_versions = []
        
        for ver, release_files in releases.items():
            if not release_files:
                continue
            
            # Get upload time from first file
            upload_time = release_files[0].get('upload_time')
            if not upload_time:
                continue
            
            try:
                pub_date = datetime.fromisoformat(upload_time.replace('Z', '+00:00'))
                if pub_date <= self.end_date:
                    valid_versions.append((ver, pub_date))
            except (ValueError, AttributeError):
                continue
        
        if not valid_versions:
            raise ValueError(f"No versions found before {self.end_date}")
        
        # Sort by date and get the latest
        valid_versions.sort(key=lambda x: x[1], reverse=True)
        latest_version = valid_versions[0][0]
        
        # For PyPI, we need to fetch the specific version info
        try:
            version_url = f"{self.registry_urls['pypi']}/{self.package}/{latest_version}/json"
            response = requests.get(version_url)
            response.raise_for_status()
            version_metadata = response.json()
            
            # Extract version data with dependencies
            version_data = {
                'upload_time': valid_versions[0][1].isoformat(),
                'requires_dist': version_metadata.get('info', {}).get('requires_dist', [])
            }
            return latest_version, version_data
        except Exception as e:
            logger.warning(f"Failed to fetch version-specific data: {e}")
            # Return basic version data without dependencies
            return latest_version, {'upload_time': valid_versions[0][1].isoformat(), 'requires_dist': []}
    
    def _parse_versions_from_metadata(self, metadata: Dict) -> Tuple[str, Dict]:
        """Fallback method to parse versions from metadata.
        
        Args:
            metadata: Package metadata
            
        Returns:
            Tuple of (version string, version data)
        """
        versions = metadata.get('versions', {})
        valid_versions = []
        
        for ver, ver_data in versions.items():
            # Get publish date
            published = ver_data.get('dist', {}).get('published')
            if not published:
                continue
            
            try:
                pub_date = datetime.fromisoformat(published.replace('Z', '+00:00'))
                if pub_date <= self.end_date:
                    valid_versions.append((ver, pub_date, ver_data))
            except (ValueError, AttributeError):
                continue
        
        if not valid_versions:
            raise ValueError(f"No versions found before {self.end_date}")
        
        # Sort by date and get the latest
        valid_versions.sort(key=lambda x: x[1], reverse=True)
        return valid_versions[0][0], valid_versions[0][2]
    
    def extract_dependencies(self, version_data: Dict) -> Dict[str, str]:
        """Extract dependencies from version data.
        
        Args:
            version_data: Version data from metadata
            
        Returns:
            Dictionary mapping dependency names to constraints
        """
        if self.ecosystem == "npm":
            return version_data.get('dependencies', {})
        elif self.ecosystem == "pypi":
            # PyPI stores dependencies differently
            requires_dist = version_data.get('requires_dist', [])
            deps = {}
            for req in requires_dist or []:
                # Parse requirement string
                parts = req.split(';')[0].strip()  # Remove environment markers
                if '(' in parts or '[' in parts:
                    continue  # Skip extras
                
                if ' ' in parts:
                    name, constraint = parts.split(' ', 1)
                    deps[name] = constraint
                else:
                    deps[parts] = '*'
            return deps
        
        return {}
    
    def get_all_versions_with_dates(self, metadata: Dict, package_name: Optional[str] = None) -> List[Tuple[str, datetime]]:
        """Get all versions and their release dates within the date range.
        
        Args:
            metadata: Package metadata
            package_name: Package name (for npm view time command)
            
        Returns:
            List of (version, date) tuples
        """
        if self.ecosystem == "npm" and package_name:
            # Try using npm view time for more reliable date fetching
            try:
                cmd = ['npm', 'view', package_name, 'time', '--json']
                result = subprocess.run(
                    cmd, 
                    capture_output=True, 
                    text=True, 
                    timeout=30
                )
                
                if result.returncode == 0:
                    time_data = json.loads(result.stdout)
                    # Remove metadata entries
                    time_data.pop('modified', None)
                    time_data.pop('created', None)
                    
                    version_dates = []
                    for ver, timestamp in time_data.items():
                        try:
                            pub_date = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                            if self.start_date <= pub_date <= self.end_date:
                                version_dates.append((ver, pub_date))
                        except (ValueError, AttributeError):
                            continue
                    
                    return sorted(version_dates, key=lambda x: x[1])
            except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as e:
                logger.warning(f"npm view time failed for {package_name}, falling back to metadata: {e}")
        
        # Fallback to parsing metadata
        if self.ecosystem == "npm":
            versions = metadata.get('versions', {})
        elif self.ecosystem == "pypi":
            versions = metadata.get('releases', {})
        else:
            versions = {}
        
        version_dates = []
        
        for ver, ver_data in versions.items():
            if self.ecosystem == "npm":
                published = ver_data.get('dist', {}).get('published')
            elif self.ecosystem == "pypi":
                # For PyPI, ver_data is a list of release files
                if isinstance(ver_data, list) and ver_data:
                    published = ver_data[0].get('upload_time')
                else:
                    published = ver_data.get('upload_time') if isinstance(ver_data, dict) else None
            else:
                published = None
            
            if not published:
                continue
            
            try:
                pub_date = datetime.fromisoformat(published.replace('Z', '+00:00'))
                if self.start_date <= pub_date <= self.end_date:
                    version_dates.append((ver, pub_date))
            except (ValueError, AttributeError):
                continue
        
        return sorted(version_dates, key=lambda x: x[1])
    
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
        if self.ecosystem == "npm":
            return self._resolve_npm_version(dependency, constraint, before_date)
        elif self.ecosystem == "pypi":
            return self._resolve_pypi_version(dependency, constraint, before_date)
        
        return None
    
    def _resolve_npm_version(
        self, 
        dependency: str, 
        constraint: str, 
        before_date: datetime
    ) -> Optional[str]:
        """Resolve NPM dependency version."""
        try:
            # Use npm view with --before flag
            cmd = [
                'npm', 'view', 
                f'{dependency}@{constraint}',
                'version',
                '--json',
                '--before', before_date.isoformat()
            ]
            
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=30
            )
            
            if result.returncode == 0:
                output = result.stdout.strip()
                if output:
                    versions = json.loads(output)
                    if isinstance(versions, list):
                        return versions[-1]  # Return latest matching version
                    return versions
        except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as e:
            logger.warning(f"Error resolving npm version for {dependency}: {e}")
        
        return None
    
    def _resolve_pypi_version(
        self, 
        dependency: str, 
        constraint: str, 
        before_date: datetime
    ) -> Optional[str]:
        """Resolve PyPI dependency version."""
        try:
            metadata = self.fetch_package_metadata(dependency)
            versions = metadata.get('releases', {})
            
            # Filter versions by date and constraint
            spec = SpecifierSet(constraint) if constraint != '*' else None
            valid_versions = []
            
            for ver, releases in versions.items():
                if not releases:
                    continue
                
                # Get upload date
                upload_date_str = releases[0].get('upload_time')
                if not upload_date_str:
                    continue
                
                upload_date = datetime.fromisoformat(upload_date_str.replace('Z', '+00:00'))
                if upload_date > before_date:
                    continue
                
                # Check constraint
                try:
                    parsed_ver = pkg_version.parse(ver)
                    if spec is None or parsed_ver in spec:
                        valid_versions.append((ver, upload_date))
                except Exception:
                    continue
            
            if valid_versions:
                # Sort and return latest
                valid_versions.sort(key=lambda x: (x[1], pkg_version.parse(x[0])))
                return valid_versions[-1][0]
        
        except Exception as e:
            logger.warning(f"Error resolving pypi version for {dependency}: {e}")
        
        return None
    
    def get_highest_version_at_date(
        self, 
        dependency: str, 
        at_date: datetime
    ) -> Optional[str]:
        """Get highest available version at a specific date.
        
        Args:
            dependency: Dependency name
            at_date: Date to check versions
            
        Returns:
            Highest version or None
        """
        try:
            metadata = self.fetch_package_metadata(dependency)
            
            # Get all versions up to at_date without modifying instance state
            if self.ecosystem == "npm":
                # Try npm view time first
                try:
                    cmd = ['npm', 'view', dependency, 'time', '--json']
                    result = subprocess.run(
                        cmd, 
                        capture_output=True, 
                        text=True, 
                        timeout=30
                    )
                    
                    if result.returncode == 0:
                        time_data = json.loads(result.stdout)
                        time_data.pop('modified', None)
                        time_data.pop('created', None)
                        
                        valid_versions = []
                        for ver, timestamp in time_data.items():
                            try:
                                pub_date = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                                if pub_date <= at_date:
                                    valid_versions.append(ver)
                            except (ValueError, AttributeError):
                                continue
                        
                        if valid_versions:
                            valid_versions.sort(key=lambda v: pkg_version.parse(v))
                            return valid_versions[-1]
                except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as e:
                    logger.warning(f"npm view time failed for {dependency}, falling back to metadata: {e}")
                
                # Fallback to metadata parsing
                versions = metadata.get('versions', {})
                valid_versions = []
                
                for ver, ver_data in versions.items():
                    published = ver_data.get('dist', {}).get('published')
                    if not published:
                        continue
                    
                    pub_date = datetime.fromisoformat(published.replace('Z', '+00:00'))
                    if pub_date <= at_date:
                        valid_versions.append(ver)
                
                if valid_versions:
                    valid_versions.sort(key=lambda v: pkg_version.parse(v))
                    return valid_versions[-1]
            
            elif self.ecosystem == "pypi":
                releases = metadata.get('releases', {})
                valid_versions = []
                
                for ver, release_files in releases.items():
                    if not release_files:
                        continue
                    
                    upload_time = release_files[0].get('upload_time')
                    if not upload_time:
                        continue
                    
                    upload_date = datetime.fromisoformat(upload_time.replace('Z', '+00:00'))
                    if upload_date <= at_date:
                        valid_versions.append(ver)
                
                if valid_versions:
                    valid_versions.sort(key=lambda v: pkg_version.parse(v))
                    return valid_versions[-1]
        
        except Exception as e:
            logger.warning(f"Error getting highest version for {dependency}: {e}")
        
        return None
    
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
        constraint: str,
        package_version: str,
        dep_metadata: Dict,
        osv_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Analyze a single dependency.
        
        Args:
            dependency: Dependency name
            constraint: Version constraint
            package_version: Parent package version
            dep_metadata: Dependency metadata
            osv_df: OSV vulnerability dataframe
            
        Returns:
            DataFrame with analysis results
        """
        logger.info(f"Analyzing dependency: {dependency}")
        
        # Get all version release dates for the dependency
        dep_versions = self.get_all_versions_with_dates(dep_metadata, package_name=dependency)
        
        # Create intervals
        intervals = []
        interval_dates = [self.start_date]
        
        # Add dependency version release dates
        for ver, date in dep_versions:
            if date not in interval_dates:
                interval_dates.append(date)
        
        # Sort and create intervals
        interval_dates.sort()
        
        records = []
        for i in range(len(interval_dates) - 1):
            interval_start = interval_dates[i]
            interval_end = interval_dates[i + 1]
            
            # Resolve dependency version at this interval
            dep_version = self.resolve_dependency_version(
                dependency, constraint, interval_start
            )
            
            # Get highest available version
            highest_version = self.get_highest_version_at_date(dependency, interval_start)
            
            # Check if updated
            updated = (dep_version == highest_version) if dep_version and highest_version else False
            
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
                'package_version': package_version,
                'dependency': dependency,
                'dependency_constraint': constraint,
                'dependency_version': dep_version,
                'dependency_highest_version': highest_version,
                'interval_start': interval_start,
                'interval_end': interval_end,
                'updated': updated,
                'remediated': remediated,
                'age_of_interval': age_of_interval,
                'weight': weight
            })
        
        # If the interval_start for first record is default (i.e., '1900-01-01 00:00:00')
        # set the weight of that record to 0
        print (records[0])
        if records and records[0]['interval_start'] == datetime(1900, 1, 1, 0, 0, tzinfo=timezone.utc):
            # Remove the default placeholder record
            records.pop(0)
        
        return pd.DataFrame(records)
    
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
        if dep_version is None:
            return False
        
        # Check if OSV dataframe has data
        if len(osv_df) == 0 or 'package' not in osv_df.columns:
            return True  # No vulnerability data available
        
        # Get vulnerabilities for this dependency
        dep_vulns = osv_df[osv_df['package'] == dependency]
        
        if len(dep_vulns) == 0:
            return True  # No vulnerabilities
        
        try:
            current_ver = pkg_version.parse(dep_version)
        except Exception:
            return False
        
        # Check each vulnerability
        for _, vuln in dep_vulns.iterrows():
            try:
                intro_ver = pkg_version.parse(vuln['vul_introduced'])
                fixed_ver = pkg_version.parse(vuln['vul_fixed'])
                
                # Check if current version is in vulnerable range
                if intro_ver <= current_ver < fixed_ver:
                    # Get fixed version release date
                    fixed_date = self._get_version_release_date(
                        dependency, vuln['vul_fixed'], dep_metadata
                    )
                    
                    # If fixed version was available before interval_start, it's not remediated
                    if fixed_date and fixed_date <= interval_start:
                        return False
            except Exception:
                continue
        
        return True
    
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
        try:
            if self.ecosystem == "npm":
                versions = metadata.get('versions', {})
                ver_data = versions.get(version)
                if ver_data:
                    published = ver_data.get('dist', {}).get('published')
                    if published:
                        return datetime.fromisoformat(published.replace('Z', '+00:00'))
            
            elif self.ecosystem == "pypi":
                releases = metadata.get('releases', {})
                release_files = releases.get(version, [])
                if release_files:
                    upload_time = release_files[0].get('upload_time')
                    if upload_time:
                        return datetime.fromisoformat(upload_time.replace('Z', '+00:00'))
        
        except Exception:
            pass
        
        return None
    
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
    
    def analyze(self) -> Dict[str, Any]:
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
        osv_db_file = self.output_dir / "osv_database.parquet"
        if osv_db_file.exists():
            osv_df = pd.read_parquet(osv_db_file)
            osv_df = osv_df[osv_df['ecosystem'] == self.ecosystem.upper()]
        else:
            osv_df = pd.DataFrame()
        
        # Analyze each dependency
        all_deps_data = {}
        ttu_values = []
        ttr_values = []
        
        for dep_name, dep_constraint in dependencies.items():
            print(f"  Analyzing {dep_name}...")
            
            try:
                # Fetch dependency metadata
                dep_metadata = self.fetch_package_metadata(dep_name)
                
                # Analyze dependency
                dep_df = self.analyze_dependency(
                    dep_name, 
                    dep_constraint, 
                    pkg_version,
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
