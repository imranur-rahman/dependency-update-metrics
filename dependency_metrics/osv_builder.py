"""
OSV (Open Source Vulnerabilities) database builder.

The builder downloads one OSV zip for each supported ecosystem (npm, PyPI,
crates.io), reads each JSON record directly from the zip, and writes a flat
Parquet index (``osv_database.parquet``).
"""

import json
import logging
import os
import re
import tempfile
import time
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)

OSV_BASE_URL = "https://storage.googleapis.com/osv-vulnerabilities"

# OSV folder names of the ecosystems that the Tool analyzes. The analyzer and
# the CLI use only these three ecosystems, so the builder does not download
# the full all.zip of every ecosystem.
OSV_ECOSYSTEMS = ("npm", "PyPI", "crates.io")

FetchZip = Callable[[str, Path], None]

# Parquet metadata key that holds the UTC build date of the index (ISO format).
_INDEX_DATE_KEY = b"osv_index_date"

# Environment value that names the index file. The "-osv-" container image sets it,
# because a mount on the output dir (/data) would hide an index inside that dir.
OSV_INDEX_ENV = "DEPENDENCY_METRICS_OSV_INDEX"


def osv_index_file(output_dir: Path) -> Path:
    """Return the OSV index file: the file that ``OSV_INDEX_ENV`` names, if it is set,
    else ``<output_dir>/osv_database.parquet``."""
    override = os.environ.get(OSV_INDEX_ENV)
    return Path(override) if override else Path(output_dir) / "osv_database.parquet"


def _normalize_severity(raw: str) -> str:
    """Map raw severity string to normalized label.

    Args:
        raw: Raw severity string (e.g. "CRITICAL", "HIGH", "MODERATE", "MEDIUM", "LOW")

    Returns:
        Normalized severity label: "Critical", "High", "Medium", "Low", or "None"
    """
    upper = raw.upper() if raw else ""
    if upper == "CRITICAL":
        return "Critical"
    elif upper == "HIGH":
        return "High"
    elif upper in ("MODERATE", "MEDIUM"):
        return "Medium"
    elif upper == "LOW":
        return "Low"
    return "None"


def _raw_severity(data: Dict[str, Any], affected: Dict[str, Any]) -> str:
    """Return the raw severity: per-package, then top-level, then a numeric score."""
    raw = affected.get("database_specific", {}).get("severity", "")
    if not raw:
        raw = data.get("database_specific", {}).get("severity", "")
    if raw:
        return raw
    top_severity = data.get("severity", [{}])
    if not top_severity:
        return ""
    score_val = top_severity[0].get("score", "")
    # Only use numeric scores; skip CVSS vector strings
    if not score_val or str(score_val).startswith("CVSS:"):
        return ""
    try:
        score = float(score_val)
    except (ValueError, TypeError):
        return ""
    if score >= 9.0:
        return "CRITICAL"
    if score >= 7.0:
        return "HIGH"
    if score >= 4.0:
        return "MEDIUM"
    return "LOW"


def _records_from_osv_entry(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flatten one OSV JSON entry to index rows, one row per fixed range."""
    records: List[Dict[str, Any]] = []
    vul_id = data.get("id", "")
    for affected in data.get("affected", []):
        if "package" not in affected or "ranges" not in affected:
            continue
        package_name = affected["package"].get("name", "")
        ecosystem = affected["package"].get("ecosystem", "").upper()
        severity = _normalize_severity(_raw_severity(data, affected))
        for range_data in affected["ranges"]:
            if "events" not in range_data:
                continue
            vul_introduced = None
            for event in range_data["events"]:
                if "introduced" in event:
                    vul_introduced = event["introduced"]
                elif "fixed" in event and vul_introduced is not None:
                    records.append(
                        {
                            "vul_id": vul_id,
                            "ecosystem": ecosystem,
                            "package": package_name,
                            "vul_introduced": vul_introduced,
                            "vul_fixed": event["fixed"],
                            "severity": severity,
                        }
                    )
    return records


def download_file(url: str, dest: Path, max_retries: int = 5) -> None:
    """Download ``url`` to ``dest`` with retries; raise RuntimeError on failure."""
    logger.warning(f"Downloading OSV data from {url}")
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()
            total_size = int(response.headers.get("content-length", 0))
            with open(dest, "wb") as f:
                with tqdm(total=total_size, unit="B", unit_scale=True) as pbar:
                    for chunk in response.iter_content(chunk_size=1 << 16):
                        f.write(chunk)
                        pbar.update(len(chunk))
            if not zipfile.is_zipfile(dest):
                raise zipfile.BadZipFile("Downloaded file is not a valid zip")
            return
        except (
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            zipfile.BadZipFile,
        ) as e:
            if dest.exists():
                os.remove(dest)
            if attempt < max_retries:
                wait = 2**attempt
                logger.warning(
                    f"Download attempt {attempt}/{max_retries} failed ({e}); "
                    f"retrying in {wait}s..."
                )
                time.sleep(wait)
            else:
                raise RuntimeError(f"OSV download failed after {max_retries} attempts: {e}") from e


class OSVBuilder:
    """Build and manage OSV vulnerability database."""

    def __init__(
        self,
        output_dir: Path,
        fetch_zip: Optional[FetchZip] = None,
        ecosystems: Iterable[str] = OSV_ECOSYSTEMS,
        clock: Optional[Callable[[], datetime]] = None,
    ):
        """Initialize OSV builder.

        Args:
            output_dir: Directory to store OSV data and database
            fetch_zip: Function that downloads a URL to a path. Tests inject a
                fake; the default downloads over HTTP.
            ecosystems: OSV folder names of the ecosystems to download.
            clock: Returns the current UTC time; the build records its date.
        """
        self.output_dir = Path(output_dir)
        self.osv_db_file = osv_index_file(self.output_dir)
        self._fetch_zip: FetchZip = fetch_zip or download_file
        self.ecosystems = tuple(ecosystems)
        self._clock: Callable[[], datetime] = clock or (lambda: datetime.now(timezone.utc))

    def zip_urls(self) -> List[str]:
        """Return the URL of the OSV zip of each ecosystem."""
        return [f"{OSV_BASE_URL}/{eco}/all.zip" for eco in self.ecosystems]

    def transformation_semver(self, version: str) -> str:
        """Transform version strings to semver format.

        Args:
            version: Version string to transform

        Returns:
            Transformed version string
        """
        if version == "0":
            return "0.0.0"
        elif version.count(".") == 0:
            return version + ".0.0"
        elif re.match(r"(\d+(\.\d*))", version) and version.count(".") == 1:
            return version + ".0"
        else:
            return version

    def parse_osv_zips(self, zip_paths: Iterable[Path]) -> pd.DataFrame:
        """Read each JSON record directly from the zips and build the index rows.

        A record that affects more than one ecosystem is in more than one zip.
        The builder reads each OSV id only once, so the index has no duplicates.

        Returns:
            DataFrame with vulnerability information
        """
        records: List[Dict[str, Any]] = []
        seen_ids: Set[str] = set()
        for zip_path in zip_paths:
            with zipfile.ZipFile(zip_path, "r") as zf:
                names = [n for n in zf.namelist() if n.endswith(".json")]
                for name in tqdm(names, desc=f"Processing {Path(zip_path).name}"):
                    try:
                        data = json.loads(zf.read(name))
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.warning(f"Error processing {name} in {zip_path}: {e}")
                        continue
                    if not isinstance(data, dict):
                        logger.warning(f"Skipping {name} in {zip_path}: not an OSV record")
                        continue
                    vul_id = data.get("id", "")
                    if vul_id and vul_id in seen_ids:
                        continue
                    seen_ids.add(vul_id)
                    records.extend(_records_from_osv_entry(data))

        df = pd.DataFrame(records)
        if len(df) > 0:
            df["vul_introduced"] = df["vul_introduced"].apply(self.transformation_semver)
            df["vul_fixed"] = df["vul_fixed"].apply(self.transformation_semver)
        logger.info(f"Parsed {len(df)} vulnerability records")
        return df

    def _load_existing(self) -> pd.DataFrame:
        logger.info(f"Loading existing OSV database from {self.osv_db_file}")
        osv_df = pd.read_parquet(self.osv_db_file)
        if "severity" not in osv_df.columns:
            osv_df["severity"] = "None"
            logger.warning(
                "OSV database missing 'severity' column — "
                "rebuild with --build-osv for severity support."
            )
        return osv_df

    def build_database(self, force: bool = False) -> pd.DataFrame:
        """Return the OSV index; build it when it is missing or ``force`` is True.

        Returns:
            DataFrame with OSV vulnerability data
        """
        if self.osv_db_file.exists() and not force:
            return self._load_existing()

        index_dir = self.osv_db_file.parent
        index_dir.mkdir(parents=True, exist_ok=True)
        # The zips and the new index go in a temporary folder next to the index,
        # so that the final os.replace is a rename on the same file system.
        with tempfile.TemporaryDirectory(prefix=".osv-build-", dir=index_dir) as tmp:
            tmp_dir = Path(tmp)
            zip_paths: List[Path] = []
            for eco, url in zip(self.ecosystems, self.zip_urls()):
                dest = tmp_dir / f"osv-{eco}.zip"
                self._fetch_zip(url, dest)
                zip_paths.append(dest)
            df = self.parse_osv_zips(zip_paths)
            tmp_index = tmp_dir / self.osv_db_file.name
            self._write_index(df, tmp_index)
            # One rename: a reader sees the old index or the new one, never a partial file.
            os.replace(tmp_index, self.osv_db_file)
        logger.info(f"Saved OSV database to {self.osv_db_file}")
        return df

    def _write_index(self, df: pd.DataFrame, path: Path) -> None:
        """Write the index with its build date (UTC) in the Parquet metadata.

        The date sits in the same file as the rows, so the one rename that
        replaces the index also replaces its date.
        """
        table = pa.Table.from_pandas(df, preserve_index=False)
        metadata = dict(table.schema.metadata or {})
        metadata[_INDEX_DATE_KEY] = self._clock().date().isoformat().encode()
        pq.write_table(table.replace_schema_metadata(metadata), path)

    def index_date(self) -> Optional[date]:
        """Return the UTC date on which the current index was built.

        Returns None when there is no index, or when an older version of the
        Tool built it without a date.
        """
        if not self.osv_db_file.exists():
            return None
        metadata = pq.read_schema(self.osv_db_file).metadata or {}
        raw = metadata.get(_INDEX_DATE_KEY)
        return date.fromisoformat(raw.decode()) if raw else None

    def get_vulnerabilities(self, ecosystem: str, package: str) -> pd.DataFrame:
        """Get vulnerabilities for a specific package.

        Args:
            ecosystem: Ecosystem name (npm, pypi, etc.)
            package: Package name

        Returns:
            DataFrame with vulnerabilities for the package
        """
        if not self.osv_db_file.exists():
            raise FileNotFoundError("OSV database not found. Run with --build-osv first.")

        df = pd.read_parquet(self.osv_db_file)

        # Filter by ecosystem and package
        ecosystem_upper = ecosystem.upper()
        filtered = df[(df["ecosystem"] == ecosystem_upper) & (df["package"] == package)]

        return filtered
