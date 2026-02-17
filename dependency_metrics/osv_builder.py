"""
OSV (Open Source Vulnerabilities) database builder.
"""

import json
import logging
import os
import re
import shutil
import zipfile
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from tqdm import tqdm


logger = logging.getLogger(__name__)


class OSVBuilder:
    """Build and manage OSV vulnerability database."""
    
    OSV_URL = "https://storage.googleapis.com/osv-vulnerabilities/all.zip"
    
    def __init__(self, output_dir: Path):
        """Initialize OSV builder.
        
        Args:
            output_dir: Directory to store OSV data and database
        """
        self.output_dir = Path(output_dir)
        self.osv_dir = self.output_dir / "osv-data"
        self.osv_zip = self.output_dir / "osv-all.zip"
        self.osv_db_file = self.output_dir / "osv_database.parquet"
        
    def download_osv_data(self) -> None:
        """Download OSV vulnerability data."""
        logger.warning(f"Downloading OSV data from {self.OSV_URL}")
        logger.warning("Downloading OSV vulnerability database...")
        
        response = requests.get(self.OSV_URL, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(self.osv_zip, 'wb') as f:
            with tqdm(total=total_size, unit='B', unit_scale=True) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    pbar.update(len(chunk))
        
        logger.warning(f"Downloaded OSV data to {self.osv_zip}")
    
    def extract_osv_data(self) -> None:
        """Extract OSV zip file."""
        logger.warning(f"Extracting OSV data to {self.osv_dir}")
        logger.warning("Extracting OSV data...")
        
        # Remove existing directory if it exists
        if self.osv_dir.exists():
            shutil.rmtree(self.osv_dir)
        
        with zipfile.ZipFile(self.osv_zip, 'r') as zip_ref:
            zip_ref.extractall(self.osv_dir)
        
        logger.warning("Extraction complete")
    
    def transformation_semver(self, version: str) -> str:
        """Transform version strings to semver format.
        
        Args:
            version: Version string to transform
            
        Returns:
            Transformed version string
        """
        if version == '0':
            return '0.0.0'
        elif version.count('.') == 0:
            return version + '.0.0'
        elif re.match(r'(\d+(\.\d*))', version) and version.count('.') == 1:
            return version + '.0'
        else:
            return version
    
    def parse_osv_files(self) -> pd.DataFrame:
        """Parse OSV JSON files and create dataframe.
        
        Returns:
            DataFrame with vulnerability information
        """
        logger.info("Parsing OSV JSON files")
        logger.info("Parsing OSV vulnerability data...")
        
        records = []
        
        # Walk through all JSON files
        json_files = list(self.osv_dir.rglob("*.json"))
        
        for json_file in tqdm(json_files, desc="Processing vulnerabilities"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                vul_id = data.get("id", "")
                
                if 'affected' in data:
                    for affected in data['affected']:
                        if 'package' in affected and 'ranges' in affected:
                            package_name = affected['package'].get('name', '')
                            ecosystem = affected['package'].get('ecosystem', '').upper()
                            
                            ranges = affected['ranges']
                            for range_data in ranges:
                                if 'events' not in range_data:
                                    continue
                                
                                events = range_data['events']
                                vul_introduced = None
                                
                                for event in events:
                                    if 'introduced' in event:
                                        vul_introduced = event['introduced']
                                    elif 'fixed' in event and vul_introduced is not None:
                                        vul_fixed = event['fixed']
                                        
                                        # Add record
                                        records.append({
                                            'vul_id': vul_id,
                                            'ecosystem': ecosystem,
                                            'package': package_name,
                                            'vul_introduced': vul_introduced,
                                            'vul_fixed': vul_fixed
                                        })
                                        
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Error processing {json_file}: {e}")
                continue
        
        df = pd.DataFrame(records)
        
        # Transform version strings
        if len(df) > 0:
            df['vul_introduced'] = df['vul_introduced'].apply(self.transformation_semver)
            df['vul_fixed'] = df['vul_fixed'].apply(self.transformation_semver)
        
        logger.info(f"Parsed {len(df)} vulnerability records")
        return df
    
    def build_database(self) -> pd.DataFrame:
        """Build complete OSV database.
        
        Returns:
            DataFrame with OSV vulnerability data
        """
        # Check if database already exists
        if self.osv_db_file.exists():
            logger.info(f"Loading existing OSV database from {self.osv_db_file}")
            logger.info("Loading existing OSV database...")
            return pd.read_parquet(self.osv_db_file)
        
        # Download and extract if needed
        if not self.osv_zip.exists():
            self.download_osv_data()
        
        if not self.osv_dir.exists():
            self.extract_osv_data()
        
        # Parse OSV files
        df = self.parse_osv_files()
        
        # Save database
        df.to_parquet(self.osv_db_file, index=False)
        logger.info(f"Saved OSV database to {self.osv_db_file}")
        
        # Clean up zip file and extracted data to save space
        if self.osv_zip.exists():
            os.remove(self.osv_zip)
        if self.osv_dir.exists():
            shutil.rmtree(self.osv_dir)
        
        return df
    
    def get_vulnerabilities(self, ecosystem: str, package: str) -> pd.DataFrame:
        """Get vulnerabilities for a specific package.
        
        Args:
            ecosystem: Ecosystem name (npm, pypi, etc.)
            package: Package name
            
        Returns:
            DataFrame with vulnerabilities for the package
        """
        if not self.osv_db_file.exists():
            raise FileNotFoundError(
                "OSV database not found. Run with --build-osv first."
            )
        
        df = pd.read_parquet(self.osv_db_file)
        
        # Filter by ecosystem and package
        ecosystem_upper = ecosystem.upper()
        filtered = df[
            (df['ecosystem'] == ecosystem_upper) & 
            (df['package'] == package)
        ]
        
        return filtered
