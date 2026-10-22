"""
Dependency Update Metrics Tool

A tool for analyzing time-to-update and time-to-remediate metrics for package dependencies.
"""

__version__ = "0.1.0"
__author__ = "Imranur Rahman"

from .api import AnalysisResult, DataSource, Mode, ReleaseResult, WindowSummary, analyze
from .cli import main
from .errors import DataSourceError, NoReleaseInWindowError, PackageNotFoundError
from .models import ProgressEvent, ProgressStage, Status

__all__ = [
    "main",
    "analyze",
    "AnalysisResult",
    "WindowSummary",
    "ReleaseResult",
    "ProgressEvent",
    "Mode",
    "DataSource",
    "Status",
    "ProgressStage",
    "DataSourceError",
    "PackageNotFoundError",
    "NoReleaseInWindowError",
]
