"""
Dependency Update Metrics Tool

A tool for analyzing time-to-update and time-to-remediate metrics for package dependencies.
"""

__version__ = "0.1.0"
__author__ = "Imranur Rahman"

from .api import AnalysisResult, ReleaseResult, WindowSummary, analyze
from .cli import main
from .models import ProgressEvent

__all__ = [
    "main",
    "analyze",
    "AnalysisResult",
    "WindowSummary",
    "ReleaseResult",
    "ProgressEvent",
]
