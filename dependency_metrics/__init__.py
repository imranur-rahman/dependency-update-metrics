"""
Dependency Update Metrics Tool

A tool for analyzing time-to-update and time-to-remediate metrics for package dependencies.
"""

__version__ = "0.1.0"
__author__ = "Imranur Rahman"

from .cli import main

__all__ = ["main"]
