"""
Typed errors of the data sources, so that callers can tell the outcomes apart
without reading error text.
"""

from __future__ import annotations


class DataSourceError(RuntimeError):
    """A data source (for example deps.dev) did not give the data, after its retries.

    The analysis cannot go on without the data, so the result is an error and never a
    silent 0.
    """


class PackageNotFoundError(DataSourceError, LookupError):
    """The data source does not know the package (deps.dev answered 404)."""

    def __init__(self, system: str, package: str) -> None:
        super().__init__(f"Package {package} not found in {system}")
        self.system = system
        self.package = package


class NoReleaseInWindowError(ValueError):
    """The package has no release that the analysis window can use."""
