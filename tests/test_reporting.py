from pathlib import Path

import pandas as pd

from dependency_metrics.reporting import (
    export_osv_data,
    export_worksheets,
    safe_filename_stem,
    save_results_json,
)


def test_reporting_exports(tmp_path: Path):
    output_dir = tmp_path / "out"
    dep_df = pd.DataFrame({"dependency": ["demo"], "value": [1]})
    results = {
        "ttu": 1.0,
        "ttr": 2.0,
        "num_dependencies": 1,
        "dependency_data": {"demo": dep_df},
        "osv_data": pd.DataFrame({"package": ["demo"], "vul_id": ["CVE-1"]}),
    }

    results_file = save_results_json(results, output_dir, "demo")
    osv_file = export_osv_data(results, output_dir, "demo")
    excel_file = export_worksheets(results, output_dir, "demo")

    assert results_file.exists()
    assert osv_file is not None and osv_file.exists()
    assert excel_file is not None and excel_file.exists()


def test_scoped_npm_package_name_does_not_create_subdirectory(tmp_path: Path):
    """Scoped npm packages like "@medv/finder" contain a "/" — naively
    interpolating them into a path makes pandas/Path treat it as a (missing)
    subdirectory ("output/@medv/finder_results.json"), raising
    "Cannot save file into a non-existent directory". Exporters must sanitize
    the package name into a single filename component, written directly inside
    output_dir (no nested directory created)."""
    output_dir = tmp_path / "out"
    package = "@medv/finder"
    results = {"ttu": 1.0, "ttr": 2.0, "num_dependencies": 0}

    results_file = save_results_json(results, output_dir, package)

    assert results_file.exists()
    assert results_file.parent == output_dir
    assert not (output_dir / "@medv").exists()
    assert "/" not in results_file.name


def test_safe_filename_stem_replaces_path_separators():
    assert safe_filename_stem("@medv/finder") == "@medv__finder"
    assert "/" not in safe_filename_stem("@scope/pkg")
    assert "\\" not in safe_filename_stem("scope\\pkg")
    assert safe_filename_stem("plain-package") == "plain-package"
