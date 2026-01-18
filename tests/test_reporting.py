from pathlib import Path

import pandas as pd

from dependency_metrics.reporting import export_osv_data, export_worksheets, save_results_json


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
