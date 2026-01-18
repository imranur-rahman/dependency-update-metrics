from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from dependency_metrics.analyzer import DependencyAnalyzer


def test_check_remediation_false_when_fix_available_before_interval(tmp_path: Path):
    analyzer = DependencyAnalyzer(
        ecosystem="pypi",
        package="demo",
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 10),
        output_dir=tmp_path,
    )

    osv_df = pd.DataFrame(
        [{
            "package": "dep",
            "vul_introduced": "1.0.0",
            "vul_fixed": "2.0.0",
        }]
    )
    dep_metadata = {
        "releases": {
            "2.0.0": [{"upload_time": "2020-01-02T00:00:00Z"}]
        }
    }

    remediated = analyzer._check_remediation(
        dependency="dep",
        dep_version="1.5.0",
        interval_start=datetime(2020, 1, 5, tzinfo=timezone.utc),
        osv_df=osv_df,
        dep_metadata=dep_metadata,
    )

    assert remediated is False


def test_check_remediation_true_when_fix_after_interval(tmp_path: Path):
    analyzer = DependencyAnalyzer(
        ecosystem="pypi",
        package="demo",
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 10),
        output_dir=tmp_path,
    )

    osv_df = pd.DataFrame(
        [{
            "package": "dep",
            "vul_introduced": "1.0.0",
            "vul_fixed": "2.0.0",
        }]
    )
    dep_metadata = {
        "releases": {
            "2.0.0": [{"upload_time": "2020-01-09T00:00:00Z"}]
        }
    }

    remediated = analyzer._check_remediation(
        dependency="dep",
        dep_version="1.5.0",
        interval_start=datetime(2020, 1, 5, tzinfo=timezone.utc),
        osv_df=osv_df,
        dep_metadata=dep_metadata,
    )

    assert remediated is True
