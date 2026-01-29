"""Tests for bulk CSV processing helpers."""

from pathlib import Path

import pandas as pd

from dependency_metrics.cli import _load_input_csv


def test_load_input_csv_parses_headers(tmp_path: Path) -> None:
    csv_content = (
        "ecosystem,package_name,end_date,start_date,extra\n"
        "npm,express,2024-01-01,2020-01-01,one\n"
        "npm,express,2024-01-01,2020-01-01,two\n"
        "pypi,requests,2024-06-01,,three\n"
    )
    csv_path = tmp_path / "input.csv"
    csv_path.write_text(csv_content, encoding="utf-8")

    rows = _load_input_csv(csv_path)
    assert len(rows) == 3
    assert rows[0]["ecosystem"] == "npm"
    assert rows[0]["package_name"] == "express"
    assert rows[0]["end_date"] == "2024-01-01"
    assert rows[0]["start_date"] == "2020-01-01"

    df = pd.DataFrame(rows)
    assert "extra" in df.columns
