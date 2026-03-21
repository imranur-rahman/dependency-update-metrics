"""Tests for bulk CSV processing helpers."""

from pathlib import Path

import pandas as pd
import pytest

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


def test_load_input_csv_missing_required_column(tmp_path: Path) -> None:
    csv_path = tmp_path / "missing_col.csv"
    csv_path.write_text("ecosystem,end_date\npypi,2024-01-01\n", encoding="utf-8")
    with pytest.raises(ValueError, match="package_name"):
        _load_input_csv(csv_path)


def test_load_input_csv_bom_header(tmp_path: Path) -> None:
    csv_path = tmp_path / "bom.csv"
    csv_path.write_bytes(b"\xef\xbb\xbfecosystem,package_name,end_date\npypi,requests,2024-01-01\n")
    rows = _load_input_csv(csv_path)
    assert len(rows) == 1
    assert rows[0]["ecosystem"] == "pypi"


def test_load_input_csv_case_insensitive_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "upper.csv"
    csv_path.write_text(
        "ECOSYSTEM,PACKAGE_NAME,END_DATE\nnpm,express,2024-01-01\n", encoding="utf-8"
    )
    rows = _load_input_csv(csv_path)
    assert len(rows) == 1
    assert rows[0]["ecosystem"] == "npm"
    assert rows[0]["package_name"] == "express"
    assert rows[0]["end_date"] == "2024-01-01"


def test_load_input_csv_empty_file_raises(tmp_path: Path) -> None:
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text("ecosystem,package_name,end_date\n", encoding="utf-8")
    with pytest.raises(ValueError, match="no data rows"):
        _load_input_csv(csv_path)
