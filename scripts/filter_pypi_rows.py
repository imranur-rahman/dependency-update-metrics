#!/usr/bin/env python3
"""Remove rows with ecosystem == PYPI (case-insensitive) from a CSV file."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Filter out PYPI rows from a CSV file by ecosystem column."
    )
    parser.add_argument("input_csv", help="Path to input CSV")
    parser.add_argument(
        "output_csv",
        help="Path to output CSV (filtered)",
    )
    args = parser.parse_args()

    input_path = Path(args.input_csv)
    output_path = Path(args.output_csv)

    df = pd.read_csv(input_path, sep=None, engine="python")
    if "ecosystem" not in df.columns:
        raise ValueError("Input CSV missing required column: ecosystem")

    mask = df["ecosystem"].astype(str).str.strip().str.lower() != "pypi"
    filtered = df[mask]
    filtered.to_csv(output_path, index=False)


if __name__ == "__main__":
    main()
