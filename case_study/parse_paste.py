"""Reconstruct the two case-study sample CSVs from the newline-stripped paste."""

import csv
import re
from pathlib import Path

HERE = Path(__file__).parent
raw_lines = (HERE / "raw_paste.txt").read_text().splitlines()

# Each record starts with "<ecosystem>,<tier>," — split on that boundary.
row_start = re.compile(r"(?=(?:cargo|npm|pypi),(?:zero|low|med|high),)")


def parse_blob(blob: str, header_prefix: str):
    header_end = blob.index(header_prefix) + len(header_prefix)
    header = blob[:header_end].split(",")
    body = blob[header_end:]
    rows = [r for r in row_start.split(body) if r]
    parsed = []
    for r in rows:
        fields = r.split(",")
        # package names can contain commas? no — npm scopes use '/', so plain split works,
        # but guard on field count anyway
        assert len(fields) == len(header), (len(fields), len(header), r)
        parsed.append(dict(zip(header, fields)))
    return header, parsed


mttu_header, mttu_rows = parse_blob(raw_lines[0], "mttu")
mttr_header, mttr_rows = parse_blob(raw_lines[1], "mttr_low")

for name, header, rows in [
    ("mttu_sample.csv", mttu_header, mttu_rows),
    ("mttr_sample.csv", mttr_header, mttr_rows),
]:
    with open(HERE / name, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        w.writerows(rows)
    print(name, len(rows), "rows")

# Combined input CSV for dependency-metrics bulk mode (dedupe on ecosystem+package)
seen = set()
with open(HERE / "case_study_input.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["ecosystem", "package_name", "start_date", "end_date"])
    for row in mttu_rows + mttr_rows:
        key = (row["ecosystem"], row["package_name"])
        if key in seen:
            continue
        seen.add(key)
        w.writerow([row["ecosystem"], row["package_name"], row["start_date"], row["end_date"]])
print("case_study_input.csv", len(seen), "unique packages")

from collections import Counter

print(Counter((r["ecosystem"], r["tier"]) for r in mttu_rows))
print(Counter((r["ecosystem"], r["tier"]) for r in mttr_rows))
