"""Make the Sample Comparison of the Webapp again (spec #39, ticket #45).

The script runs the Comparison the way the Host runs a Job: it puts the Comparison in a
Job queue in a temporary folder, runs it with a Host worker and the Host's analysis
function (the Tool with deps.dev data), and writes the body of ``GET /api/jobs/{id}``.
It adds two fields: ``computed_on`` (the date of this run) and ``osv_index_date``.

Run it from the repository root, inside the venv (see webapp/README.md):

    python webapp/sample/make_sample.py --output-dir output

The output folder must hold an OSV index with a build date, so that MTTR is real. Build
it first with ``dependency-metrics --build-osv --output-dir output`` if it is missing.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_DIR / "host"))

from dependency_metrics.osv_builder import OSVBuilder  # noqa: E402
from dmhost import JobQueue, ToolAnalysis, Worker  # noqa: E402
from dmhost.app import job_view  # noqa: E402
from dmhost.models import Comparison  # noqa: E402

# The Sample Comparison of spec #39: the fixed window 2024-01-01 to 2025-12-31, the default
# weighting.
SAMPLE = Comparison(
    ecosystem="npm",
    packages=["express", "koa", "fastify"],
    start_date=date(2024, 1, 1),
    end_date=date(2025, 12, 31),
    mode="window_summary",
    weighting_type="disable",
)
SAMPLE_FILE = REPO_DIR / "webapp" / "site" / "static" / "sample" / "sample_comparison.json"


def make_sample(output_dir: Path) -> dict:
    """Run the Sample Comparison as a Host Job and return the body of the done Job."""
    index_date = OSVBuilder(output_dir).index_date()
    if index_date is None:
        raise SystemExit(
            f"No OSV index with a build date in {output_dir}. "
            f"Run: dependency-metrics --build-osv --output-dir {output_dir}"
        )
    with tempfile.TemporaryDirectory() as queue_dir:
        queue = JobQueue(Path(queue_dir) / "jobs.db")
        job_id = queue.add(SAMPLE)
        Worker(queue, ToolAnalysis(output_dir)).run_once()
        job = queue.get(job_id)
        assert job is not None
        body = job_view(queue, job)
    if body["state"] != "done":
        raise SystemExit(f"The Job did not finish: {body}")
    failed = [r["package"] for r in body["results"] if r["error"]]
    if failed:
        raise SystemExit(f"These packages failed: {failed}. See the log above.")
    body["job_id"] = "sample"
    body["computed_on"] = datetime.now(timezone.utc).date().isoformat()
    body["osv_index_date"] = index_date.isoformat()
    return body


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=REPO_DIR / "output",
        help="The Tool output folder: its cache and its OSV index (default: output/).",
    )
    parser.add_argument("--file", type=Path, default=SAMPLE_FILE, help="The file to write.")
    args = parser.parse_args()
    body = make_sample(args.output_dir)
    args.file.parent.mkdir(parents=True, exist_ok=True)
    args.file.write_text(json.dumps(body, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {args.file}")


if __name__ == "__main__":
    main()
