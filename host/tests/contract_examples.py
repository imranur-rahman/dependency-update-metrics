"""Read the shared API contract examples in ``contract/examples/``.

The Webapp tests read the same files, so a change to the contract fails both sides.
"""

import json
from pathlib import Path

CONTRACT_DIR = Path(__file__).resolve().parents[2] / "contract" / "examples"
PLACEHOLDER_JOB_ID = "example-job-id"


def load_example(name: str) -> dict:
    """Return one contract example (``contract/examples/<name>.json``)."""
    return json.loads((CONTRACT_DIR / f"{name}.json").read_text(encoding="utf-8"))


def with_job_id(example: dict, job_id: str) -> dict:
    """Return the example with each use of the placeholder job ID replaced by ``job_id``."""
    return json.loads(json.dumps(example).replace(PLACEHOLDER_JOB_ID, job_id))
