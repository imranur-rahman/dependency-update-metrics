# Host

The Host is the server that runs the Tool for Webapp users: the API, the job queue, and the workers.
See `GLOSSARY.md` and spec #1.

This folder is a placeholder. The Host code comes in later tickets:

- `host/requirements.txt` holds the Python dependencies of the Host (FastAPI, uvicorn, ...).
- `host/tests/` holds the Host tests. CI runs them with `pytest host/tests`.

CI (`.github/workflows/host.yml`) runs `black --check`, `flake8 --max-line-length=100`, and `pytest`
on this folder when a file in `host/` changes.
