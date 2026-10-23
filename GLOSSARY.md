# Dependency Update Metrics

This project measures how fast a package updates its direct dependencies (MTTU) and how fast it removes vulnerable dependency versions (MTTR). People use it as a command-line tool, through a hosted web version, in VS Code, or in a GitHub Actions workflow.

## Parts of the project

**Tool**:
The `dependency-metrics` Python package and its command-line interface, as published on PyPI.
_Avoid_: CLI app, library, core

**Host**:
The server that runs the Tool for web users: the API, the job queue, and the workers.
_Avoid_: backend, server app

**Webapp**:
The static web page on GitHub Pages where users submit a Comparison and see its results.
_Avoid_: frontend, homepage, site

**Extension**:
The VS Code extension that runs the Tool on the user's machine and shows the MTTU and MTTR of each dependency in the manifest of the open workspace.
_Avoid_: plugin, add-on

**Action**:
The GitHub Action that runs the Tool in its container image in a CI workflow: it reports the MTTU and MTTR of chosen packages, or of the dependencies that a pull request adds.
_Avoid_: workflow, bot, CI plugin

## Requests

**Comparison**:
One user request from the Webapp or the Extension: 1 to 5 packages from one ecosystem, with one time window, one mode, and one weighting.
_Avoid_: query, batch, submission

**Job**:
The unit of work that the Host queues and runs for one Comparison.
_Avoid_: task, run, request

**Sample Comparison**:
A Comparison with precomputed results that the Webapp shows at once, with no Job, so that a first-time reader sees results without a wait.
_Avoid_: demo, example run

## Results

**Explanation**:
The plain-English meaning of the MTTU and MTTR of one package in a Comparison, with the limitations of the data.
_Avoid_: interpretation, insight, summary (summary is the result of the Window summary mode)

## Modes

**Window summary**:
The mode that gives one MTTU and one MTTR for each package over the whole time window.
_Avoid_: per-package mode, single-package mode (that name means `--package` as opposed to `--input-csv`)

**Per-release**:
The mode that gives one MTTU and one MTTR at each release of each package in the time window.
_Avoid_: release mode, timeline mode
