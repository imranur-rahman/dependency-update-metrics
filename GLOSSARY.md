# Dependency Update Metrics

This project measures how fast a package updates its direct dependencies (MTTU) and how fast it removes vulnerable dependency versions (MTTR). People use it as a command-line tool, through a hosted web version, or in VS Code.

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

## Requests

**Comparison**:
One user request from the Webapp or the Extension: 1 to 5 packages from one ecosystem, with one time window, one mode, and one weighting.
_Avoid_: query, batch, submission

**Job**:
The unit of work that the Host queues and runs for one Comparison.
_Avoid_: task, run, request

## Modes

**Window summary**:
The mode that gives one MTTU and one MTTR for each package over the whole time window.
_Avoid_: per-package mode, single-package mode (that name means `--package` as opposed to `--input-csv`)

**Per-release**:
The mode that gives one MTTU and one MTTR at each release of each package in the time window.
_Avoid_: release mode, timeline mode
