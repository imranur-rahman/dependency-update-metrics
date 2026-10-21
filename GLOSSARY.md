# Dependency Update Metrics

This project measures how fast a package updates its direct dependencies (MTTU) and how fast it removes vulnerable dependency versions (MTTR). People use it as a command-line tool or through a hosted web version.

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

## Web requests

**Comparison**:
One user request: 1 to 5 packages from one ecosystem, with one time window, one mode, and one weighting.
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
