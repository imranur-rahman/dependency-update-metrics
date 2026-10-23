# Webapp

The Webapp is the static page on GitHub Pages where users submit a Comparison and see its results.
See `GLOSSARY.md` and spec #1.

- `webapp/site/` is the published folder. `.github/workflows/webapp.yml` uploads only this folder
  to GitHub Pages, at https://imranur-rahman.github.io/dependency-update-metrics/.
- `index.html` keeps the local Host URL (`http://localhost:8080`) and the Turnstile test site key.
  Before the upload, the workflow runs `webapp/configure.mjs`, which puts the repository variables
  `DM_HOST_URL` and `DM_TURNSTILE_SITE_KEY` into the published copy. The deploy fails if one is
  missing. `host/RUNBOOK.md` says how to set them.
- `webapp/tests/` holds the Webapp tests. They use Node's built-in test runner, with no npm packages
  and no build step: `node --test webapp/tests/*.test.mjs`.

To see the page locally, serve the published folder: `python -m http.server -d webapp/site 8000`.

## Sample Comparison

The button "Show a sample Comparison" in the Try It section shows precomputed results, with no
Job and no request to the Host. The page reads them from
`webapp/site/static/sample/sample_comparison.json`. `SAMPLE` in `webapp/sample/make_sample.py`
defines the Comparison. The file is the body of a done Job (the shape of
`contract/examples/job_done_window_summary.json`) plus `computed_on` and `osv_index_date`. The page
shows `computed_on` as "Precomputed on <date>".

`webapp/sample/make_sample.py` makes the file again. It runs the Comparison as a Host Job: a Job
queue in a temporary folder, a Host worker, and the Host's analysis function (the Tool with
deps.dev data). It stops if the output folder has no OSV index with a build date, because
without the index every MTTR is 0. Run it from the repository root, inside the venv:

```bash
source metrics_venv/bin/activate
dependency-metrics --build-osv --output-dir output   # only if output/ has no current OSV index
python webapp/sample/make_sample.py --output-dir output
node --test webapp/tests/*.test.mjs
```

Change the dates in `SAMPLE` in the script and in `webapp/tests/sample.test.mjs` together.
