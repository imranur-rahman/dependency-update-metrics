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
