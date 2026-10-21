# Webapp

The Webapp is the static page on GitHub Pages where users submit a Comparison and see its results.
See `GLOSSARY.md` and spec #1.

- `webapp/site/` is the published folder. `.github/workflows/webapp.yml` uploads only this folder
  to GitHub Pages, at https://imranur-rahman.github.io/dependency-update-metrics/.
- `webapp/tests/` holds the Webapp tests. They use Node's built-in test runner, with no npm packages
  and no build step: `node --test webapp/tests/*.test.mjs`.

To see the page locally, serve the published folder: `python -m http.server -d webapp/site 8000`.
