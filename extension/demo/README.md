# MTTX demo project

This folder is a small project for the MTTX extension. It has one manifest of each kind that MTTX reads, with the dependency sections of each ecosystem:

| File | Sections |
|---|---|
| `package.json` | `dependencies`, `devDependencies`, `peerDependencies`, `optionalDependencies` |
| `pyproject.toml` | `[project] dependencies` (with a marker and an extra), `[project.optional-dependencies]`, `[dependency-groups]` (two names on one line) |
| `Cargo.toml` | `[dependencies]` (with a renamed crate), `[dev-dependencies]`, `[build-dependencies]`, `[target.'cfg(windows)'.dependencies]` |

`lodash`, `typescript`, and `fsevents` have no dependencies, so MTTX shows "could not compute" for them.

The project does not build or run. It exists only to show the hints, the hovers, and a Comparison.

## Use it

1. Install MTTX from the [Visual Studio Marketplace](https://marketplace.visualstudio.com/items?itemName=imranur-rahman.mttx) or [Open VSX](https://open-vsx.org/extension/imranur-rahman/mttx).
2. Open this folder in VS Code: `code extension/demo`.
3. Open a manifest. The first run installs the Tool and asks to build the OSV index. Accept, so that MTTR has data.
4. Hover a dependency to see the details. Select "Compare with alternatives" in the hover, or run "MTTX: Compare packages", to open a Comparison.
