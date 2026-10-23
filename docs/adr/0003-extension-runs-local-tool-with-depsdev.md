# The Extension runs the local Tool with deps.dev

The Extension runs the Tool on the user's machine, in a venv that the Extension makes in its global storage folder, and it does not call the Host. The Host needs a Turnstile token for each Job, so a program without a browser cannot use it, and it allows only 5 packages for each Job, while one manifest can hold hundreds of dependencies. The Host also runs on a research credit that ends on 2 September 2027 (ADR 0002), so editor traffic must not depend on it. The Extension uses deps.dev as its default data source, because the deps.dev resolver does not need the patched `pip` fork in `vendor/pip`. A setting switches to the native registries.

## Considered Options

- **Call the Host API.** We rejected it for the reasons above. It also needs a new kind of authentication on the Host.
- **Use the interpreter that the user selected in the Python extension.** We rejected it as the default, because it installs the Tool into the user's project environment. The setting `mttx.toolPath` still lets a user point at an installed `dependency-metrics`.

## Consequences

- The Tool gets a `--json` flag that prints one result on stdout, with the same fields as the Host API result in `contract/examples/`. The Extension pins a range of Tool versions, so this JSON shape can change only with a new minor version of the Tool.
- The user needs Python 3.10 or later. MTTR needs the OSV index, a large download (`AGENTS.md` gives its size) that the Extension builds only after the user agrees.
- The Extension and the Host both use deps.dev, so for the same window and weighting they give the same values. The paper validates the native resolvers, so a user who wants the validated path switches the setting to the native registries.
