// The Cargo.toml parser of the core module. This module has no `vscode` import.

import type { ManifestDependency } from "./manifest.ts";
import { parseToml, tableAt, type TomlTable, type TomlValue } from "./toml.ts";

// The dependency tables, with the old names with an underscore that Cargo still reads.
const SECTIONS: Record<string, string> = {
  dependencies: "dependencies",
  "dev-dependencies": "dev-dependencies",
  dev_dependencies: "dev-dependencies",
  "build-dependencies": "build-dependencies",
  build_dependencies: "build-dependencies",
};

// Parse Cargo.toml. It reads the dependency tables at the top level and below
// `[target.<cfg>]`, in the inline-table and the table forms. A `package = "..."` key gives
// the name of the crate. Text with a syntax error gives no dependencies.
export function parseCargoToml(text: string): ManifestDependency[] {
  let root: TomlTable;
  try {
    root = parseToml(text);
  } catch {
    return [];
  }
  const deps: ManifestDependency[] = [];
  addSections(deps, root, "");
  for (const [target] of tableAt(root, "target")?.entries ?? []) {
    addSections(deps, tableAt(root, "target", target), `target.${target}.`);
  }
  return deps.sort((a, b) => a.line - b.line || a.startCharacter - b.startCharacter);
}

function addSections(deps: ManifestDependency[], parent: TomlTable | undefined, prefix: string): void {
  for (const [key, section] of Object.entries(SECTIONS)) {
    for (const [name, entry] of tableAt(parent, key)?.entries ?? []) {
      const crate = crateName(name, entry.value);
      if (crate) deps.push({ ecosystem: "cargo", name: crate, section: prefix + section, ...entry.key });
    }
  }
}

// Return the crates.io name of a dependency, or undefined for a dependency that does not come
// from crates.io: a git dependency, a path dependency without a version, or a dependency
// from another registry.
function crateName(key: string, value: TomlValue): string | undefined {
  if (value.kind === "string") return key;
  if (value.kind !== "table") return undefined;
  const { entries } = value;
  if (entries.has("git") || entries.has("registry")) return undefined;
  if (entries.has("path") && !entries.has("version")) return undefined;
  const renamed = entries.get("package")?.value;
  return renamed?.kind === "string" ? renamed.value : key;
}
