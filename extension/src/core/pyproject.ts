// The pyproject.toml parser of the core module. This module has no `vscode` import.

import type { ManifestDependency } from "./manifest.ts";
import { parseToml, tableAt, type TomlEntry, type TomlTable, type TomlValue } from "./toml.ts";

// The name at the start of a PEP 508 requirement, for example "requests" in
// "requests[socks]>=2; python_version >= '3.8'".
const REQUIREMENT_NAME = /^\s*([A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?)/;
// A requirement with a direct URL ("name @ https://..."), which does not come from PyPI.
const DIRECT_URL = /^[^;]*@/;
// The keys of a Poetry dependency that does not come from PyPI.
const POETRY_NOT_PYPI = ["git", "path", "url"];

// Parse pyproject.toml. It reads the PEP 621 dependencies and optional dependencies, the
// PEP 735 dependency groups, and the Poetry dependency tables. Text with a syntax error
// gives no dependencies.
export function parsePyproject(text: string): ManifestDependency[] {
  let root: TomlTable;
  try {
    root = parseToml(text);
  } catch {
    return [];
  }
  const deps: ManifestDependency[] = [];
  const project = tableAt(root, "project");
  addRequirements(deps, project?.entries.get("dependencies")?.value, "project.dependencies");
  for (const [extra, entry] of tableAt(project, "optional-dependencies")?.entries ?? []) {
    addRequirements(deps, entry.value, `project.optional-dependencies.${extra}`);
  }
  for (const [group, entry] of tableAt(root, "dependency-groups")?.entries ?? []) {
    addRequirements(deps, entry.value, `dependency-groups.${group}`);
  }
  const poetry = tableAt(root, "tool", "poetry");
  addPoetryTable(deps, tableAt(poetry, "dependencies"), "tool.poetry.dependencies");
  addPoetryTable(deps, tableAt(poetry, "dev-dependencies"), "tool.poetry.dev-dependencies");
  for (const [group] of tableAt(poetry, "group")?.entries ?? []) {
    const table = tableAt(poetry, "group", group, "dependencies");
    addPoetryTable(deps, table, `tool.poetry.group.${group}.dependencies`);
  }
  return deps.sort((a, b) => a.line - b.line || a.startCharacter - b.startCharacter);
}

// Add the requirements of a list of PEP 508 strings. The list can also hold tables, for
// example `{ include-group = "test" }`, which the parser skips.
function addRequirements(deps: ManifestDependency[], list: TomlValue | undefined, section: string): void {
  if (list?.kind !== "array") return;
  for (const item of list.items) {
    if (item.kind !== "string" || DIRECT_URL.test(item.value)) continue;
    const name = REQUIREMENT_NAME.exec(item.value)?.[1];
    if (!name) continue;
    // The columns cover the name in the string. A string with an escape in front of the name
    // gives the columns of the whole string.
    const inRaw = REQUIREMENT_NAME.exec(item.raw);
    const { line, startCharacter, endCharacter } = item.position;
    const nameStart = startCharacter + 1 + (inRaw ? inRaw[0].length - name.length : 0);
    deps.push({
      ecosystem: "pypi",
      name,
      section,
      line,
      startCharacter: inRaw?.[1] === name ? nameStart : startCharacter,
      endCharacter: inRaw?.[1] === name ? nameStart + name.length : endCharacter,
    });
  }
}

// Add the dependencies of a Poetry table: `name = "^1.0"`, `name = { version = ... }`, or a
// list of constraints. The key "python" is the Python version, not a dependency.
function addPoetryTable(deps: ManifestDependency[], table: TomlTable | undefined, section: string): void {
  for (const [name, entry] of table?.entries ?? []) {
    if (name.toLowerCase() === "python" || !isPoetryPypiDependency(entry)) continue;
    deps.push({ ecosystem: "pypi", name, section, ...entry.key });
  }
}

function isPoetryPypiDependency(entry: TomlEntry): boolean {
  const { value } = entry;
  if (value.kind !== "table") return true;
  return !POETRY_NOT_PYPI.some((key) => value.entries.has(key));
}
