// The label of the inlay hint of one dependency. This module has no `vscode` import.

import type { ManifestDependency } from "./manifest.ts";

// Return the label for a dependency, from its hint text. The hints of a line sit together at
// the end of the line, so a dependency that shares its line with another dependency (for
// example `test = ["pytest", "coverage"]` in pyproject.toml) gets its name in front.
export function hintLabel(dep: ManifestDependency, deps: ManifestDependency[], text: string): string {
  const shared = deps.some((other) => other !== dep && other.line === dep.line);
  return shared ? `${dep.name}: ${text}` : text;
}
