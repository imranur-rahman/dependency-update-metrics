// The label of the inlay hint of one dependency. This module has no `vscode` import.

import type { ManifestDependency } from "./manifest.ts";

// Return the label for a dependency, from its hint text. The hints of a line sit together at
// the end of the line, so a dependency that shares its line with another dependency (for
// example `test = ["pytest", "coverage"]` in pyproject.toml) gets its name in front.
export function hintLabel(dep: ManifestDependency, deps: ManifestDependency[], text: string): string {
  const shared = deps.some((other) => other !== dep && other.line === dep.line);
  return shared ? `${dep.name}: ${text}` : text;
}

// Return the dependency for a hover at a position: the dependency whose name holds the
// position, else the first dependency of the line, else undefined.
export function dependencyAt(
  deps: ManifestDependency[],
  line: number,
  character: number,
): ManifestDependency | undefined {
  const onLine = deps.filter((dep) => dep.line === line);
  const under = onLine.find((dep) => dep.startCharacter <= character && character <= dep.endCharacter);
  return under ?? onLine[0];
}
