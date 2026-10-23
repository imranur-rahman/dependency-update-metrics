// The rule that reads a JSON object from the stdout of the Tool. This module has no `vscode`
// import. The Tool prints its JSON as the last line of stdout, but a stray print of a
// dependency can come after it, so the rule reads the lines from the end.

// Return the last line of stdout that holds a JSON object that `accept` takes, parsed, or
// undefined when no line does.
export function lastJsonObject(
  stdout: string,
  accept: (value: Record<string, unknown>) => boolean = () => true,
): Record<string, unknown> | undefined {
  for (const line of stdout.trim().split(/\r?\n/).reverse()) {
    if (!line.startsWith("{")) continue;
    try {
      const value: unknown = JSON.parse(line);
      if (typeof value === "object" && value !== null && accept(value as Record<string, unknown>)) {
        return value as Record<string, unknown>;
      }
    } catch {
      continue;
    }
  }
  return undefined;
}
