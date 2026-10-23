// The rules for the OSV index (#30). MTTR needs the OSV index, a download of about 260 MB,
// so the Extension asks once before the Tool builds it. The Tool is the single source of
// the index: `--osv-index-date` prints its build date, and `--build-osv` builds it.
// This module has no `vscode` import.

// What the Extension knows about the index in the Tool output dir.
export type OsvIndexState = { kind: "missing" } | { kind: "built"; date: string };

// The answer of the developer to the prompt. The Extension keeps "declined", so that the
// prompt does not come back on each run.
export type OsvConsent = "accepted" | "declined";

// The prompt before the first build, and its two buttons.
export const OSV_PROMPT = {
  message:
    "MTTX needs the OSV index to compute MTTR. The download is about 260 MB. " +
    "Without the index, the hints show MTTU only.",
  build: "Download the OSV index",
  decline: "Not now",
} as const;

// Return the arguments of `dependency-metrics` that print the index date.
export function osvIndexDateArgs(outputDir: string): string[] {
  return ["--osv-index-date", "--output-dir", outputDir];
}

// Return the arguments of `dependency-metrics` that build the index and do no analysis.
export function buildOsvArgs(outputDir: string): string[] {
  return ["--build-osv", "--output-dir", outputDir];
}

// Return true when the Extension must ask before it builds the index: the index is
// missing, and the developer did not decline before. An unknown index state never asks.
export function shouldAskForOsvIndex(
  index: OsvIndexState | undefined,
  consent: OsvConsent | undefined,
): boolean {
  return index?.kind === "missing" && consent !== "declined";
}

// Return the consent for the button that the developer selected. A closed prompt
// (undefined) declines, so that it does not come back on each run.
export function consentFromAnswer(answer: string | undefined): OsvConsent {
  return answer === OSV_PROMPT.build ? "accepted" : "declined";
}

// Return true when the Tool gives a known index state that differs from the last known
// state, for example after a build by hand with `--build-osv`. Then the values of the old
// state are stale. An unknown state on either side changes nothing, because the result
// store skips the cache while the state is unknown.
export function osvIndexChanged(before: OsvIndexState | undefined, after: OsvIndexState | undefined): boolean {
  if (before === undefined || after === undefined) return false;
  return JSON.stringify(before) !== JSON.stringify(after);
}

// Return a short text about the index, for the rebuild command and the status.
export function osvIndexText(index: OsvIndexState | undefined): string {
  if (index === undefined) return "OSV index date unknown";
  if (index.kind === "missing") return "no OSV index";
  return `OSV index built on ${index.date}`;
}

// Return the text and the tooltip of the status bar item of the index. The item shows the
// build date where the developer can see it without a rebuild, and a click rebuilds the index.
export function osvIndexStatus(index: OsvIndexState | undefined): { text: string; tooltip: string } {
  const date = index === undefined ? "unknown" : index.kind === "missing" ? "none" : index.date;
  return { text: `$(database) OSV ${date}`, tooltip: `MTTX: ${osvIndexText(index)}. Click to rebuild it.` };
}

// Return the last line of a stderr chunk of `--build-osv`, without the log level, for the
// progress notification. A tqdm bar rewrites its line with "\r", so "\r" also ends a line.
export function progressLine(chunk: string): string | undefined {
  const lines = chunk.split(/[\r\n]+/).map((line) => line.trim()).filter((line) => line !== "");
  return lines.pop()?.replace(/^(?:INFO|WARNING|ERROR): /, "");
}

// Return the index state from the stdout of `dependency-metrics --osv-index-date`, or
// undefined when stdout has no such JSON line (for example, from an older Tool).
export function parseOsvIndexDate(stdout: string): OsvIndexState | undefined {
  for (const line of stdout.trim().split(/\r?\n/).reverse()) {
    if (!line.startsWith("{")) continue;
    try {
      const value: unknown = JSON.parse(line);
      if (typeof value !== "object" || value === null || !("osv_index_date" in value)) continue;
      const date = value.osv_index_date;
      if (date === null) return { kind: "missing" };
      if (typeof date === "string") return { kind: "built", date };
    } catch {
      continue;
    }
  }
  return undefined;
}
