// The state of each dependency, and the runs that fill it. This module has no `vscode`
// import. #32 replaces the fixed limit with the setting, and adds the one-day cache and the
// automatic-run rule.

import type { DependencyState } from "./display.ts";
import type { Ecosystem } from "./manifest.ts";
import type { ToolRequest, ToolRunner } from "./tool.ts";

export interface PackageKey {
  ecosystem: Ecosystem;
  name: string;
}

const MAX_PARALLEL_RUNS = 2;

export class ResultStore {
  private readonly states = new Map<string, DependencyState>();
  private readonly waiting: Array<() => Promise<void>> = [];
  private readonly listeners = new Set<() => void>();
  private readonly runner: ToolRunner;
  private readonly makeRequest: (key: PackageKey) => ToolRequest;
  private readonly abort = new AbortController();
  private readonly now: () => Date;
  private active = 0;

  // `now` gives the clock for the date of a value (a test hook).
  constructor(
    runner: ToolRunner,
    makeRequest: (key: PackageKey) => ToolRequest,
    now: () => Date = () => new Date(),
  ) {
    this.runner = runner;
    this.makeRequest = makeRequest;
    this.now = now;
  }

  // Return the state of a package. A package that the store does not know gets a Tool run
  // and the state "computing". The store calls the listeners when a run finishes.
  get(key: PackageKey): DependencyState {
    const id = `${key.ecosystem}:${key.name}`;
    let state = this.states.get(id);
    if (!state) {
      state = { kind: "computing" };
      this.states.set(id, state);
      this.waiting.push(() => this.runOne(id, key));
      this.next();
    }
    return state;
  }

  onDidUpdate(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  // Stop the runs that wait or run. Their states stay "computing".
  dispose(): void {
    this.waiting.length = 0;
    this.listeners.clear();
    this.abort.abort();
  }

  private async runOne(id: string, key: PackageKey): Promise<void> {
    const outcome = await this.runner.run(this.makeRequest(key), this.abort.signal);
    if (this.abort.signal.aborted) return;
    this.states.set(
      id,
      outcome.kind === "result" ? { ...outcome, computedOn: localDate(this.now()) } : outcome,
    );
    for (const listener of this.listeners) listener();
  }

  private next(): void {
    while (this.active < MAX_PARALLEL_RUNS && this.waiting.length > 0) {
      const job = this.waiting.shift()!;
      this.active++;
      job().finally(() => {
        this.active--;
        this.next();
      });
    }
  }
}

// Return the date (YYYY-MM-DD) of a time in the local time zone.
function localDate(time: Date): string {
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${time.getFullYear()}-${pad(time.getMonth() + 1)}-${pad(time.getDate())}`;
}
