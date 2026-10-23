// The state of each dependency, and the runs that fill it: a queue that runs at most
// `parallelRuns` Tool processes at a time, and the one-day cache. This module has no
// `vscode` import.

import type { DependencyState } from "./display.ts";
import type { Ecosystem } from "./manifest.ts";
import { FRESH_MS, type ResultCache } from "./resultCache.ts";
import type { ToolOutcome, ToolRequest, ToolRunner } from "./tool.ts";

export interface PackageKey {
  ecosystem: Ecosystem;
  name: string;
}

export const DEFAULT_PARALLEL_RUNS = 2;

export interface ResultStoreOptions {
  runner: ToolRunner;
  // The request of one package, with the current settings.
  makeRequest: (key: PackageKey) => ToolRequest;
  // The most Tool processes that run at a time (the setting `mttx.parallelRuns`).
  parallelRuns?: number;
  // The cache that keeps the results across sessions. Without it, the store keeps the
  // results only in memory.
  cache?: ResultCache;
  // The clock: for the age of a value, and for the date of a value that the hover shows.
  now?: () => Date;
  // false: `get` starts no Tool run, and only `analyze` does (the setting `mttx.automaticRuns`).
  automaticRuns?: boolean;
}

// A queued Tool run: start it, or stop it before it starts.
interface QueuedRun {
  start: () => Promise<void>;
  cancel: () => void;
}

// The state of one package, the time when it stops being fresh, and the run that fills it.
interface Entry {
  state: DependencyState;
  freshUntil: number;
  run?: Promise<ToolOutcome>;
}

export class ResultStore {
  private readonly entries = new Map<string, Entry>();
  private readonly waiting: QueuedRun[] = [];
  private readonly listeners = new Set<() => void>();
  private readonly runner: ToolRunner;
  private readonly makeRequest: (key: PackageKey) => ToolRequest;
  private readonly cache: ResultCache | undefined;
  private readonly now: () => Date;
  private readonly abort = new AbortController();
  private readonly parallelRuns: number;
  private readonly automaticRuns: boolean;
  private active = 0;

  constructor(options: ResultStoreOptions) {
    this.runner = options.runner;
    this.makeRequest = options.makeRequest;
    this.cache = options.cache;
    this.now = options.now ?? (() => new Date());
    this.parallelRuns = Math.max(1, Math.floor(options.parallelRuns ?? DEFAULT_PARALLEL_RUNS));
    this.automaticRuns = options.automaticRuns ?? true;
  }

  // Return the state of a package. A fresh value (from this session or from the cache) shows
  // at once. Any other package gets a Tool run and the state "computing", or no state when
  // the automatic runs are off. The store calls the listeners when a run finishes.
  get(key: PackageKey): DependencyState | undefined {
    const known = this.known(key);
    if (known) return known.state;
    return this.automaticRuns ? this.start(key).state : undefined;
  }

  // Run the Tool for each package that has no fresh value or a failed run, and give the
  // outcomes in the order of the keys. A package with a run in progress joins that run. The
  // queue limit applies, so a caller can give many packages at once.
  analyze(keys: PackageKey[]): Promise<ToolOutcome[]> {
    return Promise.all(
      keys.map((key) => {
        const known = this.known(key);
        if (known?.run) return known.run;
        if (known && known.state.kind !== "computing" && known.state.kind !== "failed") {
          return Promise.resolve(known.state);
        }
        return this.start(key).run!;
      }),
    );
  }

  onDidUpdate(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  // Stop the runs that wait or run. Their states stay "computing", and the waiting
  // `analyze` calls get a failed outcome.
  dispose(): void {
    for (const queued of this.waiting.splice(0)) queued.cancel();
    this.listeners.clear();
    this.abort.abort();
  }

  // Return the entry of a package when it runs or is fresh. A fresh result in the cache
  // becomes the entry.
  private known(key: PackageKey): Entry | undefined {
    const id = idOf(key);
    const entry = this.entries.get(id);
    if (entry && (entry.run || this.now().getTime() < entry.freshUntil)) return entry;
    const cached = this.cache?.get(this.makeRequest(key));
    if (!cached) return undefined;
    const fresh: Entry = {
      state: {
        kind: "result",
        result: cached.result,
        computedOn: localDate(new Date(cached.savedAt)),
      },
      freshUntil: cached.savedAt + FRESH_MS,
    };
    this.entries.set(id, fresh);
    return fresh;
  }

  // Queue a Tool run for the package and return its entry with the state "computing".
  private start(key: PackageKey): Entry {
    const id = idOf(key);
    const request = this.makeRequest(key);
    const entry: Entry = { state: { kind: "computing" }, freshUntil: 0 };
    entry.run = new Promise<ToolOutcome>((resolve) => {
      this.waiting.push({
        start: async () => {
          const outcome = await this.runner.run(request, this.abort.signal);
          resolve(outcome);
          if (this.abort.signal.aborted) return;
          this.finish(id, request, outcome);
        },
        cancel: () => resolve({ kind: "failed", message: "the run stopped" }),
      });
    });
    this.entries.set(id, entry);
    this.next();
    return entry;
  }

  // Keep the outcome for one day. Only a result without a package error goes into the cache,
  // so that the next session runs a failed package again.
  // A result carries the local date of the run, so that the hover can show it.
  private finish(id: string, request: ToolRequest, outcome: ToolOutcome): void {
    const now = this.now();
    const state = outcome.kind === "result" ? { ...outcome, computedOn: localDate(now) } : outcome;
    this.entries.set(id, { state, freshUntil: now.getTime() + FRESH_MS });
    if (outcome.kind === "result" && !outcome.result.error) {
      // A failed write loses only the cache entry, and the next session runs the Tool again.
      this.cache?.set(request, outcome.result).catch(() => undefined);
    }
    for (const listener of this.listeners) listener();
  }

  private next(): void {
    while (this.active < this.parallelRuns && this.waiting.length > 0) {
      const queued = this.waiting.shift()!;
      this.active++;
      queued.start().finally(() => {
        this.active--;
        this.next();
      });
    }
  }
}

function idOf(key: PackageKey): string {
  return `${key.ecosystem}:${key.name}`;
}

// Return the date (YYYY-MM-DD) of a time in the local time zone.
function localDate(time: Date): string {
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${time.getFullYear()}-${pad(time.getMonth() + 1)}-${pad(time.getDate())}`;
}
