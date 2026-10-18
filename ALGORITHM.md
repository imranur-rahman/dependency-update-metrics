# How dependency-metrics computes MTTU and MTTR

This document describes the algorithm that `DependencyAnalyzer.analyze()` (`dependency_metrics/analyzer.py`) runs for one package. MTTU (Mean Time To Update) and MTTR (Mean Time To Remediate) are defined in Rahman et al., *No Vulnerability Data, No Problem* ([arXiv:2403.17382](https://arxiv.org/abs/2403.17382)).

## Notation

- `t` is a time, `v` is a version, and `d` is a dependency.
- `t(v)` is the publish time of version `v`. A version "by `t`" has `t(v) <= t`.
- `Vuln[d]` lists the affected ranges `[v_intro, v_fix)` of `d`. `v_intro` is the version that introduced a vulnerability, and `v_fix` is the first version that fixed it.
- The weight function `w` is `linear`, `exponential` (with a half-life), `inverse`, or none (`--weighting-type disable`).

## Algorithm

```
Input:  package P, window [t_s, t_e], weight function w (or none),
        vulnerability data Vuln
Output: MTTU and MTTR of P

 1  v_P ← latest release of P by t_e
 2  D ← runtime dependencies of v_P
 3  for all d ∈ D do
 4      t_0 ← first time in [t_s, t_e] at which a release of P declares d
 5      t_0 < t_1 < … < t_n ← t_0, t_e, and the release times of P and d between them
 6      for i ← 0 to n−1 do                              ▷ interval [t_i, t_{i+1})
 7          req   ← requirement on d in highest release of P by t_i
 8          v_res ← highest version of d by t_i that satisfies req
 9          v_new ← highest stable version of d by t_i
10          updated_i    ← (v_res = v_new)
11          remediated_i ← no (v_intro, v_fix) ∈ Vuln[d] has
                           v_intro ≤ v_res < v_fix and t(v_fix) ≤ t_i
12          Δ_i ← t_{i+1} − t_i                         ▷ Δ_i: interval length
13      Outdated_d     ← {i : ¬updated_i}
14      Unremediated_d ← {i : ¬remediated_i}
15      TTU_d ← Agg(Outdated_d)                         ▷ time to update d
16      TTR_d ← Agg(Unremediated_d)                     ▷ time to remediate d
17  MTTU ← (1/|D|) Σ_{d∈D} TTU_d                        ▷ mean over D
18  MTTR ← (1/|D|) Σ_{d∈D} TTR_d
19  return MTTU, MTTR

20  function Agg(I)                                     ▷ I: set of intervals
21      if w is none then
22          return Σ_{i∈I} Δ_i
23      return Σ_{i∈I} w(t_e − t_i)·Δ_i / Σ_{i∈I} w(t_e − t_i)
```

## Key decisions

- **No look-ahead (lines 7–9).** Every interval uses only data published by its start `t_i`. For PyPI, the patched `pip` in `vendor/pip` resolves constraints as of a date. For npm and Cargo, the resolver filters releases by publish time and applies each ecosystem's version rules.
- **Fix-gated remediation (line 11).** An interval counts against MTTR only if `v_fix` was already published at `t_i`. MTTR therefore measures how fast a project adopts an available fix, not how long it is exposed to a vulnerability.
- **Recency weight (line 23).** Each interval is weighted by its age `t_e − t_i`, so a study can favor recent behavior. Without a weight, `Agg` adds up the days.

## Behavior that differs by ecosystem or mode

- **Line 1.** npm and PyPI pick the release with the latest publish date by `t_e`. Cargo picks the highest stable semver by `t_e`.
- **Line 2.** npm uses `dependencies`. Cargo uses normal dependencies that are not optional. PyPI drops a requirement only if it names extras on the dependency itself (`foo[bar]`).
- **Lines 8–9.** Pre-releases are skipped unless the constraint names one. `v_new` falls back to a pre-release only when no stable version exists.
- **Line 11.** Versions are compared with PEP 440 rules. If `v_res` does not exist, the interval is remediated. If `v_res` does not parse, it is not remediated. A vulnerability with an unknown fix date never counts. The OSV index keeps only ranges with a `fixed` event, so a vulnerability with no fix does not affect MTTR.
- **Lines 17–18.** In single-package mode, a dependency with no intervals adds 0 to the mean, and a dependency that raises an error is left out. Bulk mode skips a dependency with no intervals.
- **Weights.** With `--weighting-type disable`, TTU and TTR are sums of days. With a weight, they are weighted means. These are different quantities, so compare results only under the same setting.
- **`--per-release`.** `analyze_at_release_points()` runs the same steps for each release `R` of `P`, with `R` in place of `v_P` and `t(R)` in place of `t_e`. It builds one timeline per dependency and cuts it at every release, so all releases share the interval construction.
