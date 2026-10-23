// The Explanation module (GLOSSARY.md): the plain-English meaning of the MTTU and the MTTR
// of each package in a Comparison, and one shared list of the limitations of the data. It
// has no DOM code, so Node's test runner tests it (webapp/tests/explanation.test.mjs). The
// Webapp page code (webapp.js), the Extension webview, and the Extension hover call it.
//
// The results have the shape of the Host API results (contract/examples/).

import { formatDays } from "./comparison.js";

const NO_WEIGHTING = "disable";
const EXPONENTIAL = "exponential";

// The limitations of the data. They hold for each Comparison, so a page shows them once.
export const LIMITATIONS = [
  "Only vulnerabilities with a fix count. A vulnerability with no fix does not change MTTR.",
  "The results use the OSV data as of the date of the OSV index. A vulnerability that OSV added later does not count.",
  "A dependency with no version constraint always resolves to the newest version, so it counts as up to date.",
  "Only direct dependencies count. The dependencies of a dependency do not count.",
  "The results depend on the declared version constraints and on the release dates in the registry.",
];

const MTTU_ZERO = "In this time window, each direct dependency used the newest available version at all times.";
// The result cannot show that no dependency was vulnerable (it has no count of vulnerable
// versions and no flag for a missing OSV index), so the text never says that.
const MTTR_ZERO =
  "In this time window, no direct dependency stayed on a version with a known vulnerability after a fix was available.";

// What each metric counts, in the words of the full sentence and of the short hover line.
const BEHIND = "behind the newest available version";
const VULNERABLE = "stayed on a version with a known vulnerability after a fix was available";

// Return true when a summary or a release has a value: the status is "ok" and it has
// dependencies. The rule matches hasNoDependencies in comparison.js.
function hasValue(entry) {
  return (
    entry?.status === "ok" &&
    entry.num_dependencies > 0 &&
    typeof entry.mttu === "number" &&
    typeof entry.mttr === "number"
  );
}

// Return the words that name the weighting of a weighted result.
function weightingWords(result) {
  if (result.weighting_type === EXPONENTIAL) {
    return `exponential weighting and a half-life of ${result.half_life} days`;
  }
  return `${result.weighting_type} weighting`;
}

// Return the sentence for one metric. Without weighting the value is a sum of days; with
// weighting it is a weighted mean (AGENTS.md, "Metric semantics that are intended").
function metricSentence(metric, days, result) {
  if (days === 0) return metric === "mttu" ? MTTU_ZERO : MTTR_ZERO;
  const name = metric.toUpperCase();
  const value = formatDays(days);
  const what = metric === "mttu" ? `was ${BEHIND}` : VULNERABLE;
  const amount =
    result.weighting_type === NO_WEIGHTING
      ? `for a sum of ${value} days, as a mean over the direct dependencies`
      : `for ${value} days on average, with ${weightingWords(result)}`;
  return `${name} is ${value} days: in this time window, a direct dependency of ${result.package} ${what} ${amount}.`;
}

// Return the releases of a Per-release result that have a value, in release order.
function releasesWithValues(result) {
  return (result.releases ?? []).filter(hasValue);
}

// Return the entry that the Explanation describes: the summary of a Window summary result,
// or the latest release with a value of a Per-release result. Return null when the result
// has an error or no value.
function describedEntry(result) {
  if (result.error) return null;
  if (result.mode === "per_release") return releasesWithValues(result).at(-1) ?? null;
  return hasValue(result.summary) ? result.summary : null;
}

// Return the Explanation of one package result: { title, sentences }, with one sentence for
// each metric, and for Per-release one more sentence for each metric on the change over the
// window. Return null for a result with an error or without dependencies; the result table
// keeps its error text.
export function explainResult(result) {
  const entry = describedEntry(result);
  if (entry === null) return null;
  const sentences = [metricSentence("mttu", entry.mttu, result), metricSentence("mttr", entry.mttr, result)];
  if (result.mode !== "per_release") return { title: result.package, sentences };
  const releases = releasesWithValues(result);
  if (releases.length > 1) {
    for (const metric of ["mttu", "mttr"]) {
      const from = formatDays(releases[0][metric]);
      const to = formatDays(entry[metric]);
      sentences.push(`${metric.toUpperCase()} went from ${from} to ${to} days over ${releases.length} releases.`);
    }
  }
  const title = `${result.package} ${entry.package_version}, the latest release with a value (${entry.package_release_date})`;
  return { title, sentences };
}

// Return the Explanation of a done Job: { packages, limitations }. packages holds one
// Explanation for each result with a value, in the order of the results. limitations is the
// shared list, or [] when no package has a value.
export function explainComparison(job) {
  const packages = job.results.map(explainResult).filter((item) => item !== null);
  return { packages, limitations: packages.length > 0 ? [...LIMITATIONS] : [] };
}

// Return the amount of days for the hover line.
function hoverAmount(days, result) {
  const value = formatDays(days);
  if (result.weighting_type === NO_WEIGHTING) return `a sum of ${value} days`;
  const weighting = result.weighting_type === EXPONENTIAL ? `half-life ${result.half_life} days` : `${result.weighting_type} weighting`;
  return `${value} days on average (${weighting})`;
}

// Return the one short line of the Extension hover for a result, or null when the result
// has an error or no value.
export function hoverExplanation(result) {
  const entry = describedEntry(result);
  if (entry === null) return null;
  const mttu =
    entry.mttu === 0
      ? "Each direct dependency used the newest version at all times"
      : `A direct dependency was behind the newest version for ${hoverAmount(entry.mttu, result)}`;
  let mttr;
  if (entry.mttr === 0) {
    mttr = "none stayed on a known vulnerable version after a fix was available";
  } else {
    const subject = entry.mttu === 0 ? "a direct dependency stayed " : "";
    mttr = `${subject}on a known vulnerable version after a fix for ${hoverAmount(entry.mttr, result)}`;
  }
  return `${mttu}, and ${mttr}.`;
}
