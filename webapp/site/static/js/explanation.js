// The Explanation module (GLOSSARY.md): the plain-English meaning of the MTTU and the MTTR
// of each package in a Comparison, and one shared list of the limitations of the data. It
// has no DOM code, so Node's test runner tests it (webapp/tests/explanation.test.mjs). The
// Webapp page code (webapp.js), the Extension webview, and the Extension hover call it.
//
// The results have the shape of the Host API results (contract/examples/).

import { formatDays, hasValue, EXPONENTIAL, NO_WEIGHTING } from "./comparison.js";

// The limitations of the data. They hold for each Comparison, so a page shows them once.
export const LIMITATIONS = [
  "Only vulnerabilities with a fix count. A vulnerability with no fix does not change MTTR.",
  "The results use the OSV data as of the date of the OSV index. A vulnerability that OSV added later does not count.",
  "A dependency with no version constraint always resolves to the newest version, so it counts as up to date.",
  "Only direct dependencies count. The dependencies of a dependency do not count.",
  "The results depend on the declared version constraints and on the release dates in the registry.",
];

// The headings of the Explanation on a page.
export const EXPLANATION_HEADING = "What the results mean";
export const LIMITATIONS_HEADING = "Limitations";

// The index of the OSV limitation in LIMITATIONS.
const OSV_LIMITATION = 1;

// Return the Limitations list. With the build date of the OSV index (the Sample Comparison
// and the Extension know it), the OSV limitation names the date.
export function limitationsFor(osvIndexDate) {
  const list = [...LIMITATIONS];
  if (typeof osvIndexDate === "string" && osvIndexDate !== "") {
    list[OSV_LIMITATION] =
      `The results use the OSV data as of ${osvIndexDate}, the build date of the OSV index. ` +
      "A vulnerability that OSV added later does not count.";
  }
  return list;
}

// The words of each metric: the full sentence and the short hover line, for a value of 0.0
// and for other values. The MTTR text never says that no dependency was vulnerable: the
// result has no count of vulnerable versions and no flag for a missing OSV index.
const METRIC_WORDS = {
  mttu: {
    zero: "In this time window, each direct dependency used the newest available version at all times.",
    what: "was behind the newest available version",
    hoverZero: "Each direct dependency used the newest version at all times",
    hover: "A direct dependency was behind the newest version",
  },
  mttr: {
    zero: "In this time window, no direct dependency stayed on a version with a known vulnerability after a fix was available.",
    what: "stayed on a version with a known vulnerability after a fix was available",
    hoverZero: "none stayed on a known vulnerable version after a fix was available",
    hover: "on a known vulnerable version after a fix",
  },
};

// Return true when a value shows as 0.0. The text follows the value that the reader sees,
// so a value of 0.03 days gets the 0.0 sentence.
function showsZero(days) {
  return formatDays(days) === formatDays(0);
}

// Return the words for an amount of days: { sentence, hover }. Without weighting, the
// analyzer sums the days of each dependency, then takes the mean over the direct
// dependencies; with weighting, the value is a weighted mean (AGENTS.md, "Metric semantics
// that are intended").
function amountWords(days, result) {
  const value = formatDays(days);
  if (result.weighting_type === NO_WEIGHTING) {
    return {
      sentence: `for ${value} days in total, on average over the direct dependencies`,
      hover: `${value} days in total (mean over the direct dependencies)`,
    };
  }
  const exponential = result.weighting_type === EXPONENTIAL;
  const weighting = exponential
    ? { long: `exponential weighting and a half-life of ${result.half_life} days`, short: `half-life ${result.half_life} days` }
    : { long: `${result.weighting_type} weighting`, short: `${result.weighting_type} weighting` };
  return {
    sentence: `for ${value} days on average, with ${weighting.long}`,
    hover: `${value} days on average (${weighting.short})`,
  };
}

// Return the sentence for one metric ("mttu" or "mttr").
function metricSentence(metric, days, result) {
  const words = METRIC_WORDS[metric];
  if (showsZero(days)) return words.zero;
  const value = formatDays(days);
  const amount = amountWords(days, result).sentence;
  return `${metric.toUpperCase()} is ${value} days: in this time window, a direct dependency of ${result.package} ${words.what} ${amount}.`;
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
// shared list (with the date of the OSV index when the Job has "osv_index_date"), or []
// when no package has a value.
export function explainComparison(job) {
  const packages = job.results.map(explainResult).filter((item) => item !== null);
  return { packages, limitations: packages.length > 0 ? limitationsFor(job.osv_index_date) : [] };
}

// Return the blocks that a page draws for the Explanation of a done Job, in order: the
// heading, one paragraph for each package with a value ({ title, text }), the Limitations
// heading, and the Limitations list ({ items }). The Webapp (webapp.js) and the Extension
// webview draw the same blocks, each with its own elements. Return [] when no package has
// a value.
export function explanationBlocks(job) {
  const { packages, limitations } = explainComparison(job);
  if (packages.length === 0) return [];
  return [
    { kind: "heading", text: EXPLANATION_HEADING },
    ...packages.map((item) => ({ kind: "paragraph", title: item.title, text: item.sentences.join(" ") })),
    { kind: "subheading", text: LIMITATIONS_HEADING },
    { kind: "list", items: limitations },
  ];
}

// Return the one short line of the Extension hover for a result, or null when the result
// has an error or no value.
export function hoverExplanation(result) {
  const entry = describedEntry(result);
  if (entry === null) return null;
  const { mttu, mttr } = METRIC_WORDS;
  const mttuText = showsZero(entry.mttu) ? mttu.hoverZero : `${mttu.hover} for ${amountWords(entry.mttu, result).hover}`;
  let mttrText = mttr.hoverZero;
  if (!showsZero(entry.mttr)) {
    // After the 0.0 text of MTTU, the MTTR part needs its own subject.
    const subject = showsZero(entry.mttu) ? "a direct dependency stayed " : "";
    mttrText = `${subject}${mttr.hover} for ${amountWords(entry.mttr, result).hover}`;
  }
  return `${mttuText}, and ${mttrText}.`;
}
