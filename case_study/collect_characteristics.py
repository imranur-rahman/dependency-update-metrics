"""Collect package characteristics from deps.dev and GitHub for the case study.

For every package in mttu_sample.csv / mttr_sample.csv this script records:
  - deps.dev: version count, first/last release dates, age, default version,
    direct/transitive dependency counts, dependent counts, licenses,
    linked source repository, OpenSSF Scorecard (via project entry),
    stars/forks/open issues.
  - GitHub (via `gh` CLI): repo creation date, last push, archived flag,
    contributor count, funding links.

Raw API responses are dumped to case_study/raw/ for provenance; the merged
summary is written to case_study/characteristics.csv.
"""

import csv
import json
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote

import requests

HERE = Path(__file__).parent
RAW_DIR = HERE / "raw"
RAW_DIR.mkdir(exist_ok=True)

DEPSDEV = "https://api.deps.dev/v3alpha"
SYSTEM = {"npm": "npm", "pypi": "pypi", "cargo": "cargo"}
END_DATE = "2026-07-06"

session = requests.Session()
session.headers["User-Agent"] = "dependency-update-metrics-case-study"


def get_json(url, ok404=True):
    for attempt in range(4):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 404 and ok404:
                return None
            if r.status_code == 200:
                return r.json()
            time.sleep(2**attempt)
        except requests.RequestException:
            time.sleep(2**attempt)
    return None


def gh_api(path, extra=None):
    cmd = ["gh", "api", path] + (extra or [])
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if p.returncode != 0:
        return None
    try:
        return json.loads(p.stdout)
    except json.JSONDecodeError:
        return None


def gh_contributor_count(owner, repo):
    """Count contributors via the Link header's last-page number."""
    p = subprocess.run(
        ["gh", "api", "-i", f"repos/{owner}/{repo}/contributors?per_page=1&anon=1"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if p.returncode != 0:
        return None
    headers, _, body = p.stdout.partition("\r\n\r\n")
    if not body:
        headers, _, body = p.stdout.partition("\n\n")
    for line in headers.splitlines():
        if line.lower().startswith("link:") and 'rel="last"' in line:
            import re

            m = re.search(r"[?&]page=(\d+)>; rel=\"last\"", line)
            if m:
                return int(m.group(1))
    # No Link header -> zero or one page of results
    try:
        return len(json.loads(body))
    except json.JSONDecodeError:
        return None


def gh_funding(owner, repo):
    q = """query($owner:String!,$name:String!){
      repository(owner:$owner,name:$name){ fundingLinks{ platform url } isArchived }
    }"""
    p = subprocess.run(
        ["gh", "api", "graphql", "-f", f"query={q}", "-f", f"owner={owner}", "-f", f"name={repo}"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if p.returncode != 0:
        return None
    try:
        return json.loads(p.stdout)["data"]["repository"]["fundingLinks"]
    except (json.JSONDecodeError, KeyError, TypeError):
        return None


def repo_from_links(version_info, project_ids):
    """Prefer deps.dev's related project; fall back to SOURCE_REPO link."""
    for pid in project_ids:
        if pid.startswith("github.com/"):
            parts = pid.split("/")
            if len(parts) >= 3:
                return parts[1], parts[2]
    for link in (version_info or {}).get("links", []):
        url = link.get("url", "")
        if link.get("label") == "SOURCE_REPO" and "github.com" in url:
            tail = url.split("github.com/", 1)[1]
            parts = tail.replace(".git", "").rstrip("/").split("/")
            if len(parts) >= 2:
                return parts[0], parts[1]
    return None


def collect(row):
    eco, name = row["ecosystem"], row["package_name"]
    sys_ = SYSTEM[eco]
    enc = quote(name, safe="")
    out = {"ecosystem": eco, "package_name": name}
    raw = {}

    pkg = get_json(f"{DEPSDEV}/systems/{sys_}/packages/{enc}")
    raw["package"] = pkg
    if pkg and pkg.get("versions"):
        versions = pkg["versions"]
        out["num_versions"] = len(versions)
        dates = sorted(v.get("publishedAt") for v in versions if v.get("publishedAt"))
        out["first_release"] = dates[0][:10] if dates else None
        out["last_release"] = dates[-1][:10] if dates else None
        default = next(
            (v for v in versions if v.get("isDefault")),
            max(versions, key=lambda v: v.get("publishedAt") or ""),
        )
        ver = default["versionKey"]["version"]
        out["default_version"] = ver

        vinfo = get_json(f"{DEPSDEV}/systems/{sys_}/packages/{enc}/versions/{quote(ver, safe='')}")
        raw["version"] = vinfo
        if vinfo:
            out["licenses"] = ";".join(vinfo.get("licenses", []))
        project_ids = [
            rp["projectKey"]["id"]
            for rp in (vinfo or {}).get("relatedProjects", [])
            if "projectKey" in rp
        ]

        deps = get_json(
            f"{DEPSDEV}/systems/{sys_}/packages/{enc}/versions/{quote(ver, safe='')}:dependencies"
        )
        raw["dependencies"] = deps
        if deps and deps.get("nodes"):
            rels = [n.get("relation") for n in deps["nodes"]]
            out["direct_deps_latest"] = rels.count("DIRECT")
            out["transitive_deps_latest"] = rels.count("INDIRECT")

        dpts = get_json(
            f"{DEPSDEV}/systems/{sys_}/packages/{enc}/versions/{quote(ver, safe='')}:dependents"
        )
        raw["dependents"] = dpts
        if dpts:
            out["dependents_total"] = dpts.get("dependentCount")
            out["dependents_direct"] = dpts.get("directDependentCount")
            out["dependents_indirect"] = dpts.get("indirectDependentCount")

        gh = repo_from_links(vinfo, project_ids)
        if gh:
            owner, repo_name = gh
            out["github_repo"] = f"{owner}/{repo_name}"
            proj = get_json(f"{DEPSDEV}/projects/{quote(f'github.com/{owner}/{repo_name}', safe='')}")
            raw["project"] = proj
            if proj:
                out["stars"] = proj.get("starsCount")
                out["forks"] = proj.get("forksCount")
                out["open_issues"] = proj.get("openIssuesCount")
                sc = proj.get("scorecard") or {}
                out["scorecard_overall"] = (sc.get("overallScore")
                                            if isinstance(sc, dict) else None)
                for check in sc.get("checks", []) if isinstance(sc, dict) else []:
                    if check.get("name") in ("Maintained", "Code-Review", "Security-Policy"):
                        out[f"scorecard_{check['name'].lower().replace('-', '_')}"] = check.get(
                            "score"
                        )
            repo_info = gh_api(f"repos/{owner}/{repo_name}")
            raw["github_repo_api"] = repo_info
            if repo_info:
                out["repo_created_at"] = (repo_info.get("created_at") or "")[:10]
                out["repo_pushed_at"] = (repo_info.get("pushed_at") or "")[:10]
                out["repo_archived"] = repo_info.get("archived")
                out["repo_license"] = (repo_info.get("license") or {}).get("spdx_id")
                out["contributors"] = gh_contributor_count(owner, repo_name)
                funding = gh_funding(owner, repo_name)
                raw["funding"] = funding
                out["funding_links"] = (
                    ";".join(f"{f['platform']}:{f['url']}" for f in funding) if funding else ""
                )
                out["has_funding"] = bool(funding)

    safe_name = name.replace("/", "__")
    (RAW_DIR / f"{eco}_{safe_name}.json").write_text(json.dumps(raw, indent=1))
    print(f"done {eco}/{name}", flush=True)
    return out


def main():
    rows = {}
    for f in ("mttu_sample.csv", "mttr_sample.csv"):
        with open(HERE / f) as fh:
            for r in csv.DictReader(fh):
                key = (r["ecosystem"], r["package_name"])
                entry = rows.setdefault(
                    key, {"ecosystem": r["ecosystem"], "package_name": r["package_name"]}
                )
                if "mttu" in r:
                    entry["mttu_tier"] = r["tier"]
                    entry["mttu"] = r["mttu"]
                    entry["num_dependencies_sample"] = r["num_dependencies"]
                else:
                    entry["mttr_tier"] = r["tier"]
                    entry["mttr_all_severities"] = r["mttr_all_severities"]
                    entry["num_dependencies_sample"] = r["num_dependencies"]

    with ThreadPoolExecutor(max_workers=6) as ex:
        results = list(ex.map(collect, rows.values()))

    merged = []
    for base, res in zip(rows.values(), results):
        merged.append({**base, **res})

    fieldnames = [
        "ecosystem", "package_name", "mttu_tier", "mttu", "mttr_tier",
        "mttr_all_severities", "num_dependencies_sample", "num_versions",
        "first_release", "last_release", "default_version", "licenses",
        "direct_deps_latest", "transitive_deps_latest", "dependents_total",
        "dependents_direct", "dependents_indirect", "github_repo", "stars",
        "forks", "open_issues", "scorecard_overall", "scorecard_maintained",
        "scorecard_code_review", "scorecard_security_policy", "repo_created_at",
        "repo_pushed_at", "repo_archived", "repo_license", "contributors",
        "funding_links", "has_funding",
    ]
    with open(HERE / "characteristics.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(merged)
    print(f"wrote {len(merged)} rows to characteristics.csv")


if __name__ == "__main__":
    main()
