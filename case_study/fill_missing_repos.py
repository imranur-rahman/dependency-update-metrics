"""Second pass: for packages whose GitHub repo was not found via deps.dev,
look up the repository URL in the native registry (npm / PyPI / crates.io),
then re-collect the GitHub-side characteristics."""

import csv
import json
import re
from pathlib import Path
from urllib.parse import quote

import requests

from collect_characteristics import (
    HERE,
    RAW_DIR,
    DEPSDEV,
    get_json,
    gh_api,
    gh_contributor_count,
    gh_funding,
)

GH_RE = re.compile(r"github\.com[:/]+([^/]+)/([^/#?]+)")


def registry_repo(eco, name):
    try:
        if eco == "npm":
            d = requests.get(f"https://registry.npmjs.org/{quote(name, safe='@/')}", timeout=30).json()

            def npm_fields(obj):
                repo = obj.get("repository")
                url = repo.get("url", "") if isinstance(repo, dict) else str(repo or "")
                return [url, obj.get("homepage") or "", (obj.get("bugs") or {}).get("url", "")]

            candidates = npm_fields(d)
            # repository often only appears inside version objects
            for vobj in reversed(list((d.get("versions") or {}).values())):
                candidates += npm_fields(vobj)
        elif eco == "pypi":
            d = requests.get(f"https://pypi.org/pypi/{name}/json", timeout=30).json()
            info = d.get("info", {})
            urls = info.get("project_urls") or {}
            candidates = list(urls.values()) + [info.get("home_page", ""), info.get("description", "")[:2000]]
            # older releases sometimes carry metadata the latest upload lost
            releases = sorted((d.get("releases") or {}).keys(), reverse=True)
            for rel in releases[:5]:
                try:
                    ri = requests.get(f"https://pypi.org/pypi/{name}/{rel}/json", timeout=30).json().get("info", {})
                    candidates += list((ri.get("project_urls") or {}).values()) + [ri.get("home_page", "")]
                except Exception:
                    pass
        else:  # cargo
            d = requests.get(
                f"https://crates.io/api/v1/crates/{name}",
                headers={"User-Agent": "dependency-update-metrics-case-study"},
                timeout=30,
            ).json()
            c = d.get("crate", {})
            candidates = [c.get("repository", ""), c.get("homepage", ""), c.get("documentation", "")]
    except Exception:
        return None
    for cand in candidates:
        if not cand:
            continue
        m = GH_RE.search(cand)
        if m:
            return m.group(1), m.group(2).removesuffix(".git")
    return None


def main():
    rows = list(csv.DictReader(open(HERE / "characteristics.csv")))
    fieldnames = rows[0].keys()
    filled = 0
    for row in rows:
        if row.get("github_repo"):
            continue
        eco, name = row["ecosystem"], row["package_name"]
        gh = registry_repo(eco, name)
        if not gh:
            print(f"no repo found: {eco}/{name}", flush=True)
            continue
        owner, repo_name = gh
        repo_info = gh_api(f"repos/{owner}/{repo_name}")
        if not repo_info:
            print(f"repo dead: {eco}/{name} -> {owner}/{repo_name}", flush=True)
            row["github_repo"] = f"{owner}/{repo_name} (unreachable)"
            continue
        # GitHub may redirect renamed repos; use the canonical name
        owner, repo_name = repo_info["full_name"].split("/")
        row["github_repo"] = f"{owner}/{repo_name}"
        row["repo_created_at"] = (repo_info.get("created_at") or "")[:10]
        row["repo_pushed_at"] = (repo_info.get("pushed_at") or "")[:10]
        row["repo_archived"] = repo_info.get("archived")
        row["repo_license"] = (repo_info.get("license") or {}).get("spdx_id")
        row["contributors"] = gh_contributor_count(owner, repo_name)
        funding = gh_funding(owner, repo_name)
        row["funding_links"] = ";".join(f"{f['platform']}:{f['url']}" for f in funding) if funding else ""
        row["has_funding"] = bool(funding)
        proj = get_json(f"{DEPSDEV}/projects/{quote(f'github.com/{owner}/{repo_name}'.lower(), safe='')}")
        if proj:
            row["stars"] = proj.get("starsCount")
            row["forks"] = proj.get("forksCount")
            row["open_issues"] = proj.get("openIssuesCount")
            sc = proj.get("scorecard") or {}
            row["scorecard_overall"] = sc.get("overallScore") if isinstance(sc, dict) else None
        else:
            row["stars"] = repo_info.get("stargazers_count")
            row["forks"] = repo_info.get("forks_count")
            row["open_issues"] = repo_info.get("open_issues_count")
        safe = name.replace("/", "__")
        raw_file = RAW_DIR / f"{eco}_{safe}.json"
        raw = json.loads(raw_file.read_text()) if raw_file.exists() else {}
        raw["github_repo_api_fallback"] = repo_info
        raw_file.write_text(json.dumps(raw, indent=1))
        filled += 1
        print(f"filled {eco}/{name} -> {owner}/{repo_name}", flush=True)

    with open(HERE / "characteristics.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"filled {filled} packages")


if __name__ == "__main__":
    main()
