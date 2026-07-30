"""Merge per-release metric output with tier labels and characteristics,
classify pins/floats, and produce the case-study summary tables.

Outputs (all in case_study/):
  case_database.csv    — one row per case: tiers, provided metrics, recomputed
                         final-window metrics, trajectory features, pin/float
                         counts, and all collected characteristics.
  tier_summary_mttu.csv / tier_summary_mttr.csv — per-tier medians (IQR).
  spearman.csv         — tier vs characteristic rank correlations (BH-corrected).
"""

import re
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

HERE = Path(__file__).parent
OUT = HERE.parent / "output"
END_DATE = pd.Timestamp("2026-07-06")

RESULTS = OUT / "case_study_input_depsdev_per_release_results.csv"
DETAILS = OUT / "case_study_input_depsdev_per_release_dependency_details.csv"

TIER_ORDER = {"zero": 0, "low": 1, "med": 2, "high": 3}


# ---------------------------------------------------------------- pins/floats
def classify_constraint(eco: str, constraint) -> str | None:
    """Return 'pin' | 'float' | None (unclassifiable/missing)."""
    if constraint is None or (isinstance(constraint, float) and np.isnan(constraint)):
        return None
    c = str(constraint).strip()
    if not c or c in ("*", "latest"):
        return "float"
    if eco == "cargo":
        # bare "1.2.3" is caret semantics -> float; only "=1.2.3" pins
        return "pin" if c.startswith("=") and not c.startswith(("=>", "=<")) else "float"
    if eco == "npm":
        if re.fullmatch(r"v?\d+(\.\d+){2}(-[\w.+-]+)?", c):
            return "pin"
        return "float"
    if eco == "pypi":
        if re.fullmatch(r"==\s*[\w.!+*]+", c) and "*" not in c:
            return "pin"
        return "float"
    return None


def pin_float_counts(details: pd.DataFrame, res: pd.DataFrame) -> pd.DataFrame:
    """Per package: constraint mix of dependencies declared by the LATEST
    released version's own manifest (not every dependency ever tracked
    across the package's whole history — a dependency dropped by a later
    release keeps its last-known interval open in the continuous timeline,
    which would otherwise inflate the count for packages that shrank their
    dependency set). ``package_release_date`` in the results CSV is
    day-truncated, so same-day releases (e.g. two releases on one day)
    can tie; a stable sort preserves the CSV's original row order for
    ties, which reflects deps.dev's own chronological version ordering.
    """
    # the version governing the final analyzed release, per package
    ok = res[res["status"] == "ok"].copy()
    ok["package_release_date"] = pd.to_datetime(ok["package_release_date"])
    final_version = (
        ok.sort_values("package_release_date", kind="stable")
        .groupby(["ecosystem", "package_name"])
        .tail(1)
        .set_index(["ecosystem", "package_name"])["package_version"]
    )

    rows = []
    for (eco, pkg), grp in details.groupby(["ecosystem", "package"]):
        fv = final_version.get((eco, pkg))
        cur = grp[grp["package_version"] == fv] if fv is not None else grp.iloc[0:0]
        last = cur.sort_values("interval_start").groupby("dependency").tail(1)
        cls = last["dependency_constraint"].map(lambda c: classify_constraint(eco, c))
        rows.append(
            {
                "ecosystem": eco,
                "package_name": pkg,
                "num_direct_deps_final": len(last),
                "num_pins": int((cls == "pin").sum()),
                "num_floats": int((cls == "float").sum()),
            }
        )
    df = pd.DataFrame(rows)
    df["pin_fraction"] = df["num_pins"] / df[["num_pins", "num_floats"]].sum(axis=1).replace(0, np.nan)
    return df


# ---------------------------------------------------------- trajectory features
def trajectory_features(res: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (eco, pkg), grp in res.groupby(["ecosystem", "package_name"]):
        ok = grp[grp["status"] == "ok"].copy()
        if ok.empty:
            rows.append({"ecosystem": eco, "package_name": pkg, "n_releases_analyzed": 0})
            continue
        ok["package_release_date"] = pd.to_datetime(ok["package_release_date"])
        ok = ok.sort_values("package_release_date")
        final = ok.iloc[-1]
        mttr_col = "mttr_all_severities" if "mttr_all_severities" in ok.columns else "mttr"
        feat = {
            "ecosystem": eco,
            "package_name": pkg,
            "n_releases_analyzed": len(ok),
            "first_analyzed_release": str(ok.iloc[0]["package_release_date"].date()),
            "last_analyzed_release": str(final["package_release_date"].date()),
            "final_mttu": final["mttu"],
            "final_mttr": final[mttr_col],
            "max_mttu": ok["mttu"].max(),
            "median_mttu": ok["mttu"].median(),
            "max_mttr": ok[mttr_col].max(),
            "releases_with_nonzero_mttu": int((ok["mttu"] > 0).sum()),
            "final_num_dependencies": final["num_dependencies"],
        }
        for c in ("mttr_critical", "mttr_high", "mttr_medium", "mttr_low"):
            if c in ok.columns:
                feat[f"final_{c}"] = final[c]
        # monotonic trend of MTTU across releases
        if len(ok) >= 3 and ok["mttu"].nunique() > 1:
            rho, p = stats.spearmanr(np.arange(len(ok)), ok["mttu"])
            feat["mttu_trend_rho"] = rho
        rows.append(feat)
    return pd.DataFrame(rows)


def iqr_fmt(series):
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return "--"
    return f"{s.median():.1f} ({s.quantile(0.25):.1f}--{s.quantile(0.75):.1f})"


NUMERIC_CHARS = [
    "num_versions", "age_days", "days_since_last_release", "direct_deps_latest",
    "transitive_deps_latest", "dependents_total", "stars", "forks",
    "open_issues", "contributors", "scorecard_overall", "num_pins",
    "num_floats", "pin_fraction", "n_releases_analyzed",
]


def tier_table(db: pd.DataFrame, tier_col: str, out_name: str):
    sub = db[db[tier_col].notna()].copy()
    rows = []
    for tier in ["zero", "low", "med", "high"]:
        grp = sub[sub[tier_col] == tier]
        if grp.empty:
            continue
        row = {"tier": tier, "n": len(grp)}
        for c in NUMERIC_CHARS:
            if c in grp.columns:
                row[c] = iqr_fmt(grp[c])
        row["has_funding_pct"] = f"{100 * (grp['has_funding'].astype(str) == 'True').mean():.0f}"
        row["archived_pct"] = f"{100 * (grp['repo_archived'].astype(str) == 'True').mean():.0f}"
        row["no_repo_pct"] = f"{100 * grp['github_repo'].isna().mean():.0f}"
        rows.append(row)
    pd.DataFrame(rows).to_csv(HERE / out_name, index=False)


def spearman_table(db: pd.DataFrame):
    out = []
    for tier_col, label in [("mttu_tier", "MTTU"), ("mttr_tier", "MTTR")]:
        sub = db[db[tier_col].notna()].copy()
        ranks = sub[tier_col].map(TIER_ORDER)
        for c in NUMERIC_CHARS:
            if c not in sub.columns:
                continue
            vals = pd.to_numeric(sub[c], errors="coerce")
            mask = vals.notna()
            if mask.sum() < 10:
                continue
            rho, p = stats.spearmanr(ranks[mask], vals[mask])
            out.append({"metric": label, "characteristic": c, "n": int(mask.sum()),
                        "rho": round(rho, 3), "p": p})
    df = pd.DataFrame(out)
    for label in df["metric"].unique():
        m = df["metric"] == label
        pvals = df.loc[m, "p"].values
        order = np.argsort(pvals)
        bh = np.empty_like(pvals)
        n = len(pvals)
        prev = 1.0
        for rank_i, idx in enumerate(order[::-1]):
            i = n - rank_i  # 1-based rank from largest p
            prev = min(prev, pvals[idx] * n / i)
            bh[idx] = prev
        df.loc[m, "p_bh"] = bh
    df["significant"] = df["p_bh"] < 0.05
    df.sort_values(["metric", "p_bh"]).to_csv(HERE / "spearman.csv", index=False)


def main():
    res = pd.read_csv(RESULTS)
    chars = pd.read_csv(HERE / "characteristics.csv")
    traj = trajectory_features(res)

    db = chars.merge(traj, on=["ecosystem", "package_name"], how="left")

    if DETAILS.exists():
        details = pd.read_csv(DETAILS, low_memory=False)
        print("details columns:", list(details.columns))
        pf = pin_float_counts(details, res)
        db = db.merge(pf, on=["ecosystem", "package_name"], how="left")

    db["age_days"] = (END_DATE - pd.to_datetime(db["first_release"])).dt.days
    db["days_since_last_release"] = (END_DATE - pd.to_datetime(db["last_release"])).dt.days

    # reproduction check: recomputed full-window metric vs provided tier value
    db["mttu_provided"] = pd.to_numeric(db["mttu"], errors="coerce")
    db["mttr_provided"] = pd.to_numeric(db["mttr_all_severities"], errors="coerce")
    bulk_path = OUT / "case_study_input_depsdev_bulk_results.csv"
    if bulk_path.exists():
        bulk = pd.read_csv(bulk_path)
        bulk_mttr_col = (
            "mttr_all_severities" if "mttr_all_severities" in bulk.columns else "mttr"
        )
        bulk = bulk.rename(
            columns={"mttu": "mttu_recomputed", bulk_mttr_col: "mttr_recomputed"}
        )[["ecosystem", "package_name", "mttu_recomputed", "mttr_recomputed"]]
        db = db.merge(bulk, on=["ecosystem", "package_name"], how="left")
        db["mttu_abs_diff"] = (db["mttu_recomputed"] - db["mttu_provided"]).abs()
        db["mttr_abs_diff"] = (db["mttr_recomputed"] - db["mttr_provided"]).abs()
    else:
        db["mttu_abs_diff"] = (db["final_mttu"] - db["mttu_provided"]).abs()
        db["mttr_abs_diff"] = (db["final_mttr"] - db["mttr_provided"]).abs()

    db.to_csv(HERE / "case_database.csv", index=False)
    tier_table(db, "mttu_tier", "tier_summary_mttu.csv")
    tier_table(db, "mttr_tier", "tier_summary_mttr.csv")
    spearman_table(db)

    print(f"{len(db)} cases")
    err = res[res["status"] != "ok"]
    print(f"error rows in per-release results: {len(err)}")
    if not err.empty:
        print(err[["ecosystem", "package_name", "error"]].drop_duplicates().to_string())
    print("\nreproduction check (MTTU):")
    m = db[db["mttu_tier"].notna()]
    print("  median |diff| =", m["mttu_abs_diff"].median(), " max =", m["mttu_abs_diff"].max())
    print("reproduction check (MTTR):")
    r = db[db["mttr_tier"].notna()]
    print("  median |diff| =", r["mttr_abs_diff"].median(), " max =", r["mttr_abs_diff"].max())


if __name__ == "__main__":
    main()
