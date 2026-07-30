"""Two figures for the paper: real per-release trajectories for a small,
hand-picked set of packages that illustrate the MTTU and MTTR mechanisms
described in the results text.

Each figure has two panels:
  (a) packages whose score mostly WORSENED over time -- by the
      measurement date (2026-07-06) they landed in the high tier.
  (b) packages whose score mostly IMPROVED over time, or never grew much
      to begin with -- by the measurement date they landed in the
      zero/low tier.
Solid line = value at each of the package's own releases. Dashed line =
what happens between the last release and the measurement date, during
which a dependency with a flexible version range can still catch up on
its own even if the package itself never releases again.

Color assignment is fixed across both figures: blue/violet mark the
"worsened" packages, orange/aqua mark the "improved" packages.
"""

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

END = pd.Timestamp("2026-07-06")

# fixed categorical slots (validated for colorblind-safe separation)
BLUE = "#2a78d6"    # worsened, primary
VIOLET = "#4a3aa7"  # worsened, secondary
ORANGE = "#eb6834"  # improved, primary
AQUA = "#1baf7a"    # improved, secondary

TEXT = "#0b0b0b"
TEXT2 = "#52514e"
GRID = "#e8e7e2"

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "axes.linewidth": 0.8,
    "pdf.fonttype": 42,
})

res = pd.read_csv("output/case_study_input_depsdev_per_release_results.csv")
res["package_release_date"] = pd.to_datetime(res["package_release_date"])
db = pd.read_csv("case_study/case_database.csv")


def style_axis(ax, title, ylabel=None):
    ax.set_title(title, fontsize=10.5, color=TEXT, loc="left", fontweight="bold", pad=8)
    ax.axvline(END, color=TEXT2, lw=0.7, ls=":", zorder=0)
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color(TEXT2)
    ax.tick_params(colors=TEXT2, labelsize=8.5)
    ax.grid(axis="y", color=GRID, lw=0.6, zorder=0)
    ax.set_axisbelow(True)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=9.5, color=TEXT)


def plot_pkg(ax, pkg, metric_col, color, label, agg_col):
    traj = res[(res.package_name == pkg) & (res.status == "ok")].sort_values(
        "package_release_date"
    )
    ax.plot(
        traj.package_release_date, traj[metric_col], color=color, lw=2,
        solid_capstyle="round", zorder=3, marker="o", markersize=2.5, label=label,
    )
    row = db[db.package_name == pkg].iloc[0]
    agg = row[agg_col]
    last_date = traj.package_release_date.iloc[-1]
    last_val = traj[metric_col].iloc[-1]
    ax.plot([last_date, END], [last_val, agg], color=color, lw=2, ls=(0, (3, 2)), zorder=3)


def add_legend(ax, loc="upper left"):
    leg = ax.legend(
        loc=loc, fontsize=8, frameon=True, framealpha=0.92, edgecolor=GRID,
        borderpad=0.6, handlelength=1.6, labelcolor=TEXT,
    )
    leg.get_frame().set_linewidth(0.7)


def annotate_at(ax, date, value, text, text_date, text_value, color, ha="left"):
    ax.annotate(
        text, xy=(pd.Timestamp(date), value), xytext=(pd.Timestamp(text_date), text_value),
        textcoords="data", fontsize=7.3, color=color, ha=ha,
        arrowprops=dict(arrowstyle="-", color=color, lw=0.7, shrinkA=2, shrinkB=4),
    )


def annotate_offset(ax, date, value, text, dx, dy, color, ha="left"):
    ax.annotate(
        text, xy=(pd.Timestamp(date), value), xytext=(dx, dy), textcoords="offset points",
        fontsize=7.3, color=color, ha=ha,
        arrowprops=dict(arrowstyle="-", color=color, lw=0.7, shrinkA=2, shrinkB=4),
    )


def save(fig, name):
    fig.savefig(f"case_study/{name}.pdf", bbox_inches="tight", pad_inches=0.03)
    fig.savefig(f"case_study/{name}.png", dpi=300, bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)


CAPTION = (
    "solid line: value at each release   ·   dashed line: drift after the last "
    "release, up to the measurement date (dotted line)"
)


# ------------------------------------------------------------------ Figure 1: MTTU
fig, axes = plt.subplots(1, 2, figsize=(9.8, 3.6))

ax = axes[0]
plot_pkg(ax, "electron-rebuilder", "mttu", BLUE, "electron-rebuilder (npm)", "mttu_recomputed")
plot_pkg(ax, "odoo12-addon-account-cutoff-accrual-picking", "mttu", VIOLET, "odoo12 accrual-picking (PyPI)", "mttu_recomputed")
style_axis(ax, "(a) Score mostly worsened over time", ylabel="MTTU (days)")
ax.set_xlim(pd.Timestamp("2019-06-01"), pd.Timestamp("2028-06-01"))
ax.set_ylim(-8, 130)
add_legend(ax, loc="upper left")
annotate_at(ax, "2022-06-07", 0, "15 releases in\n2 days, June 2022", "2019-09-01", 78, BLUE)
annotate_offset(ax, "2024-06-19", 23.9, "then fixed\n9 days later", 15, -20, BLUE)

ax = axes[1]
plot_pkg(ax, "koatty_proto", "mttu", ORANGE, "koatty_proto (npm)", "mttu_recomputed")
plot_pkg(ax, "azure_mgmt_botservice", "mttu", AQUA, "Azure SDK (3 crates, Cargo)", "mttu_recomputed")
style_axis(ax, "(b) Score mostly improved over time")
ax.set_xlim(pd.Timestamp("2019-06-01"), pd.Timestamp("2028-06-01"))
ax.set_ylim(-2, 50)
add_legend(ax, loc="upper left")
annotate_at(ax, "2026-01-28", 42.1, "fell behind,\nfixed same day", "2023-10-01", 40, ORANGE, ha="left")

fig.text(0.5, -0.01, CAPTION, ha="center", fontsize=8, color=TEXT2)
fig.tight_layout()
save(fig, "fig_mttu")


# ------------------------------------------------------------------ Figure 2: MTTR
fig, axes = plt.subplots(1, 2, figsize=(9.8, 3.6))

ax = axes[0]
plot_pkg(ax, "tradingkit", "mttr_all_severities", BLUE, "tradingkit (PyPI)", "mttr_recomputed")
plot_pkg(ax, "quiltz-testsupport", "mttr_all_severities", VIOLET, "quiltz-testsupport (PyPI)", "mttr_recomputed")
style_axis(ax, "(a) Score mostly worsened over time", ylabel="MTTR (days)")
ax.set_xlim(pd.Timestamp("2020-06-01"), pd.Timestamp("2028-06-01"))
ax.set_ylim(-10, 165)
add_legend(ax, loc="upper left")
annotate_at(ax, "2021-10-13", 6.15, "13 patch releases\nin 3 weeks", "2020-08-01", 118, BLUE)
annotate_at(ax, "2023-05-28", 1.28, "no release since\n(1,135 days)", "2024-05-01", 35, BLUE)
annotate_offset(ax, "2023-01-08", 0, "no release since\n(1,275 days)", -105, -45, VIOLET)

ax = axes[1]
plot_pkg(ax, "@electron-forge/plugin-webpack", "mttr_all_severities", ORANGE, "@electron-forge/plugin-webpack (npm)", "mttr_recomputed")
plot_pkg(ax, "aiidalab", "mttr_all_severities", AQUA, "aiidalab (PyPI)", "mttr_recomputed")
style_axis(ax, "(b) Score mostly improved over time")
ax.set_xlim(pd.Timestamp("2020-06-01"), pd.Timestamp("2028-06-01"))
ax.set_ylim(-1, 8)
add_legend(ax, loc="upper left")
annotate_offset(ax, "2025-05-09", 3.31, "114 releases total;\nstill shipping in 2026", -102, 32, ORANGE)

fig.text(0.5, -0.01, CAPTION, ha="center", fontsize=8, color=TEXT2)
fig.tight_layout()
save(fig, "fig_mttr")

print("saved case_study/fig_mttu.{pdf,png} and case_study/fig_mttr.{pdf,png}")
