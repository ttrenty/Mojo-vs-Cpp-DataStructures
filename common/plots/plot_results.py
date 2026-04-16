"""Generate publication-quality figures from the full benchmark sweep."""

from __future__ import annotations

import argparse
import csv
import os
import shutil
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MPLCONFIGDIR = ROOT / "results" / ".matplotlib"
MPLCONFIGDIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(MPLCONFIGDIR))

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib import colors as mcolors

PROCESSED_DIR = ROOT / "results" / "processed"
CODE_METRICS_PATH = ROOT / "results" / "processed" / "code_metrics.csv"
BUILD_METRICS_PATH = ROOT / "results" / "processed" / "build_metrics.csv"
OUTPUT_BASE_DIR = ROOT / "results" / "figures"
OUTPUT_DIR = OUTPUT_BASE_DIR / "full"
PROFILE_ALIASES = {"report": "representative"}
LATEX_TEXT_AVAILABLE = all(
    shutil.which(tool) is not None for tool in ("latex", "dvipng", "dvips", "gs")
)

CPP_COLOR = "#1B4F72"
MOJO_COLOR = "#D35400"
CPP_LIGHT = "#5DADE2"
MOJO_LIGHT = "#F0B27A"
PALETTE = {"cpp": CPP_COLOR, "mojo": MOJO_COLOR}
PALETTE_LIGHT = {"cpp": CPP_LIGHT, "mojo": MOJO_LIGHT}
LANG_ORDER = ["cpp", "mojo"]
LANG_LABEL = {"cpp": "C++", "mojo": "Mojo"}

BPK_COLORS = {8: "#E74C3C", 10: "#3498DB", 12: "#2ECC71", 14: "#9B59B6"}
Q_COLORS = {16: "#E74C3C", 18: "#3498DB", 20: "#2ECC71"}
DENSITY_COLORS = {"dense": "#2ECC71", "medium": "#3498DB", "sparse": "#E74C3C"}
DENSITY_ORDER = ["dense", "medium", "sparse"]

WORKLOAD_LABELS = {
    "build": "Build",
    "build_insert": "Build (insert)",
    "contains_negative": "Negative queries",
    "contains_mixed": "Mixed queries",
    "read_heavy": "Read-heavy",
    "mixed_ops": "Mixed ops",
    "contains_delete_heavy": "Post-delete contains",
    "erase_delete_heavy": "Erase (delete-heavy)",
    "select": "Select",
    "predecessor": "Predecessor",
    "contains": "Contains",
}


def canonical_profile(profile: str) -> str:
    return PROFILE_ALIASES.get(profile, profile)


def setup_style():
    style = {
        "font.size": 10,
        "axes.titlesize": 12,
        "axes.titleweight": "bold",
        "axes.labelsize": 10,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "legend.fontsize": 9,
        "figure.dpi": 150,
        "savefig.dpi": 250,
        "savefig.bbox": "tight",
        "savefig.facecolor": "white",
        "figure.facecolor": "white",
        "axes.facecolor": "white",
        "axes.grid": True,
        "axes.xmargin": 0.01,
        "axes.ymargin": 0.02,
        "grid.alpha": 0.3,
        "grid.linestyle": ":",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
        "savefig.pad_inches": 0.03,
    }
    if LATEX_TEXT_AVAILABLE:
        style.update(
            {
                "text.usetex": True,
                "font.family": "serif",
                "font.serif": ["Latin Modern Roman", "Computer Modern Roman"],
                "text.latex.preamble": r"\usepackage[T1]{fontenc}\usepackage{lmodern}\usepackage{amsmath}",
            }
        )
    else:
        style.update(
            {
                "font.family": "serif",
                "font.serif": [
                    "Latin Modern Roman",
                    "CMU Serif",
                    "Computer Modern Roman",
                    "DejaVu Serif",
                ],
                "mathtext.fontset": "cm",
            }
        )
    plt.rcParams.update(style)


def tex_safe(text: str) -> str:
    if not LATEX_TEXT_AVAILABLE:
        return text
    return text.replace("%", r"\%")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate figures for one benchmark profile.")
    parser.add_argument(
        "--profile",
        choices=("smoke", "representative", "full", "report"),
        default="full",
        help="Processed summary profile to visualize.",
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=None,
        help="Optional summary CSV path. Defaults to results/processed/<profile>_summary.csv.",
    )
    parser.add_argument(
        "--output-dir-name",
        default=None,
        help="Optional figure-output directory name under results/figures/. Defaults to the selected profile.",
    )
    args = parser.parse_args()
    args.profile = canonical_profile(args.profile)
    args.output_dir_name = args.output_dir_name or args.profile
    return args


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def sf(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def try_float(value: str | None) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def si(value: str) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def param(row: dict[str, str], key: str) -> str | None:
    if key in row:
        return row[key]
    for item in row.get("param_tuple", "").split(";"):
        if "=" in item:
            k, v = item.split("=", 1)
            if k == key:
                return v
    return None


def param_float(row: dict[str, str], key: str) -> float:
    v = param(row, key)
    return sf(v) if v else 0.0


def param_int(row: dict[str, str], key: str) -> int:
    v = param(row, key)
    return si(v) if v else 0


def rows_for(
    rows: list[dict],
    structure: str,
    workload: str | None = None,
    language: str | None = None,
) -> list[dict]:
    out = [r for r in rows if r["structure"] == structure]
    if workload:
        out = [r for r in out if r["workload"] == workload]
    if language:
        out = [r for r in out if r["language"] == language]
    return out


def pick_best_match(rows: list[dict], **match: str) -> dict | None:
    candidates: list[dict] = []
    for row in rows:
        row_matches = True
        for key, expected_value in match.items():
            actual_value = param(row, key)
            if actual_value == expected_value:
                continue
            actual_float = try_float(actual_value)
            expected_float = try_float(expected_value)
            if actual_float is not None and expected_float is not None:
                if abs(actual_float - expected_float) < 0.01:
                    continue
            row_matches = False
            break
        if row_matches:
            candidates.append(row)
    if not candidates:
        return None
    candidates.sort(
        key=lambda row: (
            si(row.get("run_count", "0")),
            -sf(row.get("throughput_ci95_ns_per_op", "0")),
        ),
        reverse=True,
    )
    return candidates[0]


def fmt_ops(v: float) -> str:
    if v >= 1e9:
        return f"{v / 1e9:.1f}G"
    if v >= 1e6:
        return f"{v / 1e6:.1f}M"
    if v >= 1e3:
        return f"{v / 1e3:.1f}K"
    return f"{v:.0f}"


def fmt_signed_ops(v: float) -> str:
    sign = "+" if v >= 0 else "-"
    return sign + fmt_ops(abs(v))


def save_figure(fig, stem: str) -> None:
    fig.savefig(OUTPUT_DIR / f"{stem}.png")
    fig.savefig(OUTPUT_DIR / f"{stem}.pdf")


def throughput_band(row: dict[str, str]) -> tuple[float, float, float]:
    median = sf(row.get("throughput_median_ops_per_sec", "0"))
    lower_raw = row.get("throughput_p25_ops_per_sec", "")
    upper_raw = row.get("throughput_p75_ops_per_sec", "")
    lower = sf(lower_raw) if lower_raw else median
    upper = sf(upper_raw) if upper_raw else median
    return lower, median, upper


def throughput_error_bounds(row: dict[str, str]) -> tuple[float, float]:
    lower, median, upper = throughput_band(row)
    return max(median - lower, 0.0), max(upper - median, 0.0)


def ratio_band(
    cpp_row: dict[str, str], mojo_row: dict[str, str]
) -> tuple[float, float, float]:
    cpp_low, cpp_mid, cpp_high = throughput_band(cpp_row)
    mojo_low, mojo_mid, mojo_high = throughput_band(mojo_row)
    if cpp_mid <= 0 or cpp_low <= 0 or cpp_high <= 0:
        return 0.0, 0.0, 0.0
    ratio_mid = mojo_mid / cpp_mid
    ratio_low = mojo_low / cpp_high
    ratio_high = mojo_high / cpp_low
    return ratio_low, ratio_mid, ratio_high


def overlay_vertical_errorbars(
    ax,
    centers: list[float],
    values: list[float],
    err_low: list[float],
    err_high: list[float],
) -> None:
    ax.errorbar(
        centers,
        values,
        yerr=[err_low, err_high],
        fmt="none",
        ecolor="black",
        elinewidth=1.3,
        capsize=4,
        capthick=1.3,
        alpha=0.5,
        zorder=5,
    )


def overlay_horizontal_errorbars(
    ax,
    values: list[float],
    centers: list[float],
    err_low: list[float],
    err_high: list[float],
) -> None:
    ax.errorbar(
        values,
        centers,
        xerr=[err_low, err_high],
        fmt="none",
        ecolor="black",
        elinewidth=1.3,
        capsize=3,
        capthick=1.3,
        alpha=0.5,
        zorder=5,
    )


def plot_series_with_band(
    ax,
    xs: list[float],
    series_rows: list[dict[str, str]],
    *,
    color: str,
    label: str,
    line_style: str,
    marker: str,
    linewidth: float = 2.0,
    markersize: float = 6.0,
    line_alpha: float = 1.0,
    band_alpha: float = 0.15,
) -> None:
    if not xs or not series_rows:
        return
    lower = []
    median = []
    upper = []
    for row in series_rows:
        low, mid, high = throughput_band(row)
        lower.append(low)
        median.append(mid)
        upper.append(high)
    edge_alpha = min(max(band_alpha * 1.9, 0.28), 0.45)
    ax.fill_between(xs, lower, upper, color=color, alpha=band_alpha, linewidth=0, zorder=2)
    ax.plot(xs, lower, linestyle=line_style, color=color, alpha=edge_alpha, linewidth=0.9, zorder=2.2)
    ax.plot(xs, upper, linestyle=line_style, color=color, alpha=edge_alpha, linewidth=0.9, zorder=2.2)
    ax.plot(
        xs,
        median,
        linestyle=line_style,
        marker=marker,
        color=color,
        alpha=line_alpha,
        label=label,
        linewidth=linewidth,
        markersize=markersize,
        zorder=3,
    )


def add_uncertainty_note(fig) -> None:
    fig.text(
        0.5,
        0.01,
        "Line = median throughput; shaded band = interquartile range across timed runs.",
        ha="center",
        va="bottom",
        fontsize=13,
        color="#566573",
    )


def add_bar_uncertainty_note(fig, text: str) -> None:
    fig.text(
        0.5,
        0.01,
        text,
        ha="center",
        va="bottom",
        fontsize=13,
        color="#566573",
    )


CASE_LABELS = {
    "bits_per_key": "bpk",
    "target_load": "load",
    "remainder_bits": "r",
    "query_mode": "mode",
}
CASE_ORDER = ["n", "q", "remainder_bits", "target_load", "bits_per_key", "density", "query_mode"]


def compact_case_value(key: str, value: str) -> str:
    if key == "n":
        n = si(value)
        if n >= 1_000_000:
            scaled = n / 1_000_000
            return f"n={scaled:.0f}M" if scaled.is_integer() else f"n={scaled:.1f}M"
        if n >= 1_000:
            scaled = n / 1_000
            return f"n={scaled:.0f}K" if scaled.is_integer() else f"n={scaled:.1f}K"
        return f"n={n}"
    if key == "target_load":
        return f"load={sf(value):.2f}"
    label = CASE_LABELS.get(key, key)
    return f"{label}={value}"


def format_case_label(workload: str, match: dict[str, str]) -> str:
    summary = [
        compact_case_value(key, match[key]) for key in CASE_ORDER if key in match
    ]
    return WORKLOAD_LABELS.get(workload, workload) + "\n" + ", ".join(summary)


# ── Figure 1: Hero throughput overview ─────────────────────────────────────────


def figure_01_throughput_overview(rows: list[dict]) -> None:
    structure_specs = {
        "Blocked Bloom": [
            ("blocked_bloom", "build", {"n": "100000", "bits_per_key": "10"}, "Build"),
            (
                "blocked_bloom",
                "contains_negative",
                {"n": "100000", "bits_per_key": "10"},
                "Negative",
            ),
            (
                "blocked_bloom",
                "contains_mixed",
                {"n": "100000", "bits_per_key": "10"},
                "Mixed",
            ),
        ],
        "Quotient Filter": [
            (
                "quotient_filter",
                "build_insert",
                {"q": "16", "remainder_bits": "12", "target_load": "0.7"},
                "Build",
            ),
            (
                "quotient_filter",
                "read_heavy",
                {"q": "16", "remainder_bits": "12", "target_load": "0.7"},
                "Read",
            ),
            (
                "quotient_filter",
                "mixed_ops",
                {"q": "16", "remainder_bits": "12", "target_load": "0.7"},
                "Mixed",
            ),
            (
                "quotient_filter",
                "erase_delete_heavy",
                {"q": "16", "remainder_bits": "12", "target_load": "0.7"},
                "Erase",
            ),
            (
                "quotient_filter",
                "contains_delete_heavy",
                {"q": "16", "remainder_bits": "12", "target_load": "0.7"},
                "Contains\nafter delete",
            ),
        ],
        "Elias-Fano": [
            ("elias_fano", "build", {"n": "100000", "density": "medium"}, "Build"),
            ("elias_fano", "contains", {"n": "100000", "density": "medium"}, "Contains"),
            ("elias_fano", "select", {"n": "100000", "density": "medium"}, "Select"),
            (
                "elias_fano",
                "predecessor",
                {"n": "100000", "density": "medium"},
                "Predecessor",
            ),
        ],
    }

    fig, axes = plt.subplots(1, 3, figsize=(16.8, 4.2))
    handles = None

    for ax, (structure_name, specs) in zip(axes, structure_specs.items()):
        labels = []
        cpp_vals, mojo_vals = [], []
        cpp_err_low, cpp_err_high = [], []
        mojo_err_low, mojo_err_high = [], []

        for struct, wl, match, label in specs:
            labels.append(label)
            for lang, vals, errs in [
                ("cpp", cpp_vals, (cpp_err_low, cpp_err_high)),
                ("mojo", mojo_vals, (mojo_err_low, mojo_err_high)),
            ]:
                candidates = rows_for(rows, struct, wl, lang)
                best = pick_best_match(candidates, **match)
                if best is None and candidates:
                    best = candidates[0]
                vals.append(sf(best["throughput_median_ops_per_sec"]) if best else 0)
                if best:
                    low_err, high_err = throughput_error_bounds(best)
                else:
                    low_err, high_err = 0.0, 0.0
                errs[0].append(low_err)
                errs[1].append(high_err)

        x = list(range(len(labels)))
        w = 0.36
        bars_cpp = ax.bar(
            [i - w / 2 for i in x],
            cpp_vals,
            w,
            color=CPP_COLOR,
            label="C++",
            zorder=3,
        )
        bars_mojo = ax.bar(
            [i + w / 2 for i in x],
            mojo_vals,
            w,
            color=MOJO_COLOR,
            label="Mojo",
            zorder=3,
        )
        if handles is None:
            handles = [bars_cpp[0], bars_mojo[0]]
        overlay_vertical_errorbars(
            ax,
            [bar.get_x() + bar.get_width() / 2 for bar in bars_cpp],
            cpp_vals,
            cpp_err_low,
            cpp_err_high,
        )
        overlay_vertical_errorbars(
            ax,
            [bar.get_x() + bar.get_width() / 2 for bar in bars_mojo],
            mojo_vals,
            mojo_err_low,
            mojo_err_high,
        )

        ax.set_yscale("log")
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=20, ha="right", fontsize=8.5)
        ax.set_title(structure_name, fontweight="bold")
        ax.grid(axis="y", alpha=0.15, zorder=0)

        for i, (c, m) in enumerate(zip(cpp_vals, mojo_vals)):
            if c > 0 and m > 0:
                ratio = m / c
                color = "#27AE60" if ratio >= 0.95 else "#C0392B"
                ax.annotate(
                    f"{ratio:.2f}x",
                    (i, max(c, m)),
                    ha="center",
                    va="bottom",
                    fontsize=10.5,
                    color=color,
                    fontweight="bold",
                    zorder=7,
                    bbox=dict(
                        boxstyle="round,pad=0.18",
                        facecolor="white",
                        edgecolor="none",
                        alpha=0.88,
                    ),
                )

    axes[0].set_ylabel("Median throughput (ops/sec)")
    if handles is not None:
        fig.legend(
            handles,
            ["C++", "Mojo"],
            loc="upper center",
            bbox_to_anchor=(0.5, 0.98),
            ncol=2,
            framealpha=0.9,
        )
    fig.suptitle(
        "Throughput Overview: one selected configuration per workload",
        fontweight="bold",
        y=1.06,
    )
    add_bar_uncertainty_note(
        fig,
        "Bars show median throughput; error bars show the interquartile range across timed runs.",
    )
    fig.tight_layout(rect=(0, 0.05, 1, 0.90))
    save_figure(fig, "fig01_throughput_overview")
    plt.close(fig)


# ── Figure 2: Blocked Bloom scaling with n ─────────────────────────────────────


def figure_02_bloom_scaling(rows: list[dict]) -> None:
    workloads = ["build", "contains_negative", "contains_mixed"]
    fig, axes = plt.subplots(1, 3, figsize=(16, 5), sharey=False)

    for ax_idx, wl in enumerate(workloads):
        ax = axes[ax_idx]
        for lang in LANG_ORDER:
            subset = rows_for(rows, "blocked_bloom", wl, lang)
            bpk10 = [r for r in subset if param(r, "bits_per_key") == "10"]
            if wl == "build":
                bpk10 = [r for r in bpk10 if param(r, "query_mode") == "negative"]
            bpk10.sort(key=lambda r: si(r["n"]))
            ns = [si(r["n"]) for r in bpk10]
            if ns:
                plot_series_with_band(
                    ax,
                    ns,
                    bpk10,
                    color=PALETTE[lang],
                    label=LANG_LABEL[lang],
                    line_style="-" if lang == "cpp" else "--",
                    marker="o" if lang == "cpp" else "s",
                    linewidth=2,
                    markersize=6,
                    band_alpha=0.15 if lang == "cpp" else 0.18,
                )

        ax.set_xscale("log")
        ax.set_xlabel("Number of keys (n)")
        ax.set_ylabel("Throughput (ops/sec)")
        ax.set_title(WORKLOAD_LABELS.get(wl, wl))
        ax.legend()
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: fmt_ops(v)))
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: fmt_ops(v)))

    fig.suptitle(
        "Blocked Bloom Filter: Throughput Scaling with Dataset Size (bits/key=10)",
        fontweight="bold",
        y=1.02,
    )
    add_uncertainty_note(fig)
    fig.tight_layout(rect=(0, 0.04, 1, 0.98))
    save_figure(fig, "fig02_bloom_scaling")
    plt.close(fig)


# ── Figure 3: Blocked Bloom bits_per_key sweep ────────────────────────────────


def figure_03_bloom_bpk_sweep(rows: list[dict]) -> None:
    workloads = ["build", "contains_negative", "contains_mixed"]
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))

    for ax_idx, wl in enumerate(workloads):
        ax = axes[ax_idx]
        for lang in LANG_ORDER:
            subset = rows_for(rows, "blocked_bloom", wl, lang)
            n100k = [r for r in subset if si(r["n"]) == 100000]
            if wl == "build":
                n100k = [r for r in n100k if param(r, "query_mode") == "negative"]
            n100k.sort(key=lambda r: param_int(r, "bits_per_key"))
            unique_rows: list[dict[str, str]] = []
            seen_bpks: set[int] = set()
            for row in n100k:
                bits_per_key = param_int(row, "bits_per_key")
                if bits_per_key in seen_bpks:
                    continue
                seen_bpks.add(bits_per_key)
                unique_rows.append(row)
            bpks = [param_int(r, "bits_per_key") for r in unique_rows]
            if bpks:
                plot_series_with_band(
                    ax,
                    bpks,
                    unique_rows,
                    color=PALETTE[lang],
                    label=LANG_LABEL[lang],
                    line_style="-" if lang == "cpp" else "--",
                    marker="o" if lang == "cpp" else "s",
                    linewidth=2,
                    markersize=7,
                    band_alpha=0.15 if lang == "cpp" else 0.18,
                )

        ax.set_xlabel("Bits per key")
        ax.set_ylabel("Throughput (ops/sec)")
        ax.set_title(WORKLOAD_LABELS.get(wl, wl))
        ax.legend()
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: fmt_ops(v)))

    fig.suptitle(
        "Blocked Bloom Filter: Impact of bits per key (n=100K)",
        fontweight="bold",
        y=1.02,
    )
    add_uncertainty_note(fig)
    fig.tight_layout(rect=(0, 0.04, 1, 0.98))
    save_figure(fig, "fig03_bloom_bpk_sweep")
    plt.close(fig)


# ── Figure 4: Blocked Bloom FPR analysis ──────────────────────────────────────


def figure_04_bloom_fpr(rows: list[dict]) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))

    # Left: FPR vs bits_per_key for different n values
    ax = axes[0]
    for lang in LANG_ORDER:
        subset = rows_for(rows, "blocked_bloom", "contains_negative", lang)
        n100k = [r for r in subset if si(r["n"]) == 100000]
        n100k.sort(key=lambda r: param_int(r, "bits_per_key"))
        seen = {}
        for r in n100k:
            bpk = param_int(r, "bits_per_key")
            if bpk not in seen:
                seen[bpk] = sf(r["fpr"])
        bpks = sorted(seen.keys())
        fprs = [seen[b] for b in bpks]
        if bpks:
            style = "-o" if lang == "cpp" else "--s"
            ax.plot(
                bpks,
                fprs,
                style,
                color=PALETTE[lang],
                label=LANG_LABEL[lang],
                linewidth=2,
                markersize=7,
            )

    ax.set_xlabel("Bits per key")
    ax.set_ylabel("False positive rate")
    ax.set_title("FPR vs bits/key (n=100K)")
    ax.legend()

    # Right: FPR vs n for different bits_per_key
    ax = axes[1]
    for bpk in [8, 10, 12, 14]:
        subset = rows_for(rows, "blocked_bloom", "contains_negative", "cpp")
        matching = [r for r in subset if param_int(r, "bits_per_key") == bpk]
        matching.sort(key=lambda r: si(r["n"]))
        ns = [si(r["n"]) for r in matching]
        fprs = [sf(r["fpr"]) for r in matching]
        if ns:
            ax.plot(
                ns,
                fprs,
                "-o",
                color=BPK_COLORS[bpk],
                label=f"bits/key={bpk}",
                linewidth=2,
                markersize=5,
            )

    ax.set_xscale("log")
    ax.set_xlabel("Number of keys (n)")
    ax.set_ylabel("False positive rate")
    ax.set_title("FPR vs dataset size (C++)")
    ax.legend()
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: fmt_ops(v)))

    fig.suptitle(
        "Blocked Bloom Filter: False Positive Rate Analysis", fontweight="bold", y=1.02
    )
    fig.tight_layout()
    save_figure(fig, "fig04_bloom_fpr")
    plt.close(fig)


# ── Figure 5: Quotient Filter workload comparison ─────────────────────────────


def figure_05_qf_workloads(rows: list[dict]) -> None:
    workloads = [
        "build_insert",
        "read_heavy",
        "mixed_ops",
        "erase_delete_heavy",
        "contains_delete_heavy",
    ]
    load_factors = sorted(
        {
            round(param_float(row, "target_load"), 2)
            for row in rows
            if row.get("structure") == "quotient_filter"
            and param(row, "q") == "16"
            and param(row, "remainder_bits") == "12"
            and param(row, "target_load") is not None
            and param_float(row, "target_load") > 0.0
        }
    ) or [0.3, 0.4, 0.5, 0.7, 0.85]

    fig_width = max(18.0, 4.2 * len(load_factors))
    fig, axes = plt.subplots(1, len(load_factors), figsize=(fig_width, 6), sharey=True)

    for lf_idx, lf in enumerate(load_factors):
        ax = axes[lf_idx]
        x = list(range(len(workloads)))
        w = 0.35

        for lang_idx, lang in enumerate(LANG_ORDER):
            vals = []
            err_low = []
            err_high = []
            for wl in workloads:
                subset = rows_for(rows, "quotient_filter", wl, lang)
                matching = [
                    r
                    for r in subset
                    if param(r, "q") == "16"
                    and param(r, "remainder_bits") == "12"
                    and param(r, "target_load") is not None
                    and abs(param_float(r, "target_load") - lf) < 0.01
                ]
                if matching:
                    matching.sort(key=lambda row: si(row.get("run_count", "0")), reverse=True)
                    vals.append(sf(matching[0]["throughput_median_ops_per_sec"]))
                    low_err, high_err = throughput_error_bounds(matching[0])
                    err_low.append(low_err)
                    err_high.append(high_err)
                else:
                    vals.append(0)
                    err_low.append(0)
                    err_high.append(0)

            offset = -w / 2 if lang_idx == 0 else w / 2
            bars = ax.bar(
                [i + offset for i in x],
                vals,
                w,
                color=PALETTE[lang],
                label=LANG_LABEL[lang],
                zorder=3,
            )
            overlay_vertical_errorbars(
                ax,
                [bar.get_x() + bar.get_width() / 2 for bar in bars],
                vals,
                err_low,
                err_high,
            )

        ax.set_xticks(x)
        ax.set_xticklabels(
            [WORKLOAD_LABELS.get(wl, wl) for wl in workloads],
            rotation=30,
            ha="right",
            fontsize=8,
        )
        ax.set_title(f"Load factor = {lf:g}")
        ax.set_ylabel("Throughput (ops/sec)" if lf_idx == 0 else "")
        ax.set_yscale("log")
        ax.legend(loc="upper right", fontsize=8)
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: fmt_ops(v)))

    fig.suptitle(
        "Quotient Filter: Workload Profile Across Load Factors (q=16, r=12)",
        fontweight="bold",
        y=1.02,
    )
    add_bar_uncertainty_note(
        fig,
        "Bars show median throughput; error bars show the interquartile range across timed runs.",
    )
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    save_figure(fig, "fig05_qf_workloads")
    plt.close(fig)


# ── Figure 6: QF load factor sensitivity ──────────────────────────────────────


def figure_06_qf_load_sensitivity(rows: list[dict]) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))

    # Left: read_heavy throughput vs load factor, per q value
    ax = axes[0]
    for lang in LANG_ORDER:
        for q_val in [16, 18, 20]:
            subset = rows_for(rows, "quotient_filter", "read_heavy", lang)
            matching = [
                r
                for r in subset
                if param(r, "q") == str(q_val) and param(r, "remainder_bits") == "12"
            ]
            matching.sort(key=lambda r: param_float(r, "target_load"))
            loads = [param_float(r, "target_load") for r in matching]
            if loads:
                plot_series_with_band(
                    ax,
                    loads,
                    matching,
                    color=Q_COLORS[q_val],
                    label=f"{LANG_LABEL[lang]} q={q_val}",
                    line_style="-" if lang == "cpp" else "--",
                    marker="o" if lang == "cpp" else "s",
                    linewidth=2,
                    markersize=5,
                    line_alpha=1.0 if lang == "cpp" else 0.75,
                    band_alpha=0.08 if lang == "cpp" else 0.12,
                )

    ax.set_xlabel("Target load factor")
    ax.set_ylabel("Throughput (ops/sec)")
    ax.set_title("Read-heavy (r=12)")
    ax.legend(fontsize=7, ncol=2)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: fmt_ops(v)))

    # Right: build_insert throughput vs load factor
    ax = axes[1]
    for lang in LANG_ORDER:
        for q_val in [16, 18, 20]:
            subset = rows_for(rows, "quotient_filter", "build_insert", lang)
            matching = [
                r
                for r in subset
                if param(r, "q") == str(q_val) and param(r, "remainder_bits") == "12"
            ]
            matching.sort(key=lambda r: param_float(r, "target_load"))
            loads = [param_float(r, "target_load") for r in matching]
            if loads:
                plot_series_with_band(
                    ax,
                    loads,
                    matching,
                    color=Q_COLORS[q_val],
                    label=f"{LANG_LABEL[lang]} q={q_val}",
                    line_style="-" if lang == "cpp" else "--",
                    marker="o" if lang == "cpp" else "s",
                    linewidth=2,
                    markersize=5,
                    line_alpha=1.0 if lang == "cpp" else 0.75,
                    band_alpha=0.08 if lang == "cpp" else 0.12,
                )

    ax.set_xlabel("Target load factor")
    ax.set_ylabel("Throughput (ops/sec)")
    ax.set_title("Build (insert) (r=12)")
    ax.legend(fontsize=7, ncol=2)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: fmt_ops(v)))

    fig.suptitle(
        "Quotient Filter: Load Factor Sensitivity by Capacity",
        fontweight="bold",
        y=1.02,
    )
    add_uncertainty_note(fig)
    fig.tight_layout(rect=(0, 0.04, 1, 0.98))
    save_figure(fig, "fig06_qf_load_sensitivity")
    plt.close(fig)


# ── Figure 7: QF parameter heatmap ────────────────────────────────────────────


def figure_07_qf_heatmap(rows: list[dict]) -> None:
    q_source = [
        param_int(row, "q")
        for row in rows_for(rows, "quotient_filter", "read_heavy")
        if param(row, "target_load") is not None
        and abs(param_float(row, "target_load") - 0.7) < 0.01
        and param_int(row, "q") > 0
    ]
    r_source = [
        param_int(row, "remainder_bits")
        for row in rows_for(rows, "quotient_filter", "read_heavy")
        if param(row, "target_load") is not None
        and abs(param_float(row, "target_load") - 0.7) < 0.01
        and param_int(row, "remainder_bits") > 0
    ]
    q_vals = sorted(set(q_source)) or [16, 18, 20]
    r_vals = sorted(set(r_source)) or [8, 12, 16]

    matrices: dict[str, list[list[float]]] = {}
    for lang in LANG_ORDER:
        subset = rows_for(rows, "quotient_filter", "read_heavy", lang)
        load07 = [
            r
            for r in subset
            if param(r, "target_load") is not None
            and abs(param_float(r, "target_load") - 0.7) < 0.01
        ]

        matrix = []
        for q in q_vals:
            row_data = []
            for r in r_vals:
                best = pick_best_match(
                    load07, q=str(q), remainder_bits=str(r), target_load="0.7"
                )
                if best:
                    row_data.append(sf(best["throughput_median_ops_per_sec"]))
                else:
                    row_data.append(0)
            matrix.append(row_data)
        matrices[lang] = matrix

    cpp_matrix = matrices.get("cpp", [[0.0 for _ in r_vals] for _ in q_vals])
    mojo_matrix = matrices.get("mojo", [[0.0 for _ in r_vals] for _ in q_vals])
    gap_matrix: list[list[float]] = []
    for i in range(len(q_vals)):
        row_data = []
        for j in range(len(r_vals)):
            cpp_val = cpp_matrix[i][j]
            mojo_val = mojo_matrix[i][j]
            if cpp_val > 0:
                row_data.append(100.0 * (mojo_val - cpp_val) / cpp_val)
            else:
                row_data.append(0.0)
        gap_matrix.append(row_data)

    fig, axes = plt.subplots(1, 3, figsize=(18.8, 5.4), constrained_layout=True)
    throughput_max = max(
        max(max(row) for row in cpp_matrix),
        max(max(row) for row in mojo_matrix),
        1.0,
    )
    diff_abs_max = max(abs(value) for row in gap_matrix for value in row)
    diff_norm = mcolors.TwoSlopeNorm(
        vmin=-max(diff_abs_max, 1.0), vcenter=0.0, vmax=max(diff_abs_max, 1.0)
    )
    heatmap_specs = [
        ("cpp", cpp_matrix, f"{LANG_LABEL['cpp']} (load=0.7, read-heavy)", "YlOrRd", None),
        ("mojo", mojo_matrix, f"{LANG_LABEL['mojo']} (load=0.7, read-heavy)", "YlOrRd", None),
        ("diff", gap_matrix, tex_safe("Mojo vs C++ (% gap)"), "RdBu_r", diff_norm),
    ]

    throughput_images = []
    diff_image = None
    for ax_idx, (_, matrix, title, cmap, norm) in enumerate(heatmap_specs):
        ax = axes[ax_idx]
        if ax_idx < 2:
            im = ax.imshow(
                matrix, cmap=cmap, aspect="auto", vmin=0.0, vmax=throughput_max
            )
            throughput_images.append(im)
        else:
            im = ax.imshow(matrix, cmap=cmap, aspect="auto", norm=norm)
            diff_image = im
        ax.set_xticks(range(len(r_vals)))
        ax.set_xticklabels([f"r={r}" for r in r_vals])
        ax.set_yticks(range(len(q_vals)))
        ax.set_yticklabels([f"q={q}" for q in q_vals])
        ax.set_title(title)

        for i in range(len(q_vals)):
            for j in range(len(r_vals)):
                val = matrix[i][j]
                if ax_idx < 2:
                    text_label = fmt_ops(val)
                    high_contrast = val > throughput_max * 0.6
                else:
                    text_label = tex_safe(f"{val:+.1f}%")
                    high_contrast = abs(val) > max(diff_abs_max, 1.0) * 0.55
                ax.text(
                    j,
                    i,
                    text_label,
                    ha="center",
                    va="center",
                    color="white" if high_contrast else "black",
                    fontsize=10,
                    fontweight="bold",
                )
    if throughput_images:
        fig.colorbar(
            throughput_images[0],
            ax=axes[:2],
            shrink=0.82,
            pad=0.03,
            label="ops/sec",
        )
    if diff_image is not None:
        fig.colorbar(
            diff_image,
            ax=axes[2],
            shrink=0.82,
            pad=0.03,
            label=tex_safe("Mojo vs C++ (% gap)"),
        )

    fig.suptitle(
        "Quotient Filter: Throughput Heatmap and Cross-Language Gap",
        fontweight="bold",
        y=1.02,
    )
    save_figure(fig, "fig07_qf_heatmap")
    plt.close(fig)


# ── Figure 8: Elias-Fano density x workload ──────────────────────────────────


def figure_08_ef_density(rows: list[dict]) -> None:
    workloads = ["build", "contains", "select", "predecessor"]
    fig, axes = plt.subplots(1, 4, figsize=(18, 5))

    for ax_idx, wl in enumerate(workloads):
        ax = axes[ax_idx]
        x = list(range(len(DENSITY_ORDER)))
        w = 0.35

        for lang_idx, lang in enumerate(LANG_ORDER):
            vals = []
            err_low = []
            err_high = []
            for density in DENSITY_ORDER:
                subset = rows_for(rows, "elias_fano", wl, lang)
                matching = [
                    r
                    for r in subset
                    if r.get("dataset") == density and si(r["n"]) == 100000
                ]
                if matching:
                    vals.append(sf(matching[0]["throughput_median_ops_per_sec"]))
                    low_err, high_err = throughput_error_bounds(matching[0])
                    err_low.append(low_err)
                    err_high.append(high_err)
                else:
                    vals.append(0)
                    err_low.append(0)
                    err_high.append(0)

            offset = -w / 2 if lang_idx == 0 else w / 2
            bars = ax.bar(
                [i + offset for i in x],
                vals,
                w,
                color=PALETTE[lang],
                label=LANG_LABEL[lang],
                zorder=3,
            )
            overlay_vertical_errorbars(
                ax,
                [bar.get_x() + bar.get_width() / 2 for bar in bars],
                vals,
                err_low,
                err_high,
            )

        ax.set_xticks(x)
        ax.set_xticklabels([d.title() for d in DENSITY_ORDER])
        ax.set_title(WORKLOAD_LABELS.get(wl, wl))
        ax.set_ylabel("Throughput (ops/sec)" if ax_idx == 0 else "")
        if max(vals) > 0:
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: fmt_ops(v)))
        ax.legend(fontsize=7)

    fig.suptitle(
        "Elias-Fano: Throughput by Density and Workload (n=100K)",
        fontweight="bold",
        y=1.02,
    )
    add_bar_uncertainty_note(
        fig,
        "Bars show median throughput; error bars show the interquartile range across timed runs.",
    )
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    save_figure(fig, "fig08_ef_density")
    plt.close(fig)


# ── Figure 9: Elias-Fano scaling ─────────────────────────────────────────────


def figure_09_ef_scaling(rows: list[dict]) -> None:
    workloads = ["build", "contains", "select", "predecessor"]
    fig, axes = plt.subplots(1, 4, figsize=(18, 5))

    for ax_idx, wl in enumerate(workloads):
        ax = axes[ax_idx]
        for lang in LANG_ORDER:
            subset = rows_for(rows, "elias_fano", wl, lang)
            medium = [r for r in subset if r.get("dataset") == "medium"]
            medium.sort(key=lambda r: si(r["n"]))
            ns = [si(r["n"]) for r in medium]
            if ns:
                plot_series_with_band(
                    ax,
                    ns,
                    medium,
                    color=PALETTE[lang],
                    label=LANG_LABEL[lang],
                    line_style="-" if lang == "cpp" else "--",
                    marker="o" if lang == "cpp" else "s",
                    linewidth=2,
                    markersize=6,
                    band_alpha=0.15 if lang == "cpp" else 0.18,
                )

        ax.set_xscale("log")
        ax.set_xlabel("n")
        ax.set_ylabel("ops/sec" if ax_idx == 0 else "")
        ax.set_title(WORKLOAD_LABELS.get(wl, wl))
        ax.legend(fontsize=8)
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: fmt_ops(v)))
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: fmt_ops(v)))

    fig.suptitle(
        "Elias-Fano: Scaling with Dataset Size (medium density)",
        fontweight="bold",
        y=1.02,
    )
    add_uncertainty_note(fig)
    fig.tight_layout(rect=(0, 0.04, 1, 0.98))
    save_figure(fig, "fig09_ef_scaling")
    plt.close(fig)


# ── Figure 10: Performance ratio dashboard ────────────────────────────────────


def figure_10_ratio_dashboard(rows: list[dict]) -> None:
    structure_workloads = {
        "Blocked Bloom": [
            (
                "blocked_bloom",
                "build",
                {"n": "100000", "bits_per_key": "10", "query_mode": "negative"},
            ),
            (
                "blocked_bloom",
                "build",
                {"n": "1000000", "bits_per_key": "10", "query_mode": "negative"},
            ),
            (
                "blocked_bloom",
                "build",
                {"n": "5000000", "bits_per_key": "10", "query_mode": "negative"},
            ),
            (
                "blocked_bloom",
                "contains_negative",
                {"n": "100000", "bits_per_key": "10", "query_mode": "negative"},
            ),
            (
                "blocked_bloom",
                "contains_negative",
                {"n": "1000000", "bits_per_key": "10", "query_mode": "negative"},
            ),
            (
                "blocked_bloom",
                "contains_mixed",
                {"n": "5000000", "bits_per_key": "10", "query_mode": "mixed"},
            ),
        ],
        "Quotient Filter": [
            (
                "quotient_filter",
                "build_insert",
                {"q": "16", "remainder_bits": "12", "target_load": "0.7"},
            ),
            (
                "quotient_filter",
                "build_insert",
                {"q": "18", "remainder_bits": "12", "target_load": "0.7"},
            ),
            (
                "quotient_filter",
                "build_insert",
                {"q": "20", "remainder_bits": "12", "target_load": "0.7"},
            ),
            (
                "quotient_filter",
                "read_heavy",
                {"q": "16", "remainder_bits": "12", "target_load": "0.7"},
            ),
            (
                "quotient_filter",
                "read_heavy",
                {"q": "18", "remainder_bits": "12", "target_load": "0.7"},
            ),
            (
                "quotient_filter",
                "read_heavy",
                {"q": "20", "remainder_bits": "12", "target_load": "0.7"},
            ),
            (
                "quotient_filter",
                "mixed_ops",
                {"q": "16", "remainder_bits": "12", "target_load": "0.7"},
            ),
            (
                "quotient_filter",
                "mixed_ops",
                {"q": "18", "remainder_bits": "12", "target_load": "0.7"},
            ),
            (
                "quotient_filter",
                "mixed_ops",
                {"q": "20", "remainder_bits": "12", "target_load": "0.7"},
            ),
        ],
        "Elias-Fano": [
            ("elias_fano", "build", {"n": "100000", "density": "medium"}),
            ("elias_fano", "build", {"n": "1000000", "density": "medium"}),
            ("elias_fano", "build", {"n": "5000000", "density": "medium"}),
            ("elias_fano", "contains", {"n": "100000", "density": "medium"}),
            ("elias_fano", "contains", {"n": "1000000", "density": "medium"}),
            ("elias_fano", "contains", {"n": "5000000", "density": "medium"}),
            ("elias_fano", "predecessor", {"n": "100000", "density": "medium"}),
            ("elias_fano", "predecessor", {"n": "1000000", "density": "medium"}),
            ("elias_fano", "predecessor", {"n": "5000000", "density": "medium"}),
        ],
    }

    all_ratios = []
    all_ratio_err_low = []
    all_ratio_err_high = []
    all_labels = []
    all_colors = []
    struct_boundaries = []
    struct_colors = {
        "Blocked Bloom": "#2ECC71",
        "Quotient Filter": "#E74C3C",
        "Elias-Fano": "#3498DB",
    }

    for struct_name, specs in structure_workloads.items():
        struct_boundaries.append(len(all_ratios))
        for struct, wl, match in specs:
            cpp_rows = rows_for(rows, struct, wl, "cpp")
            mojo_rows = rows_for(rows, struct, wl, "mojo")

            cpp_r = pick_best_match(cpp_rows, **match)
            mojo_r = pick_best_match(mojo_rows, **match)

            if cpp_r and mojo_r:
                cpp_t = sf(cpp_r["throughput_median_ops_per_sec"])
                mojo_t = sf(mojo_r["throughput_median_ops_per_sec"])
                if cpp_t > 0:
                    ratio_low, ratio, ratio_high = ratio_band(cpp_r, mojo_r)
                    label = format_case_label(wl, match)
                    all_ratios.append(ratio)
                    all_ratio_err_low.append(max(ratio - ratio_low, 0.0))
                    all_ratio_err_high.append(max(ratio_high - ratio, 0.0))
                    all_labels.append(label)
                    all_colors.append(struct_colors[struct_name])

    struct_boundaries.append(len(all_ratios))

    if not all_ratios:
        return

    fig, ax = plt.subplots(figsize=(16, 7))
    x = range(len(all_ratios))
    bar_colors = ["#27AE60" if r >= 1.0 else "#C0392B" for r in all_ratios]
    bars = ax.barh(
        x,
        all_ratios,
        color=bar_colors,
        edgecolor=[c for c in all_colors],
        linewidth=1.5,
        zorder=3,
    )
    overlay_horizontal_errorbars(
        ax,
        all_ratios,
        [bar.get_y() + bar.get_height() / 2 for bar in bars],
        all_ratio_err_low,
        all_ratio_err_high,
    )
    ax.axvline(1.0, color="#2C3E50", linewidth=2, linestyle="-", zorder=4)
    ax.set_yticks(list(x))
    ax.set_yticklabels(all_labels, fontsize=7)
    ax.set_xlabel("Mojo / C++ throughput ratio")
    ax.invert_yaxis()
    xmin = 0.0
    xmax = max(1.05, max(all_ratios) + 0.05)
    ax.set_xlim(xmin, xmax)

    for idx, (name, color) in enumerate(struct_colors.items()):
        if idx < len(struct_boundaries) - 1:
            lo = struct_boundaries[idx]
            hi = struct_boundaries[idx + 1]
            mid = (lo + hi) / 2 - 0.5
            ax.axhspan(lo - 0.5, hi - 0.5, alpha=0.06, color=color, zorder=1)
            ax.text(
                0.99,
                mid,
                name,
                ha="right",
                va="center",
                fontsize=9,
                fontweight="bold",
                color=color,
                transform=ax.get_yaxis_transform(),
                bbox=dict(
                    facecolor="white",
                    alpha=0.8,
                    edgecolor=color,
                    boxstyle="round,pad=0.2",
                ),
            )

    for i, r in enumerate(all_ratios):
        label_x = min(r + 0.01 * max(xmax, 1.0), xmax - 0.02 * max(xmax, 1.0))
        ax.text(
            label_x,
            i,
            f"{r:.2f}x",
            va="center",
            fontsize=10,
            fontweight="bold",
            zorder=7,
            bbox=dict(
                boxstyle="round,pad=0.14",
                facecolor="white",
                edgecolor="none",
                alpha=0.88,
            ),
            clip_on=True,
        )

    ax.set_title(
        "Mojo / C++ Performance Ratio Dashboard (green = Mojo faster)",
        fontweight="bold",
    )
    add_bar_uncertainty_note(
        fig,
        "Bars show median Mojo/C++ throughput ratio; error bars conservatively propagate each language's interquartile throughput range.",
    )
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    save_figure(fig, "fig10_ratio_dashboard")
    plt.close(fig)


# ── Figure 11: QF remainder_bits impact ───────────────────────────────────────


def figure_11_qf_remainder_sweep(rows: list[dict]) -> None:
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))

    for ax_idx, q_val in enumerate([16, 18, 20]):
        ax = axes[ax_idx]
        for lang in LANG_ORDER:
            subset = rows_for(rows, "quotient_filter", "read_heavy", lang)
            load07 = [
                r
                for r in subset
                if param(r, "q") == str(q_val)
                and param(r, "target_load") is not None
                and abs(param_float(r, "target_load") - 0.7) < 0.01
            ]
            load07.sort(key=lambda r: param_int(r, "remainder_bits"))
            rbs = [param_int(r, "remainder_bits") for r in load07]
            if rbs:
                plot_series_with_band(
                    ax,
                    rbs,
                    load07,
                    color=PALETTE[lang],
                    label=LANG_LABEL[lang],
                    line_style="-" if lang == "cpp" else "--",
                    marker="o" if lang == "cpp" else "s",
                    linewidth=2,
                    markersize=7,
                    band_alpha=0.15 if lang == "cpp" else 0.18,
                )

        ax.set_xlabel("Remainder bits (r)")
        ax.set_ylabel("Throughput (ops/sec)" if ax_idx == 0 else "")
        ax.set_title(f"q={q_val} (load=0.7)")
        handles, labels = ax.get_legend_handles_labels()
        if handles:
            ax.legend(fontsize=8)
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: fmt_ops(v)))

    fig.suptitle(
        "Quotient Filter: Impact of Remainder Bits on Read-Heavy Throughput",
        fontweight="bold",
        y=1.02,
    )
    add_uncertainty_note(fig)
    fig.tight_layout(rect=(0, 0.04, 1, 0.98))
    save_figure(fig, "fig11_qf_remainder_sweep")
    plt.close(fig)


# ── Figure 12: Build + Code metrics dashboard ─────────────────────────────────


def figure_12_metrics_dashboard(code_rows: list[dict], build_rows: list[dict]) -> None:
    fig = plt.figure(figsize=(16, 10))
    gs = fig.add_gridspec(2, 2, width_ratios=[1.35, 1], height_ratios=[1, 1])

    ax1 = fig.add_subplot(gs[0, 0])
    if code_rows:
        structures = []
        cpp_tokens, mojo_tokens = [], []
        for scope in ["blocked_bloom", "quotient_filter", "elias_fano"]:
            structures.append(scope.replace("_", " ").title())
            cpp_row = next(
                (
                    r
                    for r in code_rows
                    if r["language"] == "cpp" and r["scope"] == scope
                ),
                None,
            )
            mojo_row = next(
                (
                    r
                    for r in code_rows
                    if r["language"] == "mojo" and r["scope"] == scope
                ),
                None,
            )
            cpp_tokens.append(si(cpp_row["token_count"]) if cpp_row else 0)
            mojo_tokens.append(si(mojo_row["token_count"]) if mojo_row else 0)

        x = range(len(structures))
        w = 0.35
        bars1 = ax1.bar(
            [i - w / 2 for i in x], cpp_tokens, w, color=CPP_COLOR, label="C++", zorder=3
        )
        bars2 = ax1.bar(
            [i + w / 2 for i in x],
            mojo_tokens,
            w,
            color=MOJO_COLOR,
            label="Mojo",
            zorder=3,
        )

        for bar, val in list(zip(bars1, cpp_tokens)) + list(zip(bars2, mojo_tokens)):
            ax1.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max(val * 0.02, 8),
                str(val),
                ha="center",
                va="bottom",
                fontsize=11,
                fontweight="bold",
                zorder=7,
                bbox=dict(
                    boxstyle="round,pad=0.16",
                    facecolor="white",
                    edgecolor="none",
                    alpha=0.85,
                ),
            )

        ax1.set_xticks(list(x))
        ax1.set_xticklabels(structures, fontsize=9)
        ax1.set_ylabel("Lexical Tokens")
        ax1.set_title("Implementation Surface by Structure")
        ax1.legend()

    ax2 = fig.add_subplot(gs[0, 1])
    if code_rows:
        structures = []
        cpp_max_helper, mojo_max_helper = [], []
        for scope in ["blocked_bloom", "quotient_filter", "elias_fano"]:
            structures.append(scope.replace("_", " ").title())
            cpp_row = next(
                (
                    r
                    for r in code_rows
                    if r["language"] == "cpp" and r["scope"] == scope
                ),
                None,
            )
            mojo_row = next(
                (
                    r
                    for r in code_rows
                    if r["language"] == "mojo" and r["scope"] == scope
                ),
                None,
            )
            cpp_max_helper.append(si(cpp_row["max_helper_tokens"]) if cpp_row else 0)
            mojo_max_helper.append(si(mojo_row["max_helper_tokens"]) if mojo_row else 0)

        x = range(len(structures))
        w = 0.35
        bars1 = ax2.bar(
            [i - w / 2 for i in x],
            cpp_max_helper,
            w,
            color=CPP_COLOR,
            label="C++",
            zorder=3,
        )
        bars2 = ax2.bar(
            [i + w / 2 for i in x],
            mojo_max_helper,
            w,
            color=MOJO_COLOR,
            label="Mojo",
            zorder=3,
        )
        for bar, val in list(zip(bars1, cpp_max_helper)) + list(zip(bars2, mojo_max_helper)):
            ax2.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max(val * 0.025, 6),
                str(val),
                ha="center",
                va="bottom",
                fontsize=11,
                fontweight="bold",
                zorder=7,
                bbox=dict(
                    boxstyle="round,pad=0.16",
                    facecolor="white",
                    edgecolor="none",
                    alpha=0.85,
                ),
            )
        ax2.set_xticks(list(x))
        ax2.set_xticklabels(structures, fontsize=9)
        ax2.set_ylabel("Tokens in Largest Helper")
        ax2.set_title("Worst-Case Local Chunk Size")
        ax2.legend()

    ax3 = fig.add_subplot(gs[1, 0])
    if build_rows:
        langs = [r["language"] for r in build_rows]
        times = [sf(r["compile_time_mean_ms"]) for r in build_rows]
        errors = [sf(r["compile_time_stddev_ms"]) for r in build_rows]
        reps = [si(r["repetitions"]) for r in build_rows]
        bars = ax3.bar(
            langs,
            times,
            color=[PALETTE.get(l, "#888") for l in langs],
            zorder=3,
        )
        overlay_vertical_errorbars(
            ax3,
            [bar.get_x() + bar.get_width() / 2 for bar in bars],
            times,
            errors,
            errors,
        )
        for bar, t, err, rep in zip(bars, times, errors, reps):
            ax3.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max(err, 20),
                f"{t:.0f}ms\n$\\pm${err:.0f} ({rep}x)",
                ha="center",
                va="bottom",
                fontsize=11,
                fontweight="bold",
                zorder=7,
                bbox=dict(
                    boxstyle="round,pad=0.16",
                    facecolor="white",
                    edgecolor="none",
                    alpha=0.85,
                ),
            )
        ax3.set_ylabel("Milliseconds")
        ax3.set_title(r"Compile-Only Time (mean $\pm$ stddev)")

    ax4 = fig.add_subplot(gs[1, 1])
    if build_rows:
        langs = [r["language"] for r in build_rows]
        sizes = [sf(r["binary_size_bytes"]) / 1024 for r in build_rows]
        bars = ax4.bar(
            langs, sizes, color=[PALETTE.get(l, "#888") for l in langs], zorder=3
        )
        for bar, s in zip(bars, sizes):
            ax4.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 2,
                f"{s:.0f}KB",
                ha="center",
                va="bottom",
                fontsize=11,
                fontweight="bold",
                zorder=7,
                bbox=dict(
                    boxstyle="round,pad=0.16",
                    facecolor="white",
                    edgecolor="none",
                    alpha=0.85,
                ),
            )
        ax4.set_ylabel("KiB")
        ax4.set_title("Binary Size")

    fig.suptitle(
        "Implementation Metrics: Surface Size, Local Complexity, Compile Time, Binary Size",
        fontweight="bold",
        y=1.02,
    )
    fig.tight_layout()
    save_figure(fig, "fig12_metrics_dashboard")
    plt.close(fig)


# ── Figure 13: Memory efficiency deep dive ────────────────────────────────────


def figure_13_memory_efficiency(rows: list[dict]) -> None:
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))

    # Bloom: bytes/key vs bits_per_key
    ax = axes[0]
    for lang in LANG_ORDER:
        subset = rows_for(rows, "blocked_bloom", "build", lang)
        n100k = [
            r
            for r in subset
            if si(r["n"]) == 100000 and param(r, "query_mode") == "negative"
        ]
        n100k.sort(key=lambda r: param_int(r, "bits_per_key"))
        seen = {}
        for r in n100k:
            bpk = param_int(r, "bits_per_key")
            if bpk not in seen:
                seen[bpk] = sf(r["memory_bytes"]) / max(si(r["n"]), 1)
        bpks = sorted(seen)
        bpk_vals = [seen[b] for b in bpks]
        if bpks:
            style = "-o" if lang == "cpp" else "--s"
            ax.plot(
                bpks,
                bpk_vals,
                style,
                color=PALETTE[lang],
                label=LANG_LABEL[lang],
                linewidth=2,
                markersize=7,
            )

    ax.plot(
        [8, 10, 12, 14], [1.0, 1.25, 1.5, 1.75], ":k", alpha=0.4, label="Theoretical"
    )
    ax.set_xlabel("Bits per key")
    ax.set_ylabel("Bytes per key")
    ax.set_title("Blocked Bloom")
    ax.legend(fontsize=8)

    # EF: bytes/key by density
    ax = axes[1]
    x = list(range(len(DENSITY_ORDER)))
    w = 0.35
    for lang_idx, lang in enumerate(LANG_ORDER):
        vals = []
        for density in DENSITY_ORDER:
            subset = rows_for(rows, "elias_fano", "build", lang)
            matching = [
                r
                for r in subset
                if r.get("dataset") == density and si(r["n"]) == 100000
            ]
            if matching:
                vals.append(
                    sf(matching[0]["memory_bytes"]) / max(si(matching[0]["n"]), 1)
                )
            else:
                vals.append(0)
        offset = -w / 2 if lang_idx == 0 else w / 2
        ax.bar(
            [i + offset for i in x],
            vals,
            w,
            color=PALETTE[lang],
            label=LANG_LABEL[lang],
            zorder=3,
        )

    ax.axhline(
        8.0,
        color="#E74C3C",
        linestyle=":",
        linewidth=1.5,
        label="Plain uint64 (8 B/key)",
    )
    ax.set_xticks(x)
    ax.set_xticklabels([d.title() for d in DENSITY_ORDER])
    ax.set_ylabel("Bytes per key")
    ax.set_title("Elias-Fano (n=100K)")
    ax.legend(fontsize=7)

    # EF: compression ratio
    ax = axes[2]
    for lang in LANG_ORDER:
        subset = rows_for(rows, "elias_fano", "build", lang)
        medium = [r for r in subset if r.get("dataset") == "medium"]
        medium.sort(key=lambda r: si(r["n"]))
        ns = [si(r["n"]) for r in medium]
        ratios = [
            8.0 / (sf(r["memory_bytes"]) / max(si(r["n"]), 1))
            for r in medium
            if sf(r["memory_bytes"]) > 0
        ]
        if ns and len(ratios) == len(ns):
            style = "-o" if lang == "cpp" else "--s"
            ax.plot(
                ns,
                ratios,
                style,
                color=PALETTE[lang],
                label=LANG_LABEL[lang],
                linewidth=2,
                markersize=6,
            )

    ax.set_xscale("log")
    ax.set_xlabel("n")
    ax.set_ylabel("Compression ratio vs uint64[]")
    ax.set_title("Elias-Fano Compression (medium)")
    ax.legend(fontsize=8)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: fmt_ops(v)))

    fig.suptitle("Memory Efficiency Analysis", fontweight="bold", y=1.02)
    fig.tight_layout()
    save_figure(fig, "fig13_memory_efficiency")
    plt.close(fig)


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    global OUTPUT_DIR
    args = parse_args()
    setup_style()
    OUTPUT_DIR = OUTPUT_BASE_DIR / args.output_dir_name
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary_stem = args.output_dir_name if args.output_dir_name != args.profile else args.profile
    summary_path = args.summary or (PROCESSED_DIR / f"{summary_stem}_summary.csv")
    summary = load_csv(summary_path)
    code = load_csv(CODE_METRICS_PATH)
    build = load_csv(BUILD_METRICS_PATH)

    if not summary:
        raise SystemExit(
            f"No summary data found at {summary_path}. Run the matching benchmark and summary tasks first."
        )

    print(f"Loaded {len(summary)} summary rows from {summary_path}, generating figures...")

    figure_01_throughput_overview(summary)
    print("  [1/13] Throughput overview")
    figure_02_bloom_scaling(summary)
    print("  [2/13] Bloom scaling")
    figure_03_bloom_bpk_sweep(summary)
    print("  [3/13] Bloom bits_per_key sweep")
    figure_04_bloom_fpr(summary)
    print("  [4/13] Bloom FPR analysis")
    figure_05_qf_workloads(summary)
    print("  [5/13] QF workload profiles")
    figure_06_qf_load_sensitivity(summary)
    print("  [6/13] QF load factor sensitivity")
    figure_07_qf_heatmap(summary)
    print("  [7/13] QF parameter heatmap")
    figure_08_ef_density(summary)
    print("  [8/13] EF density analysis")
    figure_09_ef_scaling(summary)
    print("  [9/13] EF scaling")
    figure_10_ratio_dashboard(summary)
    print("  [10/13] Performance ratio dashboard")
    figure_11_qf_remainder_sweep(summary)
    print("  [11/13] QF remainder bits sweep")
    figure_12_metrics_dashboard(code, build)
    print("  [12/13] Metrics dashboard")
    figure_13_memory_efficiency(summary)
    print("  [13/13] Memory efficiency")

    print(
        f"\nWrote 13 figures for profile '{args.profile}' "
        f"(output dir '{args.output_dir_name}') to {OUTPUT_DIR}"
    )


if __name__ == "__main__":
    main()
