from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RAW_BASE_DIR = ROOT / "results" / "raw"
PROCESSED_DIR = ROOT / "results" / "processed"
PROFILE_ALIASES = {"report": "representative"}
PROFILE_CANDIDATES = {
    "smoke": ("smoke",),
    "representative": ("representative", "report"),
    "full": ("full",),
}
IGNORE_PARAM_KEYS = {
    "avg_cluster_length",
    "avg_probe_distance",
    "max_cluster_length",
    "max_probe_distance",
    "observed_checksum",
    "observed_hits",
}

CANONICAL_FLOAT_KEYS = {"target_load"}


def canonical_profile(profile: str) -> str:
    return PROFILE_ALIASES.get(profile, profile)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize benchmark CSV rows for one profile.")
    parser.add_argument(
        "--profile",
        choices=("smoke", "representative", "full", "report"),
        default="representative",
        help="Benchmark profile directory under results/raw to summarize.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional output CSV path. Defaults to results/processed/<profile>_summary.csv.",
    )
    parser.add_argument(
        "--raw-dir-name",
        default=None,
        help="Optional raw-results directory name under results/raw/. Defaults to the selected profile.",
    )
    args = parser.parse_args()
    args.profile = canonical_profile(args.profile)
    args.raw_dir_name = args.raw_dir_name or args.profile
    return args


def parse_parameter_tuple(parameter_tuple: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for item in parameter_tuple.split(";"):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        fields[key] = value
    return fields


def normalize_parameter_tuple(parameter_tuple: str) -> str:
    fields = parse_parameter_tuple(parameter_tuple)
    for ignored in IGNORE_PARAM_KEYS:
        fields.pop(ignored, None)
    for key in CANONICAL_FLOAT_KEYS:
        if key in fields:
            fields[key] = f"{float(fields[key]):.2f}"
    return ";".join(f"{key}={fields[key]}" for key in sorted(fields))


def load_rows(raw_dir: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in sorted(raw_dir.glob("*.csv")):
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows.extend(csv.DictReader(handle))
    return rows


def legacy_raw_paths(profile: str) -> list[Path]:
    if profile == "smoke":
        pattern = "*_smoke.csv"
    elif profile == "representative":
        pattern = "report_*.csv"
    else:
        pattern = f"{profile}_*.csv"
    return sorted(path for path in RAW_BASE_DIR.glob(pattern) if path.is_file())


def load_profile_rows(profile: str, raw_dir_name: str | None = None) -> tuple[list[dict[str, str]], str]:
    if raw_dir_name is not None:
        raw_dir = RAW_BASE_DIR / raw_dir_name
        rows = load_rows(raw_dir)
        if rows:
            return rows, str(raw_dir)
        raise SystemExit(
            f"No raw benchmark CSVs found in {raw_dir}. "
            f"Run the matching benchmark task first."
        )

    for candidate in PROFILE_CANDIDATES[profile]:
        raw_dir = RAW_BASE_DIR / candidate
        primary_rows = load_rows(raw_dir)
        if primary_rows:
            return primary_rows, str(raw_dir)

    rows: list[dict[str, str]] = []
    legacy_paths = legacy_raw_paths(profile)
    for path in legacy_paths:
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows.extend(csv.DictReader(handle))
    if rows:
        return rows, "legacy-flat-layout"
    candidates = ", ".join(str(RAW_BASE_DIR / candidate) for candidate in PROFILE_CANDIDATES[profile])
    raise SystemExit(
        f"No raw benchmark CSVs found for profile '{profile}'. "
        f"Expected files under one of: {candidates}"
    )


def percentile(sorted_samples: list[float], fraction: float) -> float:
    if not sorted_samples:
        return 0.0
    if len(sorted_samples) == 1:
        return sorted_samples[0]
    position = fraction * (len(sorted_samples) - 1)
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    if lower_index == upper_index:
        return sorted_samples[lower_index]
    weight = position - lower_index
    lower_value = sorted_samples[lower_index]
    upper_value = sorted_samples[upper_index]
    return lower_value + (upper_value - lower_value) * weight


def summarize(samples: list[float]) -> dict[str, float]:
    ordered = sorted(samples)
    pstdev = statistics.pstdev(ordered) if len(ordered) > 1 else 0.0
    ci95 = 1.96 * pstdev / math.sqrt(len(ordered)) if len(ordered) > 1 else 0.0
    throughput_samples = sorted(1_000_000_000.0 / sample for sample in ordered if sample > 0.0)
    throughput_median = statistics.median(throughput_samples) if throughput_samples else 0.0
    return {
        "throughput_min_ns_per_op": ordered[0],
        "throughput_median_ns_per_op": statistics.median(ordered),
        "throughput_mean_ns_per_op": statistics.mean(ordered),
        "throughput_max_ns_per_op": ordered[-1],
        "throughput_stddev_ns_per_op": pstdev,
        "throughput_ci95_ns_per_op": ci95,
        "throughput_p25_ops_per_sec": percentile(throughput_samples, 0.25),
        "throughput_median_ops_per_sec": throughput_median,
        "throughput_p75_ops_per_sec": percentile(throughput_samples, 0.75),
    }


def sample_score(row: dict[str, str]) -> tuple[int, int]:
    compile_time = -1.0
    try:
        compile_time = float(row.get("compile_time_ms", "-1"))
    except ValueError:
        compile_time = -1.0
    has_compile_time = 1 if compile_time >= 0.0 else 0
    has_git_hash = 1 if row.get("git_hash", "unknown") != "unknown" else 0
    return (has_compile_time, has_git_hash)


def main() -> None:
    args = parse_args()
    output_stem = args.raw_dir_name if args.raw_dir_name != args.profile else args.profile
    output = args.output or (PROCESSED_DIR / f"{output_stem}_summary.csv")
    rows, source_description = load_profile_rows(
        args.profile,
        raw_dir_name=args.raw_dir_name if args.raw_dir_name != args.profile else None,
    )
    grouped: dict[tuple[str, ...], list[dict[str, str]]] = defaultdict(list)

    for row in rows:
        key = (
            row["language"],
            row["structure"],
            row["workload"],
            row["dataset"],
            row["n"],
            normalize_parameter_tuple(row["param_tuple"]),
        )
        grouped[key].append(row)

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = [
            "language",
            "structure",
            "workload",
            "dataset",
            "n",
            "param_tuple",
            "run_count",
            "throughput_min_ns_per_op",
            "throughput_median_ns_per_op",
            "throughput_mean_ns_per_op",
            "throughput_max_ns_per_op",
            "throughput_stddev_ns_per_op",
            "throughput_ci95_ns_per_op",
            "throughput_p25_ops_per_sec",
            "throughput_median_ops_per_sec",
            "throughput_p75_ops_per_sec",
            "memory_bytes",
            "fpr",
            "compile_time_ms",
            "git_hash",
            "compiler_version",
            "observed_hits",
            "observed_checksum",
            "avg_probe_distance",
            "max_probe_distance",
            "max_cluster_length",
            "avg_cluster_length",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()

        for key in sorted(grouped):
            group = grouped[key]
            stats = summarize([float(row["throughput_ns_per_op"]) for row in group])
            sample = max(group, key=sample_score)
            writer.writerow(
                {
                    "language": key[0],
                    "structure": key[1],
                    "workload": key[2],
                    "dataset": key[3],
                    "n": key[4],
                    "param_tuple": key[5],
                    "run_count": len(group),
                    **stats,
                    "memory_bytes": sample["memory_bytes"],
                    "fpr": sample["fpr"],
                    "compile_time_ms": sample["compile_time_ms"],
                    "git_hash": sample["git_hash"],
                    "compiler_version": sample["compiler_version"],
                    "observed_hits": sample.get("observed_hits", ""),
                    "observed_checksum": sample.get("observed_checksum", ""),
                    "avg_probe_distance": sample.get("avg_probe_distance", ""),
                    "max_probe_distance": sample.get("max_probe_distance", ""),
                    "max_cluster_length": sample.get("max_cluster_length", ""),
                    "avg_cluster_length": sample.get("avg_cluster_length", ""),
                }
            )
    print(
        f"Wrote summarized results for profile '{args.profile}' to {output} "
        f"(source: {source_description})"
    )


if __name__ == "__main__":
    main()
