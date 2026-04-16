from __future__ import annotations

import argparse
import os
import csv
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PARAMS_PATH = ROOT / "common" / "params.json"
RAW_BASE_DIR = ROOT / "results" / "raw"
BUILD_METRICS_PATH = ROOT / "results" / "processed" / "build_metrics.csv"
PROFILE_ALIASES = {"report": "representative"}
PROFILE_RUNS = {"smoke": 5, "representative": 11, "full": 9}
PROFILE_WARMUP_RUNS = {"smoke": 1, "representative": 3, "full": 2}

UNIVERSE_FACTORS = {
    "dense": 16,
    "medium": 256,
    "sparse": 4096,
}
VALID_STRUCTURES = {"blocked_bloom", "quotient_filter", "elias_fano"}


@dataclass(frozen=True)
class Job:
    language: str
    structure: str
    output_name: str
    args: list[str]


def canonical_profile(profile: str) -> str:
    return PROFILE_ALIASES.get(profile, profile)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run matched C++ and Mojo benchmark sweeps.")
    parser.add_argument(
        "--profile",
        choices=("smoke", "representative", "full", "report"),
        default="representative",
        help="Benchmark profile to execute.",
    )
    parser.add_argument(
        "--languages",
        default="cpp,mojo",
        help="Comma-separated subset of languages to benchmark.",
    )
    parser.add_argument(
        "--structures",
        default="blocked_bloom,quotient_filter,elias_fano",
        help="Comma-separated subset of structures to benchmark.",
    )
    parser.add_argument(
        "--taskset-core",
        type=int,
        default=0,
        help="Pin benchmark invocations to this CPU core when taskset is available.",
    )
    parser.add_argument(
        "--skip-build-metrics",
        action="store_true",
        help="Reuse an existing build_metrics.csv instead of collecting fresh clean-build times.",
    )
    parser.add_argument(
        "--runs-override",
        type=int,
        default=None,
        help="Override the default measured-run count for the selected profile.",
    )
    parser.add_argument(
        "--warmup-runs-override",
        type=int,
        default=None,
        help="Override the default unmeasured warmup-run count for the selected profile.",
    )
    parser.add_argument(
        "--parallel-workers",
        type=int,
        default=1,
        help="Run benchmark jobs in parallel across this many pinned cores. Use 0 for auto.",
    )
    parser.add_argument(
        "--reserve-cores",
        type=int,
        default=4,
        help="When --parallel-workers=0, leave this many CPU cores free for other processes.",
    )
    parser.add_argument(
        "--output-dir-name",
        default=None,
        help="Optional raw-results directory name under results/raw/. Defaults to the selected profile name.",
    )
    args = parser.parse_args()
    args.profile = canonical_profile(args.profile)
    args.output_dir_name = args.output_dir_name or args.profile
    return args


def load_params() -> dict[str, object]:
    import json

    with PARAMS_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def requested_languages(raw: str) -> list[str]:
    languages = [item.strip() for item in raw.split(",") if item.strip()]
    invalid = sorted(set(languages) - {"cpp", "mojo"})
    if invalid:
        raise SystemExit(f"Unsupported languages: {', '.join(invalid)}")
    return languages


def requested_structures(raw: str) -> list[str]:
    structures = [item.strip() for item in raw.split(",") if item.strip()]
    invalid = sorted(set(structures) - VALID_STRUCTURES)
    if invalid:
        raise SystemExit(f"Unsupported structures: {', '.join(invalid)}")
    return structures


def measure_builds(skip_build_metrics: bool) -> dict[str, str]:
    if not skip_build_metrics or not BUILD_METRICS_PATH.exists():
        subprocess.run(
            [sys.executable, "common/analysis/collect_build_metrics.py"],
            cwd=ROOT,
            check=True,
        )

    with BUILD_METRICS_PATH.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    compile_column = (
        "compile_time_mean_ms"
        if rows and "compile_time_mean_ms" in rows[0]
        else "compile_time_ms"
    )
    return {row["language"]: row[compile_column] for row in rows}


def git_hash() -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return completed.stdout.strip() or "unknown"


def safe_float(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return -1.0


def bench_compile_time_arg(raw_value: str) -> str:
    return str(int(round(safe_float(raw_value))))


def binary_for(language: str) -> Path:
    if language == "cpp":
        return ROOT / "build" / "cpp" / "cpp_bench"
    return ROOT / "build" / "mojo" / "mojo_bench"


def affinity_cores() -> list[int]:
    if hasattr(os, "sched_getaffinity"):
        return sorted(os.sched_getaffinity(0))
    count = os.cpu_count() or 1
    return list(range(count))


def selected_taskset_cores(
    taskset_core: int,
    parallel_workers: int,
    reserve_cores: int,
) -> list[int]:
    cores = affinity_cores()
    if not cores:
        return [taskset_core]

    if taskset_core in cores:
        start_index = cores.index(taskset_core)
        rotated = cores[start_index:] + cores[:start_index]
    else:
        rotated = cores

    if parallel_workers == 1:
        return [rotated[0]]

    if parallel_workers == 0:
        usable = max(1, len(rotated) - max(0, reserve_cores))
        return rotated[:usable]

    return rotated[: max(1, min(parallel_workers, len(rotated)))]


def base_command(language: str, taskset_core: int) -> list[str]:
    command: list[str] = []
    if shutil.which("taskset") is not None:
        command.extend(["taskset", "-c", str(taskset_core)])
    command.append(str(binary_for(language)))
    return command


def append_output(path: Path, stdout: str) -> None:
    lines = stdout.splitlines()
    if not lines:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = lines if not path.exists() else lines[1:]
    if not payload:
        return
    with path.open("a", encoding="utf-8", newline="") as handle:
        for line in payload:
            handle.write(line)
            handle.write("\n")


def smoke_jobs(languages: list[str], run_count: int, warmup_runs: int) -> list[Job]:
    jobs: list[Job] = []
    for language in languages:
        jobs.extend(
            [
                Job(
                    language,
                    "blocked_bloom",
                    f"blocked_bloom_{language}_smoke.csv",
                    [
                        "blocked_bloom",
                        "--n",
                        "100000",
                        "--bits-per-key",
                        "10",
                        "--runs",
                        str(run_count),
                        "--warmup-runs",
                        str(warmup_runs),
                        "--query-mode",
                        "negative",
                    ],
                ),
                Job(
                    language,
                    "quotient_filter",
                    f"quotient_filter_{language}_smoke.csv",
                    [
                        "quotient_filter",
                        "--q",
                        "12",
                        "--remainder-bits",
                        "10",
                        "--target-load",
                        "0.7",
                        "--runs",
                        str(run_count),
                        "--warmup-runs",
                        str(warmup_runs),
                        "--workload",
                        "read_heavy",
                        "--op-count",
                        "5000",
                    ],
                ),
                Job(
                    language,
                    "elias_fano",
                    f"elias_fano_{language}_smoke.csv",
                    [
                        "elias_fano",
                        "--n",
                        "5000",
                        "--universe-factor",
                        "64",
                        "--runs",
                        str(run_count),
                        "--warmup-runs",
                        str(warmup_runs),
                        "--workload",
                        "predecessor",
                        "--op-count",
                        "5000",
                    ],
                ),
            ]
        )
    return jobs


def representative_jobs(
    languages: list[str], run_count: int, warmup_runs: int
) -> list[Job]:
    jobs: list[Job] = []
    for language in languages:
        for bits_per_key in (8, 10, 12, 14):
            jobs.append(
                Job(
                    language,
                    "blocked_bloom",
                    f"representative_blocked_bloom_{language}.csv",
                    [
                        "blocked_bloom",
                        "--n",
                        "100000",
                        "--bits-per-key",
                        str(bits_per_key),
                        "--runs",
                        str(run_count),
                        "--warmup-runs",
                        str(warmup_runs),
                        "--query-mode",
                        "negative",
                    ],
                )
            )
        jobs.append(
            Job(
                language,
                "blocked_bloom",
                f"representative_blocked_bloom_{language}.csv",
                [
                    "blocked_bloom",
                    "--n",
                    "100000",
                    "--bits-per-key",
                    "10",
                    "--runs",
                    str(run_count),
                    "--warmup-runs",
                    str(warmup_runs),
                    "--query-mode",
                    "mixed",
                ],
            )
        )

        for target_load in ("0.3", "0.4", "0.5", "0.7", "0.85"):
            jobs.append(
                Job(
                    language,
                    "quotient_filter",
                    f"representative_quotient_filter_{language}.csv",
                    [
                        "quotient_filter",
                        "--q",
                        "16",
                        "--remainder-bits",
                        "12",
                        "--target-load",
                        target_load,
                        "--runs",
                        str(run_count),
                        "--warmup-runs",
                        str(warmup_runs),
                        "--workload",
                        "read_heavy",
                        "--op-count",
                        "40000",
                    ],
                )
            )
        for workload in ("mixed", "delete_heavy"):
            jobs.append(
                Job(
                    language,
                    "quotient_filter",
                    f"representative_quotient_filter_{language}.csv",
                    [
                        "quotient_filter",
                        "--q",
                        "16",
                        "--remainder-bits",
                        "12",
                        "--target-load",
                        "0.7",
                        "--runs",
                        str(run_count),
                        "--warmup-runs",
                        str(warmup_runs),
                        "--workload",
                        workload,
                        "--op-count",
                        "40000",
                    ],
                )
            )

        for workload in ("contains", "select", "predecessor"):
            jobs.append(
                Job(
                    language,
                    "elias_fano",
                    f"representative_elias_fano_{language}.csv",
                    [
                        "elias_fano",
                        "--n",
                        "100000",
                        "--universe-factor",
                        str(UNIVERSE_FACTORS["medium"]),
                        "--runs",
                        str(run_count),
                        "--warmup-runs",
                        str(warmup_runs),
                        "--workload",
                        workload,
                        "--op-count",
                        "40000",
                    ],
                )
            )
    return jobs


def full_jobs(
    languages: list[str],
    params: dict[str, object],
    run_count: int,
    warmup_runs: int,
) -> list[Job]:
    jobs: list[Job] = []
    bloom = params["blocked_bloom"]
    qf = params["quotient_filter"]
    ef = params["elias_fano"]

    for language in languages:
        for n in bloom["n_keys"]:
            for bits_per_key in bloom["bits_per_key"]:
                for query_mode in bloom["query_modes"]:
                    jobs.append(
                        Job(
                            language,
                            "blocked_bloom",
                            f"full_blocked_bloom_{language}.csv",
                            [
                                "blocked_bloom",
                                "--n",
                                str(n),
                                "--bits-per-key",
                                str(bits_per_key),
                                "--runs",
                                str(run_count),
                                "--warmup-runs",
                                str(warmup_runs),
                                "--query-mode",
                                str(query_mode),
                            ],
                        )
                    )

        for q in qf["q"]:
            for remainder_bits in qf["remainder_bits"]:
                for target_load in qf["target_load_factor"]:
                    for workload in ("read_heavy", "mixed", "delete_heavy"):
                        jobs.append(
                            Job(
                                language,
                                "quotient_filter",
                                f"full_quotient_filter_{language}.csv",
                                [
                                    "quotient_filter",
                                    "--q",
                                    str(q),
                                    "--remainder-bits",
                                    str(remainder_bits),
                                    "--target-load",
                                    str(target_load),
                                    "--runs",
                                    str(run_count),
                                    "--warmup-runs",
                                    str(warmup_runs),
                                    "--workload",
                                    workload,
                                    "--op-count",
                                    "40000",
                                ],
                            )
                        )

        for n in ef["n"]:
            for density in ef["density"]:
                for workload in ("contains", "select", "predecessor"):
                    jobs.append(
                        Job(
                            language,
                            "elias_fano",
                            f"full_elias_fano_{language}.csv",
                            [
                                "elias_fano",
                                "--n",
                                str(n),
                                "--universe-factor",
                                str(UNIVERSE_FACTORS[str(density)]),
                                "--runs",
                                str(run_count),
                                "--warmup-runs",
                                str(warmup_runs),
                                "--workload",
                                workload,
                                "--op-count",
                                "40000",
                            ],
                        )
                    )
    return jobs


def jobs_for_profile(
    profile: str,
    languages: list[str],
    params: dict[str, object],
    run_count: int,
    warmup_runs: int,
) -> list[Job]:
    if profile == "smoke":
        return smoke_jobs(languages, run_count, warmup_runs)
    if profile == "representative":
        return representative_jobs(languages, run_count, warmup_runs)
    return full_jobs(languages, params, run_count, warmup_runs)


def execute_job(
    job: Job,
    compile_times: dict[str, str],
    current_git_hash: str,
    taskset_core: int,
    output_dir_name: str,
) -> tuple[Path, str]:
    output_path = RAW_BASE_DIR / output_dir_name / job.output_name
    command = base_command(job.language, taskset_core) + job.args + [
        "--git-hash",
        current_git_hash,
        "--compile-time-ms",
        bench_compile_time_arg(compile_times.get(job.language, "-1")),
    ]
    print(
        f"[bench] core {taskset_core} {job.language} {job.structure}: "
        f"{' '.join(job.args)}"
    )
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as error:
        if command and Path(command[0]).name == "taskset":
            fallback_command = [str(binary_for(job.language))] + job.args + [
                "--git-hash",
                current_git_hash,
                "--compile-time-ms",
                bench_compile_time_arg(compile_times.get(job.language, "-1")),
            ]
            print(
                "[bench] taskset unavailable in this environment, retrying without CPU pinning"
            )
            completed = subprocess.run(
                fallback_command,
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
        else:
            raise error
    return output_path, completed.stdout


def run_job(
    job: Job,
    compile_times: dict[str, str],
    current_git_hash: str,
    taskset_core: int,
    output_dir_name: str,
) -> None:
    output_path, stdout = execute_job(
        job, compile_times, current_git_hash, taskset_core, output_dir_name
    )
    append_output(output_path, stdout)


def run_job_batch(
    jobs: list[Job],
    compile_times: dict[str, str],
    current_git_hash: str,
    taskset_core: int,
    output_dir_name: str,
) -> list[tuple[Path, str]]:
    outputs: list[tuple[Path, str]] = []
    for job in jobs:
        outputs.append(
            execute_job(job, compile_times, current_git_hash, taskset_core, output_dir_name)
        )
    return outputs


def main() -> None:
    args = parse_args()
    params = load_params()
    languages = requested_languages(args.languages)
    structures = requested_structures(args.structures)
    run_count = args.runs_override or PROFILE_RUNS[args.profile]
    warmup_runs = args.warmup_runs_override or PROFILE_WARMUP_RUNS[args.profile]
    selected_cores = selected_taskset_cores(
        args.taskset_core, args.parallel_workers, args.reserve_cores
    )
    compile_times = measure_builds(args.skip_build_metrics)
    current_git_hash = git_hash()

    jobs = jobs_for_profile(args.profile, languages, params, run_count, warmup_runs)
    jobs = [job for job in jobs if job.structure in structures]
    raw_dir = RAW_BASE_DIR / args.output_dir_name
    raw_dir.mkdir(parents=True, exist_ok=True)
    for output_name in sorted({job.output_name for job in jobs}):
        output_path = raw_dir / output_name
        if output_path.exists():
            output_path.unlink()

    if len(selected_cores) > 1 and jobs:
        if shutil.which("taskset") is None:
            print(
                "[bench] taskset is unavailable, so parallel mode will rely on the OS scheduler "
                "and may increase measurement noise"
            )
        print(
            f"[bench] parallel mode enabled across {len(selected_cores)} cores: "
            + ",".join(str(core) for core in selected_cores)
        )
        buckets = [[] for _ in selected_cores]
        for index, job in enumerate(jobs):
            buckets[index % len(selected_cores)].append(job)
        with ThreadPoolExecutor(max_workers=len(selected_cores)) as pool:
            futures = {
                pool.submit(
                    run_job_batch,
                    bucket,
                    compile_times,
                    current_git_hash,
                    core,
                    args.output_dir_name,
                ): core
                for core, bucket in zip(selected_cores, buckets)
                if bucket
            }
            for future in as_completed(futures):
                for output_path, stdout in future.result():
                    append_output(output_path, stdout)
    else:
        for job in jobs:
            run_job(
                job,
                compile_times,
                current_git_hash,
                selected_cores[0],
                args.output_dir_name,
            )

    print(
        f"Completed {len(jobs)} benchmark invocations for profile '{args.profile}' into {raw_dir} "
        f"(measured runs={run_count}, warmup runs={warmup_runs})"
    )


if __name__ == "__main__":
    main()
