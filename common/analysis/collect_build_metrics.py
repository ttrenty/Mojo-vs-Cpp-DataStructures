from __future__ import annotations

import csv
import os
import shutil
import statistics
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "results" / "processed" / "build_metrics.csv"
REPETITIONS = 5


def run(command: list[str]) -> None:
    subprocess.run(command, cwd=ROOT, check=True)


def cpp_setup() -> None:
    run(["python", "common/generate_hash_assets.py"])
    build_dir = ROOT / "build" / "cpp"
    if build_dir.exists():
        shutil.rmtree(build_dir)
    run(
        [
            "cmake",
            "-S",
            "cpp",
            "-B",
            "build/cpp",
            "-DCMAKE_BUILD_TYPE=Release",
            f"-DCMAKE_CXX_COMPILER={os.environ.get('CXX', 'clang++')}",
        ]
    )


def cpp_clean() -> None:
    target_dir = ROOT / "build" / "cpp" / "CMakeFiles" / "cpp_bench.dir"
    if target_dir.exists():
        for pattern in ("*.o", "*.obj", "*.o.d"):
            for path in target_dir.rglob(pattern):
                path.unlink()
    binary = ROOT / "build" / "cpp" / "cpp_bench"
    if binary.exists():
        binary.unlink()


def cpp_build() -> None:
    run(["cmake", "--build", "build/cpp", "--parallel", "--target", "cpp_bench"])


def mojo_setup() -> None:
    run(["python", "common/generate_hash_assets.py"])
    build_dir = ROOT / "build" / "mojo"
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True, exist_ok=True)


def mojo_clean() -> None:
    build_dir = ROOT / "build" / "mojo"
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True, exist_ok=True)


def mojo_build() -> None:
    run(
        [
            "pixi",
            "run",
            "mojo",
            "build",
            "-I",
            "mojo/src",
            "-I",
            "build/generated/mojo",
            "mojo/bench_main.mojo",
            "-o",
            "build/mojo/mojo_bench",
        ]
    )


TARGETS = [
    {
        "language": "cpp",
        "setup": cpp_setup,
        "clean": cpp_clean,
        "build": cpp_build,
        "binary_path": ROOT / "build" / "cpp" / "cpp_bench",
        "setup_command": (
            "python common/generate_hash_assets.py && "
            "cmake -S cpp -B build/cpp -DCMAKE_BUILD_TYPE=Release "
            f"-DCMAKE_CXX_COMPILER={os.environ.get('CXX', 'clang++')}"
        ),
        "build_command": "cmake --build build/cpp --parallel --target cpp_bench",
    },
    {
        "language": "mojo",
        "setup": mojo_setup,
        "clean": mojo_clean,
        "build": mojo_build,
        "binary_path": ROOT / "build" / "mojo" / "mojo_bench",
        "setup_command": "python common/generate_hash_assets.py",
        "build_command": (
            "pixi run mojo build -I mojo/src -I build/generated/mojo "
            "mojo/bench_main.mojo -o build/mojo/mojo_bench"
        ),
    },
]


def measure_target(target: dict[str, object]) -> dict[str, str]:
    setup = target["setup"]
    clean = target["clean"]
    build = target["build"]
    binary_path = target["binary_path"]

    assert callable(setup)
    assert callable(clean)
    assert callable(build)
    assert isinstance(binary_path, Path)

    setup_start = time.perf_counter()
    setup()
    setup_time_ms = (time.perf_counter() - setup_start) * 1000.0

    # Prime one-time generator and compiler startup work outside the measured
    # repetitions so the reported numbers reflect steady compile-only cost.
    clean()
    build()

    samples_ms: list[float] = []
    for _ in range(REPETITIONS):
        clean()
        start = time.perf_counter()
        build()
        samples_ms.append((time.perf_counter() - start) * 1000.0)

    size_bytes = binary_path.stat().st_size if binary_path.exists() else -1
    return {
        "language": str(target["language"]),
        "setup_command": str(target["setup_command"]),
        "build_command": str(target["build_command"]),
        "setup_time_ms": f"{setup_time_ms:.3f}",
        "repetitions": str(REPETITIONS),
        "compile_time_mean_ms": f"{statistics.mean(samples_ms):.3f}",
        "compile_time_median_ms": f"{statistics.median(samples_ms):.3f}",
        "compile_time_min_ms": f"{min(samples_ms):.3f}",
        "compile_time_max_ms": f"{max(samples_ms):.3f}",
        "compile_time_stddev_ms": (
            f"{statistics.pstdev(samples_ms):.3f}" if len(samples_ms) > 1 else "0.000"
        ),
        "binary_size_bytes": str(size_bytes),
        "binary_path": str(binary_path),
    }


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    rows = [measure_target(target) for target in TARGETS]
    with OUTPUT.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "language",
                "setup_command",
                "build_command",
                "setup_time_ms",
                "repetitions",
                "compile_time_mean_ms",
                "compile_time_median_ms",
                "compile_time_min_ms",
                "compile_time_max_ms",
                "compile_time_stddev_ms",
                "binary_size_bytes",
                "binary_path",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote build metrics to {OUTPUT}")


if __name__ == "__main__":
    main()
